#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import io
import os
import sys
import math
import logging
import warnings
import argparse
import calendar
import datetime
import textwrap
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from google.cloud import storage
from google.cloud import bigquery
from datetime import date, timedelta

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger()

warnings.filterwarnings('ignore')
NCHARS = 40


def calculate_costs_and_discounts(qdf, error_label=''):
    '''Calculate cost and discount from the BQ query dataframe'''
    
    # calculate costs
    cost = qdf['cost'].sum()
    
    # calculate credits
    credits = qdf.loc[qdf['credits'].apply(lambda x: len(x) > 0)]
    discounts = [sum([el['amount'] for el in row]) for row in credits['credits']]
    discount = sum(discounts)
    total_cost = cost + discount
    pid = list(set(qdf['id']))[0]
    
    attr = ['name', 'id', 'billing_label', 'wbs']
    d = {a: list(set(qdf[a]))[0] for a in attr} 
    for a in attr:
        assert len(set(qdf[a])) == 1, f"when aggregating by PROJECT_ID " + \
            f"{pid} in {error_label} - label {a} assertion error :: not unique value - {set(qdf[a])}"
    
    # keys and values
    res = {
        'project_id': d['id'],
        'project_name': d['name'],
        'billing_label': d['billing_label'], 
        'wbs': d['wbs'],
        'cost': cost,
        'discount': discount,
        'total_cost': total_cost
    }
    
    return res

def unnest_labels(df):
    ''' Unnest labels extracted from the bq billing table'''
    
    labels = df['labels']
    billing_labs = []
    for i in range(labels.shape[0]):
        row = labels.iloc[i]
        lab = ""
        for l in row:
            item = f"{l['key']}_{l['value']}"
            if l['key'] == 'billing':
                lab = item  
        billing_labs.append(lab)
    
    df['billing_label'] = billing_labs
    df['wbs'] = df['billing_label'].apply(lambda x: x.split('_')[1] if x != '' in x else '')

    return df

def aggregate_by_project(df, key, value):
    '''Aggregate biiling data in the data frame'''
    
    res = []
    if df.shape[0] > 0:
        grouped = df.groupby(df['id'])
        
        for p in grouped.groups.keys():
            d = grouped.get_group(p)                
            try:
                r = calculate_costs_and_discounts(d, value)
            except AssertionError as e:
                log.error(e)
            else:
                r[key] = value
                res.append(r)
    
    return res

def run_query(query, client, dry_run=False):
    '''Run BQ query and return query result as dataframe, data billed / 
       data read in GB (term - either date or a billing label)'''
    
    df = pd.DataFrame()
    if dry_run:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        q = client.query(query, job_config=job_config)
    else:
        q = client.query(query)
    
    return q

def get_prev_month_last_day(year, month):
    '''Get last day of the given month in a given year'''
    
    cal = calendar.monthcalendar(year, month)
    days = [d for w in cal for d in w  if d != 0]
    days.reverse()
    end = days[1]
    
    return end

def get_query_billing_label(table_name, start_date, end_date, label):
    '''
    Get cost and discount queries given: BQ table name, start/end dates, billing label
    '''
    
    query = """
    SELECT project.name,
    project.id,
    project.labels,
    cost,
    credits,
    invoice
    FROM `%s`,
        UNNEST(project.labels) project_labels
    WHERE
    ((_PARTITIONDATE >= '%s') AND
        (_PARTITIONDATE <= '%s') AND
    (project_labels.key = 'billing') AND
    (project_labels.value = '%s')) 
    """ % (table_name, start_date, end_date, label)
    
    return query

def get_query(table_name, qdate):
    ''' Get cost and discount queries given: BQ table name, Star/End dates '''
    
    query = """
    SELECT project.name,
    project.id,
    project.labels,
    cost,
    credits,
    invoice
    FROM `%s`
    WHERE
    (_PARTITIONDATE = '%s') 
    """ % (table_name, qdate)
    
    return query

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def get_dates(start, end):
    '''Get dates in a month with 1 day overlap from prev/next months'''
    
    delta = timedelta(days=1)
    
    s = datetime.datetime.strptime(start, '%Y-%m-%d')
    e = datetime.datetime.strptime(end, '%Y-%m-%d')
    start_dt = date(s.year, s.month, s.day)
    end_dt = date(e.year, e.month, e.day)
    
    log.info(f"Start date %Y-%m-%d: {start_dt}, end date %Y-%m-%d: {end_dt}")    
    
    # store dates between the two dates in a list
    dates = []
    while start_dt <= end_dt:
        # add current date to list by converting  it to iso format
        dates.append(start_dt.isoformat())
        
        # increment start date by timedelta
        start_dt += delta

    log.info(f"Total dates {len(dates)}")    

    return dates

def multiproc_wrapper(args):
    ''' Multiproc wrapper function '''
    
    try:
        res = process_batch(*args)
    except Exception as e:
        log.error(f'ERROR while processing batch {args[0]}: {e}')
        return None
    else:
        return res

def save_df(df, name, dirout):
    '''Save date report'''
    
    fname = re.sub('[^A-Za-z0-9]+', "_", name.lower())
    fout = os.path.join(dirout, f'{fname}.tsv')
    if df is not None:
        df.to_csv(fout, sep='\t', index=False)
    
    return fout

def filter_by_invoice_month(df, invoice_month):
    '''Filter by invoice month'''
    ids = df['invoice'].apply(lambda x: x['month'] == invoice_month)
    df_filtered_out = df[~ids]
    df = df[ids]
    
    if not df.empty:
        df = df.reset_index(drop=True) 
    
    return df, df_filtered_out

def process_batch(q, project_id, batch_id, name, fetch_type, invoice_month, 
                  chunk_size, dirout, save_raw, save_removed):
    '''Process single batch yquery to extract labeled/unlabeled/wbs sums'''

    client = bigquery.Client(project=project_id)
    r = run_query(q, client, dry_run = False)

    partition_date = q.split('\n')[len(q.split('\n'))-2].replace(" ", "")

    bq_result = r.result(page_size=chunk_size)
    billed = 0 if r.total_bytes_billed is None else r.total_bytes_billed
    batch_id_formatted = "{:02d}".format(batch_id)
    total_rows_processed = bq_result.total_rows
    log.info(f"Batch {batch_id_formatted}: {name} - bytes billed: {billed} ({round(billed * 1e-09, 4)} GB)")

    s = datetime.datetime.now()
    log.info(f"Batch {batch_id_formatted}: Start processing {name}" )
    
    # unnest billing label and aggreagate data by project_id
    df_list = []
    df_list_removed = []
    df_list_raw = []
    tot_pages = math.ceil(bq_result.total_rows / chunk_size)
    for df in bq_result.to_dataframe_iterable():
        
        if len(df_list) % 50 == 0:
            log.info( f"Batch {batch_id_formatted}: {name} - Processed {len(df_list)} pages out of {tot_pages}" )
        
        if not df.empty:
            df, df_removed = filter_by_invoice_month(df, invoice_month)
            df_list_removed.append(df_removed)

            df_raw = df.copy()

            df = unnest_labels(df)
            aggr = aggregate_by_project(df, fetch_type, name)
            df_list.append(aggr)

            df_list_raw.append(df_raw)

    # aggregate data between the chunks using project_id column   
    log.info( f"Batch {batch_id_formatted}: {name} - data unnested and aggregated" )
    combined = pd.DataFrame(flatten(df_list)) 
    e = datetime.datetime.now()
    log.info(f'Batch {batch_id_formatted}: {name} - proccessing finished in {round((e - s).seconds / 60, 2)} mins' )
    
    if combined.empty:
        log.info( f"Batch {batch_id_formatted}: {name} - no entries for a given invoice month were found" )
        return None, billed, total_rows_processed
    
    # save all processed data
    res = aggregate_by_attribute(combined, 'project_id')        
    fout = save_df(res, name, dirout)
    log.info( f"Batch {batch_id_formatted}: {name} - saved to {fout}" )

    # save unaggregated full data
    if save_raw:
        unaggregated = pd.concat(df_list_raw)
        unaggregated['partition_date'] = partition_date
        unaggregated['invoice_month'] = invoice_month
        fout_unaggregated = save_df(unaggregated, name + '_unaggreagted', dirout)
        log.info( f"Batch {batch_id_formatted}: {name} - saved to {fout_unaggregated}" )
    
     # save removed rows unaggregated data
    if save_removed:
        removed = pd.concat(df_list_removed)
        removed['partition_date'] = partition_date
        removed['invoice_month'] = invoice_month
        fout_removed = save_df(removed, name + "_removed_rows", dirout)
        log.info( f"Batch {batch_id_formatted}: {name} - REMOVED ROWS saved to {fout_removed}" )

    return fout, billed, total_rows_processed

def create_tmp_dir(dirout, invoice_month):
    ts = datetime.datetime.now().strftime("%d%m%YT%H%M%S")
    tmp = os.path.join(dirout, f"invoice{invoice_month}_ts_{ts}")
    if not os.path.exists(tmp):
        os.makedirs(tmp) 
    return tmp

def summarize_costs_info(l, labels = ['GCP projects', 'Billing Label', 'WBS']):
    '''Summarize costs per each label'''
    
    for i in range(len(labels)):
        by = labels[i]
        tot = l[i]['total'][l[i]['total'] != ''].iloc[0]
        log.info(f'{f"Total costs ({by}):".ljust(NCHARS)}{tot}')

def summarize_bq_costs_info(b, label = 'processed'):
    '''Summarize BQ processing costs per each billing label'''
    
    total_gb = round(sum(b) * 1e-9, 2)
    total_tb = sum(b) * 1e-12
    total_cost = total_tb * 5
    
    log.info(f'{f"(BQ) Total data {label}:".ljust(NCHARS)}{total_gb} GB ({total_tb} TB)')
    if label == 'billed':
        log.info(f'{"(BQ) Billed compute cost of queries:".ljust(NCHARS)}{total_cost} $\n')
    else:
        log.info(f'{"(BQ) Estimated compute cost of queries:".ljust(NCHARS)}{total_cost} $\n')

def unique_list(l, sort = False):
    '''Get unique elements in array preserving the order'''
    if sort:
        return list(set(l))
    else:
        return list(dict.fromkeys(l))

def aggregate_by_attribute(df, by = 'project_id', drop_cols=[]):
    '''Aggreagate data across dates'''
    
    df = df.fillna('')
    cols_calc = list(df.columns[df.dtypes == 'float64'])
    cols_summarize = list(set(df.columns).difference(set(cols_calc)))
    cols_calc.append(by)
    
    res = pd.concat([
        df[cols_summarize].groupby(by).agg(lambda x: ';'.join(unique_list(list(x), sort = True))),
        df[cols_calc].groupby(by).sum()
    ], axis = 1)
    
    # re-order and drop index
    order = ['project_id', 'project_name', 'billing_label', 'description', 'wbs', 'cost', 'discount', 'total_cost']
    
    cols_order = [c for c in order if c in list(res.columns)] + list(set(res.columns).difference(set(order)))                 
    cols = unique_list([by] + cols_order)
    res[by] = res.index
    res = res.reset_index(drop=True)
    res = res[cols]
    if len(drop_cols) > 0:
        try:
            res = res.drop(columns = drop_cols)
        except KeyError as _:
            pass 
    
    return res

def remove_project_wt_multiple_billing_labels(df, exclude_projects):
    ''' Remove projects that had multiple billing labels in the BQ before summarizing final costs
        If projects with errors were observed, exclude them altogether '''
    
    ids_exclude = df['project_id'].apply(lambda x: exclude_projects.count(x) != 0)
    df_exclude = df[ids_exclude].copy()
    df_exclude = df_exclude.reset_index(drop=True)
    
    # aggregate by attribute
    df_exclude_aggr = aggregate_by_attribute(df_exclude, by = 'project_id', drop_cols=['date'])
    
    df_filt = df[~ids_exclude]
    df_filt = df_filt.reset_index(drop=True)
    
    if len(exclude_projects) > 0:
        log.warning(f'{len(exclude_projects)} projects with multiple \
                      billing / wbs labels found - exclude from the main report.\n')
    
    return df_filt, df_exclude_aggr

def get_projects_with_errors(df):    
    if df.empty:
        return []
    grouped = df.groupby(df['project_id'])
    
    lab = []
    wbs = []
    for p in grouped.groups.keys():
        d = grouped.get_group(p)
        if len(set(d['billing_label'])) > 1 or len(set(d['wbs'])) > 1:
            lab.append(unique_list(d['billing_label']))
            wbs.append(unique_list(d['wbs']))
    
    ids = df.apply(lambda x: flatten(lab).count(x['billing_label']) > 0 or flatten(wbs).count(x['wbs']) > 0, axis = 1)
    projects = unique_list(df[ids]['project_id'])
    
    return projects

def combine_reports(files):
    '''Read files per date/billing label'''
    
    l = []
    log.info("Starting to combine data across the dates.")
    for i,f in enumerate(files):
        log.info(f"Combine file {i} to the final table: {f}")
        x = pd.read_csv(f, sep='\t', header=0, dtype={'wbs': 'string','billing_label': 'string'})
        l.append(x)
    
    if len(l) > 0:
        combined = pd.concat(l)
    else:
        combined = pd.DataFrame()
    
    combined = combined.fillna('')
    exclude_projects = get_projects_with_errors(combined)
    
    return combined, exclude_projects
    
def aggregate_and_prepare_excel_sheets(df, metadata):
    ''' Remove those project that had multiple billing labels in the BQ
        If projects with errors were observed, exclude them altogether '''
    
    # match the description
    metadata = md.copy()
    
    if 'description' not in df.columns:
        metadata['billing_label'] = 'billing_' + metadata['label']
        metadata.index = metadata['billing_label']
        
        d = list(set(df['billing_label']).difference(
            set(metadata['billing_label'])))
        v = [pd.NA] * len(d)    
        add = pd.DataFrame(list(zip(v, v, d)), 
                           columns = metadata.columns, 
                           index=d)
        
        metadata = pd.concat([metadata, add])
        df['description'] = metadata.loc[df['billing_label']]['description'].values
    
    # aggregate by attribute
    pr = aggregate_by_attribute(
        df, by = 'project_id', drop_cols=['date']
    )
    lab = aggregate_by_attribute(
        df, by = 'billing_label', drop_cols=['date']
    )
    wbs = aggregate_by_attribute(
        df, by = 'wbs', drop_cols=['date']
    )
    
    # append total to project
    result = []
    for t in [pr, lab, wbs]:
        t['total'] = [''] * len(t)
        t['total'][len(t)-1] = t['total_cost'].sum()
        result.append(t)
    
    return result

def save_result_to_excel(df_list, invoice_month, dirout, mode):
    ''' Save dataframes to the excel file'''
    
    fname = f"report_{invoice_month}_{mode}.xlsx"
    fout = os.path.join(dirout, fname)
    with pd.ExcelWriter(fout) as writer:
        df_list[0].to_excel(writer, sheet_name="by GCP project", index=False)
        df_list[1].to_excel(writer, sheet_name="by billing label", index=False)
        df_list[2].to_excel(writer, sheet_name="by WBS", index=False)
        log.info(f'Saved report to {fout}\n')

def flatten(l):
    flat_list = [item for sublist in l for item in sublist]
    return flat_list

def get_metadata_from_bucket(bucket_name, m_filename):
    '''Get metadata file from the GCS bucket'''
    
    try:    
        client_gsc = storage.Client()
        bucket = client_gsc.get_bucket(bucket_name)
        blob = bucket.get_blob(m_filename)
        data = blob.download_as_string()
        dat = pd.read_csv(io.StringIO(data.decode()), sep='\t')
    
    except Exception as e:
        log.error(f'Error ocurred while fetching the metadata : {e}')
        sys.exit()
    else:
        log.info('Succesfully read metadata file')
        return dat

def save_errors_to_excel(df_excluded, dirout, mode):
    '''Save errors into a separate excel file'''
    
    fname = f"report_{invoice_month}_{mode}_ERRORS.xlsx"
    fout = os.path.join(dirout, fname)

    billing_label = unique_list(flatten([e.split(';') for e in set(df_excluded['billing_label'])]))
    wbs = unique_list(flatten([e.split(';') for e in set(df_excluded['wbs'])]))

    # save to a separte report
    with pd.ExcelWriter(fout) as writer:
        df_excluded.to_excel(writer, sheet_name="excluded from report(by proj)", index=False)
        pd.DataFrame({'billing_label': billing_label}).to_excel(writer, sheet_name="excluded billing labels", index=False)
        pd.DataFrame({'wbs': wbs}).to_excel(writer, sheet_name="excluded wbs", index=False)
        log.info(f'\nSaved report to {fout}')

def remove_temp_files(dir):
    '''Remove tmp files'''
    files = [os.path.join(dir, f) for f in os.listdir(dir)]
    for f in files:
        os.remove(f)

def list_full_paths(directory):
    return [os.path.join(directory, file) for file in os.listdir(directory)]

def get_prev_runs(dirout, dirout_root, names):
    ''' Get a list of files from previous runs '''
    
    pattern = os.path.basename(dirout).split('_')[0]
    dir_list = []
    for f in os.listdir(dirout_root):
        if pattern in f:
            dir_list.append(os.path.join(dirout_root, f))
    files_full = flatten([list_full_paths(d) for d in dir_list if os.path.isdir(d)])
    file_bn =  [os.path.basename(f) for f in files_full]
    df = pd.DataFrame({'path': files_full, 'basename': file_bn})
    df = df[~df['basename'].duplicated()] 
    
    names_ = [re.sub('[^A-Za-z0-9]+', "_", n.lower()) for n in names]
    names_err_ = [re.sub('[^A-Za-z0-9]+', "_", n.lower()) + '_errors' for n in names]
    ids = df['basename'].apply(lambda x: x.replace('.tsv', '') in names_ or x.replace('.tsv', '') in names_err_ )
    df = df[ids]
    df = df.reset_index(drop = True)
    
    return df

def prepare_queiries(args, md):
    ''' Prepare a list of queries '''

    t_name = f"{args.project_id}.{args.dataset_id}.{args.table_id}"
    
    # start
    if args.month == 1:
        prev_month_last_day = get_prev_month_last_day(args.year-1, 12)
        start = f'{args.year-1}-12-{prev_month_last_day:02d}'
    else: 
        prev_month_last_day = get_prev_month_last_day(args.year, args.month - 1)
        start = f'{args.year}-{(args.month - 1):02d}-{prev_month_last_day:02d}'
    
    # end
    if args.month == 12:
        end = f'{args.year+1}-{1:02d}-03'      
    else:
        end = f'{args.year}-{(args.month + 1):02d}-03'

    # prepare queries
    if args.mode == 'full':
        dates = get_dates(start, end)
        queries = [get_query(t_name, d) for d in dates]
        names = [dates[i] for i, q in enumerate(queries)]
    else:
        queries = [get_query_billing_label(t_name, start, end, md['label'].iloc[i]) for i in range(md.shape[0])]
        names = [md.iloc[i]['description'] for i, q in enumerate(queries)]
        
    return queries, names

def prepare_batches(args, queries, names, lookup, dirout):
    '''Prepare batches to be processed in parallel'''
    
    fetch_type = 'date' if args.mode == 'full' else 'description'
    
    batches = []
    prev_runs = []
    total_bytes_processed = []
    
    for i,q in enumerate(queries):
        if args.check_prev_runs and not args.dry_run:
            pattern = re.sub('[^A-Za-z0-9]+', "_", names[i].lower())
            prev = lookup[lookup['basename'].apply(lambda x: pattern + '.tsv' == x)]
            if not prev.empty:
                fname = list(prev['path'])[0]
                prev_runs.append(fname)
                log.info(f'Found previously saved result for {names[i]} - use it {fname}')
            continue
        
        r = run_query(q, client, dry_run = True)
        p = 0 if r.total_bytes_processed is None else  r.total_bytes_processed
        log.info(f"[BQ] {names[i]} - bytes processed: {p} ({round(p * 1e-09, 4)} GB)")
        
        # get batches construct
        batches.append((
            q, client.project, i, names[i], fetch_type, 
            invoice_month, args.chunk_size, dirout,
            args.save_raw, 
            args.save_removed
        ))
        
        total_bytes_processed.append(p)
    
    if args.check_prev_runs:
        reuse = [f for f in set(list(lookup['basename']))]
    else:
        reuse = []
        
    log.info(f'In total {len(batches)} batches prepared for processing - found {len(reuse)} reports from previous runs to reuse.\n')
    
    return batches, prev_runs, total_bytes_processed

class BlankLinesHelpFormatter (argparse.RawTextHelpFormatter):
    def _split_lines(self, text, width):
        return super()._split_lines(text, width) + ['']

def parse_args():
    ''' Parse arguments '''

    parser = argparse.ArgumentParser(
        description='Script prepares monthly billing report.',
        usage='use "python %(prog)s --help" for more information',
        formatter_class=BlankLinesHelpFormatter
    )

    parser.add_argument('--p', '--project', 
                        dest='project_id', 
                        required=True,
                        help= textwrap.dedent('''\
                        PROJECT ID in which BQ billing table is stored.'''))
    
    parser.add_argument('-d', '--dataset', 
                        dest='dataset_id', 
                        required=True,
                        help= textwrap.dedent('''\
                        BigQuery dataset name (e.g. fimm_billing_data).'''))
    
    parser.add_argument('-t', '--table', 
                        dest='table_id', 
                        required=True,
                        help= textwrap.dedent('''\
                        BigQuery table name.'''))
        
    parser.add_argument('-y', '--year', 
                        dest='year', 
                        required=True,
                        type=int,
                        help= textwrap.dedent('''\
                        Extract records for a given year.'''))
    
    parser.add_argument('-m', '--month', 
                        dest='month', 
                        required=True, 
                        type=int,
                        help= textwrap.dedent('''\
                        Extract records for a given month number (1-12).'''))
    
    parser.add_argument('-o', '--dirout', 
                        dest='dirout', 
                        required=False, 
                        default=os.getcwd(),
                        help= textwrap.dedent('''\
                        Path to output file for saving report - not required if running with
                        `--dry-run True`.'''))

    parser.add_argument('-dry', '--dry-run', 
                        dest='dry_run', 
                        required=False,
                        default=True, 
                        type=str2bool,
                        help= textwrap.dedent('''\
                        Run queries in the DRY_RUN mode, i.e. no actual queries are executed.
                        Used for estimating costs of sql queries.'''))
    
    parser.add_argument('-mode', '--mode', 
                        required=True, 
                        choices=['full', 'metadata_billing_label'], 
                        default='full',
                        help= textwrap.dedent('''\
                        Choose the mode to run: full (extracts all available data from the BQ
                        table  for a given month)  or selected (extracts  records for billing 
                        labels specified in the metadata file).'''))
    
    parser.add_argument('-md', '--metadata', 
                        required=True,
                        help= textwrap.dedent('''\
                        Metadata file storing project labels and description.'''))

    parser.add_argument('-b', '--bucket', 
                        required=True, 
                        help= textwrap.dedent('''\
                        Extract metadata from the given bucket omiting gs:// prefix.'''))

    parser.add_argument('-r', '--remove', 
                        dest='remove', 
                        required=False,
                        default=False, 
                        type=str2bool,
                        help= textwrap.dedent('''\
                        Remove temporary files that are used for creating a final report (e.g.
                        extracted costs per day / billing label. These will be saved to a file
                        invoice<YEARMONTH>_ts_<TIMESTAMP> under the dirout folder.'''))

    parser.add_argument('-c', '--max-cpu', 
                        dest='max_cpu', 
                        required=False, 
                        default=6, 
                        type=int, 
                        help= textwrap.dedent('''\
                        Max CPUs to use.'''))

    parser.add_argument('-n', '--chunk-size', 
                        dest='chunk_size',
                        required=False, 
                        default=10000, 
                        type=int, 
                        help= textwrap.dedent('''\
                        When the table is too large - extract data in chuncks of this size.'''))

    parser.add_argument('-check', '--check-prev-runs', 
                        dest='check_prev_runs', 
                        required=False,
                        default=False, type=str2bool, 
                        help= textwrap.dedent('''\
                        Check previous runs - will be reused if available.'''))
    
    parser.add_argument('-sa', '--save-raw', 
                        dest='save_raw', 
                        required=False,
                        default=False, 
                        type=str2bool, 
                        help= textwrap.dedent('''\
                        Save unprocessed, unaggregated rows extracted directly from the BQ.'''))
    
    parser.add_argument('-se', '--save-removed', 
                        dest='save_removed', 
                        required=False,
                        default=False, 
                        type=str2bool, 
                        help= textwrap.dedent('''\
                        Save rows that are omitted from final report due to umbigous billing
                        labels throughout the month.'''))

    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()
    
    print('\n' + ('').center(80, '='))

    start_ts = datetime.datetime.now()
    log.info("STARTING BILLING REPORT PREPARATION\n")

    # inititalize invoice month, bq table name and bq client
    client = bigquery.Client(project=args.project_id)
    invoice_month = f'{args.year}{args.month:02d}'

    md = get_metadata_from_bucket(args.bucket, args.metadata)
    
    dirout = create_tmp_dir(args.dirout, invoice_month) if not args.dry_run else ''

    queries, names = prepare_queiries(args, md)
    
    lookup = get_prev_runs(dirout, args.dirout, names) if args.check_prev_runs else pd.DataFrame()

    batches_list = prepare_batches(args, queries, names, lookup, dirout)

    # process batches in parallel if not dry run
    total_billed = [0]
    total_rows = [0]
    if not args.dry_run:

        summarize_bq_costs_info(batches_list[2], 'processed')
        
        cpus = min(args.max_cpu, multiprocessing.cpu_count())
        log.info(f"Using {cpus} CPUs.")

        with Pool(processes=cpus) as pool:
            reports, total_billed, total_rows =  zip(*pool.map(multiproc_wrapper, batches_list[0]))
            reports = [f for f in reports if f is not None]

            # combine files from previous and current runs
            reports_all = reports + batches_list[1]
        
        # prepare finale repors
        df_report, exclude_projects_list = combine_reports(reports_all)

        df_report, df_excluded = remove_project_wt_multiple_billing_labels(
            df_report, exclude_projects_list
        )

        result = aggregate_and_prepare_excel_sheets(df_report, md)

        save_result_to_excel(result, invoice_month, dirout, args.mode)
        if not df_excluded.empty:
            save_errors_to_excel(df_excluded, dirout, args.mode)

        summarize_costs_info(result)
        
        if args.remove:
            remove_temp_files(dirout)
    
    print('\n' + ('').center(80, '='))
    log.info('SCANNING IS COMPLETED\n')

    log.info(f"(BQ) Total rows processed {sum(total_rows)}")
    summarize_bq_costs_info(batches_list[2], 'processed')
    summarize_bq_costs_info(total_billed, 'billed')
    
    dt = round((datetime.datetime.now() - start_ts).seconds/ 60, 2)
    log.info(f"Total processing time: {dt} mins")


