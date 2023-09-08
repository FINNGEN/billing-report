#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import io
import os
import sys
import logging
import warnings
import argparse
import calendar
import datetime
import tempfile
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from google.cloud import storage
from google.cloud import bigquery
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format='%(message)s')
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
        assert len(set(qdf[a])) == 1, f"\tERROR when aggregating by PROJECT_ID " + \
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
    pid_error = []
    if df.shape[0] > 0:
        grouped = df.groupby(df['id'])
        for p in grouped.groups.keys():
            d = grouped.get_group(p)                
            try:
                r = calculate_costs_and_discounts(d, value)
            except AssertionError as e:
                log.error(e)
                pid_error.append({
                    'project_id': p, 
                    'billing_label': ';'.join(list(set(d['billing_label']))),
                    'wbs': ';'.join(list(set(d['wbs']))),
                    'value': value
                })
            else:
                r[key] = value
                res.append(r)
    return res, pid_error

def run_query(date_or_billing_lab, query, client, invoice_month, dry_run=False):
    '''Run BQ query and return query result as dataframe, data billed / data read in GB (term - either date or a billing label)'''
    
    if (dry_run):
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        q = client.query(query, job_config=job_config)
        df = None
    else:
        q = client.query(query)
        df = q.to_dataframe()
        
        # filter by invoice month
        ids = df['invoice'].apply(lambda x: x['month'] == invoice_month)
        df = df[ids]
        if len(df) > 0:
            df = df.reset_index(drop=True) 
    
    b = q.total_bytes_billed
    p = q.total_bytes_processed
    
    log.info(f"[BQ] {date_or_billing_lab} - total bytes billed {b} ({round(b * 1e-09, 4)} GB) / processed: {p} ({round(p * 1e-09, 4)} GB)")
    
    return b, p, df

def get_month_last_day(year, month):
    '''Get last day of the given month in a given year'''
    
    cal = calendar.monthcalendar(year, month)
    days = [d for w in cal for d in w  if d != 0]
    days.reverse()
    end = days[0]
    
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

def get_dates(year, month):
    '''Get dates in a month with 1 day overlap from prev/next months'''
    
    start_day = get_month_last_day(year, month - 1)
    start_dt = date(year, month-1, start_day)
    end_dt = date(year, month+1, 1)
    delta = timedelta(days=1)
    
    # store the dates between two dates in a list
    dates = []
    
    while start_dt <= end_dt:
        # add current date to list by converting  it to iso format
        dates.append(start_dt.isoformat())
        
        # increment start date by timedelta
        start_dt += delta
    
    return dates

def multiproc_wrapper(args):
    ''' Multiproc wrapper function '''
    
    try:
        res = process_query(*args)
    except Exception as e:
        log.error(f'ERROR while processing batch {args[0]}: {e}')
        return None
    else:
        return res

def save_df(lst, name, dirout):
    '''Save date report'''

    cols = [lst[i].keys() for i,e in enumerate(lst) if len(lst[i]) > 0][0]
    df = pd.DataFrame.from_records(lst, columns=cols)
    fname = re.sub('[^A-Za-z0-9]+', "_", name.lower())
    fout = os.path.join(dirout, f'{fname}.csv')
    if df is not None:
        df.to_csv(fout, sep='\t', index=False)

def process_query(i, query, date_or_billing_lab, date_or_billing_lab_name, 
                  invoice_month, project_id, dry_run, tmpfile, dir=''):
    '''Process single date query to extract labeled/unlabeled/wbs sums'''
    
    s = datetime.datetime.now()
    log.info(f"Batch {i}: Start processing {date_or_billing_lab}" )
    
    # get client
    client = bigquery.Client(project=project_id)
    
    # fetch data from the BQ
    billed, processed, df = run_query(
        date_or_billing_lab, query, client, invoice_month, dry_run
    )
    
    # append the line to the report
    with open(tmpfile, 'a') as f:
        f.write(f"{date_or_billing_lab}\t{processed}\t{billed}\n")
    
    # unnest billing label and aggreagate data by project_id
    pid_error = []
    if df is not None:
        if len(df) > 0:
            res = unnest_labels(df)
            aggr, pid_error = aggregate_by_project(
                res, 
                date_or_billing_lab_name, 
                date_or_billing_lab
            )
            save_df(aggr, date_or_billing_lab, dir)
        
    e = datetime.datetime.now()
    log.info(f"Batch {i}: {date_or_billing_lab} " + \
             f"proccessed in {round((e - s).seconds / 60, 2)} mins" )
    
    return pid_error

def create_tmp_dir(dirout, invoice_month):
    ts = datetime.datetime.now().strftime("%d%m%YT%H%M%S")
    tmp = os.path.join(dirout, f"invoice{invoice_month}_ts_{ts}")
    if not os.path.exists(tmp):
        os.makedirs(tmp) 
    return tmp

def summarize_costs(l, labels = ['GCP projects', 'Billing Label', 'WBS']):
    '''Summarize costs per each label'''
    log.info("\nTotals:")
    for i in range(len(labels)):
        by = labels[i]
        tot = l[i]['total'][l[i]['total'] != ''].iloc[0]
        log.info(f'\t{f"Total costs ({by}):".ljust(NCHARS)}{tot}')

def summarize_bq_costs(summary_file):
    
    log.info('\n' + ('').center(50, '='))
    log.info('SCANNING IS COMPLETED')
    
    # read file with collected costs
    x = pd.read_csv(summary_file, sep="\t", header=0)
    
    total_processed_gb = round(sum(x['BQ_PROCESSED_BYTES']) * 1e-9, 2)
    total_billed_gb = round(sum(x['BQ_BILLED_BYTES']) * 1e-9, 2)
    total_processed_tb = sum(x['BQ_PROCESSED_BYTES']) * 1e-12
    total_billed_tb = sum(x['BQ_BILLED_BYTES']) * 1e-12
    
    total_processed_cost = total_processed_tb * 5
    total_billed_cost = float(total_billed_tb) * 5
    
    log.info(f'\n{"(BQ) Total data processed:".ljust(NCHARS)}{total_processed_gb} GB')
    log.info(f'{"(BQ) Total data billed:".ljust(NCHARS)}{total_billed_gb} GB')
    log.info(f'{"(BQ) Estimated compute cost of queries:".ljust(NCHARS)}{total_processed_cost} $')
    log.info(f'{"(BQ) Billed compute cost of queries:".ljust(NCHARS)}{total_billed_cost} $\n')

def unique_list(l):
    '''Get unique elements in array preserving the order'''
    return list(dict.fromkeys(l))

def aggregate_by_attribute(df, by = 'project_id', drop_cols=[]):
    '''Aggreagate data across dates'''
    
    df = df.fillna('')
    cols_calc = list(df.columns[df.dtypes == 'float64'])
    cols_summarize = list(set(df.columns).difference(set(cols_calc)))
    cols_calc.append(by)
    
    res = pd.concat([
        df[cols_summarize].groupby(by).agg(lambda x: ';'.join(unique_list(list(x)))),
        df[cols_calc].groupby(by).sum()
    ], axis = 1)
    
    # re-order and drop index
    order = ['project_id', 'project_name', 'description', 'billing_label', 'wbs']
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

def exclude_umbigous_records(df, edf):
    '''Exclude umbigous records (e.g. one project id and multiple labels) before summarizing final costs'''
    
    # colnames
    bl = 'billing_label'
    pid = 'project_id'
    wbs = 'wbs'
    sep = '\n\t- '
    
    # exclude projects with umbigous multiple lables in wbs/billing label
    # in addition, drop the whole billing label/wbs
    ex_projects = unique_list(edf[pid])    
    
    bildup = edf[bl].duplicated()
    ex_billing_labs = flatten(edf[bl][~bildup].apply(lambda x: x.split(';')).values)
    
    wbsdup = edf[wbs].duplicated()
    ex_wbs = flatten(edf[wbs][~wbsdup].apply(lambda x: x.split(';') ).values)

    
    
    lw = "(labels: '" + edf[bl] + "' wbs: '" + edf[wbs]+ "')"
    erlist = sep + sep.join(edf[pid] + lw)
    log.error(f"\n{len(edf)} projects with multiple billing " +
        f"labels - exclude project(s) from the report: {erlist}")
    
    # remove rows with pid/billing lab/wbs fror error list
    ids = df.apply(
        lambda x: x.loc[pid] in ex_projects or x.loc[wbs] in ex_wbs \
            or x.loc[bl] in ex_billing_labs, axis=1)
    
    df_excluded = df[ids]
    df_excluded = df_excluded.reset_index(drop=True)

    df_filt = df[~ids]
    df_filt = df_filt.reset_index(drop=True)
    
    return df_filt, df_excluded


def combine_dates(tmpdir, metadata, err_df):
    '''Read files per date, combine and aggregate data per project/billing label/wbs'''
    
    # list all files created in the tmp folder
    files = [os.path.join(tmpdir, f) for f in os.listdir(tmpdir)]
    
    # combine
    l = []
    log.info("\nStarting to combine data across the dates.")
    for f in files:
        x = pd.read_csv(f, sep='\t', header=0,
                        dtype={'wbs': 'string','billing_label': 'string'})
        l.append(x)
    
    combined = pd.concat(l)
    combined = combined.fillna('')
    
    # if projects with errors were observed, exclude them altogether
    if len(err_df) > 0:
        combined, excluded = exclude_umbigous_records(combined, err_df)
        # aggregate per attribute
        pr_excluded = aggregate_by_attribute(
            excluded, by = 'project_id', drop_cols=['date']
        )
    else:
        pr_excluded = pd.DataFrame()
    
    # match the description
    metadata = md.copy()
    if 'description' not in combined.columns:
        metadata['billing_label'] = 'billing_' + metadata['label']
        metadata.index = metadata['billing_label']
        
        d = list(set(combined['billing_label']).difference(
            set(metadata['billing_label'])))
        v = [pd.NA] * len(d)    
        add = pd.DataFrame(list(zip(v, v, d)), 
                           columns = metadata.columns, 
                           index=d)
        
        metadata = pd.concat([metadata, add])
        combined['description'] = metadata.loc[
            combined['billing_label']
        ]['description'].values
    
    # aggregate per attribute
    pr = aggregate_by_attribute(
        combined, by = 'project_id', drop_cols=['date']
    )
    lab = aggregate_by_attribute(
        combined, by = 'billing_label', drop_cols=['date']
    )
    wbs = aggregate_by_attribute(
        combined, by = 'wbs', drop_cols=['date']
    )
    
    # append total to project
    res = []
    for t in [pr, lab, wbs]:
        t['total'] = [''] * len(t)
        t['total'][len(t)-1] = t['total_cost'].sum()
        res.append(t)
    
    return res, pr_excluded

def save_excel(df_list, invoice_month, dirout, mode):
    ''' Save dataframes to the excel file'''
    
    fname = f"report_{invoice_month}_{mode}.xlsx"
    fout = os.path.join(dirout, fname)
    with pd.ExcelWriter(fout) as writer:
        df_list[0].to_excel(writer, sheet_name="by GCP project", index=False)
        df_list[1].to_excel(writer, sheet_name="by billing label", index=False)
        df_list[2].to_excel(writer, sheet_name="by WBS", index=False)
        log.info(f'\nSaved report to {fout}')

def flatten(l):
    flat_list = [item for sublist in l for item in sublist]
    return flat_list

def get_metadata(bucket_name, m_filename):
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

def save_err(df_excluded, df_errors, dirout, mode):
    '''Save errors into a separate excel file'''

    fname = f"report_{invoice_month}_{mode}_ERRORS.xlsx"
    fout = os.path.join(dirout, fname)
    
    df_errors = df_errors.rename(columns = {'value': 'date'})
    
    # summarize
    pid = []
    wbs = []
    billing_label = []
    if len(df_errors) > 0:
        pid = unique_list(df_errors['project_id'].values)
        wbs = unique_list(df_errors['wbs'].apply(lambda x: x.split(';')).values)
        billing_label = unique_list(df_errors['billing_label'].apply(lambda x: x.split(';')).values)
    
    # save to a separte report
    with pd.ExcelWriter(fout) as writer:
        df_errors.to_excel(writer, sheet_name="errors", index=False)
        df_excluded.to_excel(writer, sheet_name="excluded data", index=False)
        pd.DataFrame({'project_id': pid}).to_excel(writer, sheet_name="excluded projects", index=False)
        pd.DataFrame({'billing_label': billing_label}).to_excel(writer, sheet_name="excluded billing labels", index=False)
        pd.DataFrame({'wbs': wbs}).to_excel(writer, sheet_name="excluded wbs", index=False)

        log.info(f'\nSaved report to {fout}')



def remove_temp_files(dir):
    '''Remove tmp files'''
    files = [os.path.join(dir, f) for f in os.listdir(dir)]
    for f in files:
        # os.remove(f)
        print("REMOVE file", f)

def parse_args():
    ''' Parse arguments '''

    parser = argparse.ArgumentParser(
        description=('Script prepares monthly billing report.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--p', '--project', dest='project_id', required=True,
                        help='PROJECT ID in which BQ billing table is stored.')

    parser.add_argument('-d', '--dataset', dest='dataset_id', required=True,
                        help='BigQuery dataset name (e.g. fimm_billing_data).')
    
    parser.add_argument('-t', '--table', dest='table_id', required=True,
                        help='BigQuery table name (e.g. gcp_billing_export_v1_015819_A39FE2_F94565).')
        
    parser.add_argument('-y', '--year', dest='year', required=True,type=int,
                        help='Extract records for a given year.')
    
    parser.add_argument('-m', '--month', dest='month', required=True, type=int,
                        help='Extract records for a given month (number)')

    parser.add_argument('-o', '--dirout', dest='dirout', required=False, 
                        default=os.getcwd(),
                        help="Path to output file for saving report - not required if running with `--dry-run True`.")
    
    parser.add_argument('-dry', '--dry-run', dest='dry_run', required=False,
                        default=True, type=str2bool,
                        help="Run queries in the DRY_RUN mode, i.e. no actual queries are executed. Used for estimating costs of sql queries")
    
    parser.add_argument('-mode', '--mode', required=True, 
                        choices=['full', 'metadata_billing_label'], default='full',
                        help='Choose the mode to run: full (extracts all available data from the BQ table for a given month) \
                            or selected (extracts records for billing labels specified in the metadata file). Default: "full"')
    
    parser.add_argument('-md', '--metadata', required=True,
                        help='Metadata file storing project labels and description.')
    
    parser.add_argument('-b', '--bucket', required=True, 
                        help='Extract metadata from the given bucket omiting gs:// prefix.')
    
    parser.add_argument('-r', '--remove', dest='remove', required=False,
                        default=False, type=str2bool,
                        help="Remove temporary files that are used for creating a final report (e.g. extracted costs per day/billing label. " + \
                            "These will be saved to invoice<YEARMONTH>_ts_<TIMESTAMP> under the dirout folder)")

    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()
    
    start_ts = datetime.datetime.now()
    log.info('\n' + ('').center(50, '='))
    log.info("STARTING BILLING REPORT PREPARATION\n")

    # inititalize invoice month, bq table name and bq client
    invoice_month = f'{args.year}{args.month:02d}'
    t_name = f"{args.project_id}.{args.dataset_id}.{args.table_id}"

    # read metadata from the bucket
    md = get_metadata(args.bucket, args.metadata)
    
    # create tmp dir and tmp file for storing daily data and bq costs per query
    dirout = create_tmp_dir(args.dirout, invoice_month) if not args.dry_run else ''
    tmpfile = tempfile.NamedTemporaryFile(prefix='_bqcosts_', dir=args.dirout)
    
    with open(tmpfile.name, 'w') as f:
        _ = f.write(f"DATE\tBQ_PROCESSED_BYTES\tBQ_BILLED_BYTES\n")

    # if full mode - generate queries to extract data for each day
    if args.mode == 'full':

        # get dates
        dates = get_dates(args.year, args.month)
        
        # get queries
        queries = [get_query(t_name, d) for d in dates]
        
        # get batches
        batches = [(i, q, dates[i], 'date', invoice_month, args.project_id, 
                    args.dry_run, tmpfile.name, dirout) for i, q in enumerate(queries)]
    
    # else generate queries to extract data per billing label
    else:

        # get start & end dates
        prev_month_last_day = get_month_last_day(args.year, args.month - 1)
        start = f'{args.year}-{(args.month - 1):02d}-{prev_month_last_day:02d}'
        end = f'{args.year}-{(args.month + 1):02d}-01'        
        
        # get queries
        queries = [get_query_billing_label(t_name, start, end, md['label'].iloc[i]) for i in range(md.shape[0])]
        
        # get batches
        batches = [(i, q, md.iloc[i]['description'], 'description', invoice_month, 
                    args.project_id, args.dry_run, tmpfile.name, dirout) for i, q in enumerate(queries)]
    
    log.info(f"Prepared {len(batches)} batches to process")
        
    # process batches in parallel
    cpus = multiprocessing.cpu_count()
    with Pool(processes=cpus) as pool:
        e = pool.map(multiproc_wrapper, batches)
        df_errors = pd.DataFrame(flatten(e))
    
    # aggregate data and 
    if not args.dry_run:

        # combine across the dates
        result, df_excluded = combine_dates(dirout, md, df_errors)

        # remove temp files if needed
        if args.remove:
            remove_temp_files(dirout)

        # save to excel
        save_excel(result, invoice_month, dirout, args.mode)

        # save errors separately
        save_err(df_excluded, df_errors, dirout, args.mode)

        # summarize costs
        summarize_costs(result)
    
    # summarize BQ costs
    summarize_bq_costs(tmpfile.name)

    # print total processing time
    dt = round((datetime.datetime.now() - start_ts).seconds/ 60, 2)
    log.info(f"\nTotal processing time: {dt} mins")


