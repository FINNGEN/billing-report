#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import warnings
import argparse
import calendar
import datetime
import tempfile
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from google.cloud import bigquery
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger()

warnings.filterwarnings('ignore')
NCHARS = 40


def calculate_costs_and_discounts(qdf):
    '''Calculate cost and discount from the BQ query dataframe'''
    
    # calculate costs
    cost = qdf['cost'].sum()
    
    # calculate credits
    credits = qdf.loc[qdf['credits'].apply(lambda x: len(x) > 0)]
    discounts = [sum([el['amount'] for el in row]) for row in credits['credits']]
    discount = sum(discounts)
    total_cost = cost + discount
    
    attr = ['name', 'id', 'billing_label', 'wbs']
    for a in attr:
        assert len(set(qdf[a])) == 1
    d = {a: list(set(qdf[a]))[0] for a in attr} 
    
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

def aggregate_by_project(df, date):
    '''Aggregate biiling data in the data frame'''
    res = []
    if df.shape[0] > 0:
        grouped = df.groupby(df['id'])
        for p in grouped.groups.keys():
            d = grouped.get_group(p)
            r = calculate_costs_and_discounts(d) 
            r['date'] = date
            res.append(r)
    return res

def run_query(qdate, query, client, invoice_month, dry_run=False):
    '''Run BQ query and return query result as dataframe, data billed / data read in GB'''
    
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
    
    log.info(f"\n[BQ] Total bytes billed (date {qdate}): {b} ({round(b * 1e-09, 4)} GB)")
    log.info(f"[BQ] Total bytes processed (date {qdate}): {p} ({round(p * 1e-09, 4)} GB)")
    
    return b, p, df

def get_month_last_day(year, month):
    '''Get last day of the given month in a given year'''
    
    cal = calendar.monthcalendar(year, month)
    days = [d for w in cal for d in w  if d != 0]
    days.reverse()
    end = days[0]
    
    return end

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

def save_df(lst, qdate, dirout):
    '''Save date report'''
    cols = [lst[i].keys() for i,e in enumerate(lst) if len(lst[i]) > 0][0]
    df = pd.DataFrame.from_records(lst, columns=cols)
    
    fout = os.path.join(dirout, f'{qdate}.csv')
    if df is not None:
        df.to_csv(fout, sep='\t', index=False)

def process_query(i, query, qdate, invoice_month, project_id, dry_run, tmpfile, dir=''):
    '''Process single date query to extract labeled/unlabeled/wbs sums'''

    s = datetime.datetime.now()
    log.info(f"Batch {i}: Start processing date {qdate}" )
    
    # get client
    client = bigquery.Client(project=project_id)
    
    # fetch data from the BQ
    billed, processed, df = run_query(qdate, query, client, invoice_month, dry_run)

    # append the line to the report
    with open(tmpfile, 'a') as f:
        f.write(f"{qdate}\t{processed}\t{billed}\n")
    
    # unnest billing label and aggreagate data by project_id
    if df is not None:
        if len(df) > 0:
            res = unnest_labels(df)
            aggr = aggregate_by_project(res, date = qdate)
            save_df(aggr, qdate, dir)
        
    e = datetime.datetime.now()
    log.info(f"Batch {i}: Date {qdate} proccessed in {round((e - s).seconds / 60, 2)} mins" )
    
    return processed, billed

def create_tmp_dir(dirout, invoice_month):
    ts = datetime.datetime.now().strftime("%d%m%YT%H%M%S")
    tmp = os.path.join(dirout, f"invoice{invoice_month}_ts_{ts}")
    if not os.path.exists(tmp):
        os.makedirs(tmp) 
    return tmp

def summarize_costs(l, labels = ['GCP projects', 'Billing Label', 'WBS']):
    '''Summarize costs per each label'''
    log.info("\nTotals:")
    for i in range(len(l)):
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
    cols = [by] + list(res.columns)
    res[by] = res.index
    res = res.reset_index(drop=True)
    res = res[cols]
    if len(drop_cols) > 0:
        res = res.drop(columns = drop_cols)
    
    return res

def combine_dates(tmpdir):
    '''Read files per date, combine and aggregate data per project/billing label/wbs'''
    
    # list all files created in the tmp folder
    files = [os.path.join(tmpdir, f) for f in os.listdir(tmpdir)]
    
    # combine
    l = []
    log.info("\nStarting to combine data across the dates.")
    for f in files:
        x = pd.read_csv(f, sep='\t', header=0, dtype={'wbs': 'string','billing_label': 'string'})
        log.info(f"Reading aggregated data for the date {os.path.basename(f).split('.')[0]} - read {x.shape[0]} records.")
        l.append(x)
    
    combined = pd.concat(l)
    combined = combined.reset_index(drop=True) 
    
    # aggregate per attribute
    pr = aggregate_by_attribute(combined, by = 'project_id', drop_cols=['date'])
    lab = aggregate_by_attribute(combined, by = 'billing_label', drop_cols=['date'])
    wbs = aggregate_by_attribute(combined, by = 'wbs', drop_cols=['date'])
    
    # append total to project
    res = []
    for t in [pr, lab, wbs]:
        t['total'] = [''] * len(t)
        t['total'][len(t)-1] = t['total_cost'].sum()
        res.append(t)
    
    return res

def save_excel(df_list, invoice_month, dirout):
    ''' Save dataframes to the excel file'''
    
    fname = f"report_{invoice_month}.xlsx"
    fout = os.path.join(dirout, fname)
    with pd.ExcelWriter(fout) as writer:
        df_list[0].to_excel(writer, sheet_name="by GCP project", index=False)
        df_list[1].to_excel(writer, sheet_name="by billing label", index=False)
        df_list[2].to_excel(writer, sheet_name="by WBS", index=False)
        log.info(f'\nSaved report to {fout}')

def parse_args():
    ''' Parse arguments '''

    parser = argparse.ArgumentParser(
        description=('Script prepares monthly billing report (full version).'),
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
    
    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()
    
    start = datetime.datetime.now()
    log.info("Started")

    # inititalize invoice month, bq table name and bq client
    invoice_month = f'{args.year}{args.month:02d}'
    t_name = f"{args.project_id}.{args.dataset_id}.{args.table_id}"
    
    # create tmp dir and tmp file for storing daily data and bq costs per query
    tmpdir = create_tmp_dir(args.dirout, invoice_month) if not args.dry_run else ''
    tmpfile = tempfile.NamedTemporaryFile(prefix='_bqcosts_', dir=args.dirout)
    with open(tmpfile.name, 'w') as f:
        f.write(f"DATE\tBQ_PROCESSED_BYTES\tBQ_BILLED_BYTES\n")

    # get dates to extract data from
    dates = get_dates(args.year, args.month)
    log.info(f"Prepared {len(dates)} dates to extract the data")

    # get all queries definitions
    queries = [get_query(t_name, d) for d in dates]
    batches = [(i, q, dates[i], invoice_month, args.project_id, args.dry_run, tmpfile.name, tmpdir) for i, q in enumerate(queries)]

    # process batches in parallel
    cpus = multiprocessing.cpu_count()
    with Pool(processes=cpus) as pool:
        pool.map(multiproc_wrapper, batches)
    
    # aggregate data and 
    if not args.dry_run:

        # combine across the dates
        result = combine_dates(tmpdir)

        # save to excel
        save_excel(result, invoice_month, args.dirout)

        # summarize costs
        summarize_costs(result)
    
    # summarize BQ costs
    summarize_bq_costs(tmpfile.name)

    # print total processing time
    dt = round((datetime.datetime.now() - start).seconds/ 60, 2)
    log.info(f"\nTotal processing time: {dt} mins")


