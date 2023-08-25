#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import sys
import logging
import warnings
import argparse
import calendar
import pandas as pd
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger()

warnings.filterwarnings('ignore')

def summarize_results(qdf):
    '''Summarize result from the BQ query dataframe'''
    
    qdf['wbs'] = qdf['label'].apply(lambda x: x.split('_')[0])
    wbs = ','.join(list(set(qdf['wbs'])))
    
    # calculate costs
    cost = qdf['cost'].sum()
    
    # calculate credits
    credits = qdf.loc[qdf['credits'].apply(lambda x: len(x) > 0)]
    discounts = [sum([el['amount'] for el in row]) for row in credits['credits']]
    discount = sum(discounts)

    total_cost = cost + discount
    
    pid = ','.join(set(list(qdf['id'])))
    pname = ','.join(set(list(qdf['name'])))
    billing_id = ','.join(set(list(qdf['label'])))
    description = ','.join(set(list(qdf['description'])))
    
    # keys and values
    res = {
        'bq billing label': billing_id,
        'wbs': wbs,
        'description': description,
        'BQ entries found for a given time frame': True,
        'project_id': pid,
        'project_name': pname,
        'cost': cost,
        'discount': discount,
        'total_cost': total_cost
    }
    
    return res

def calculate(df, by='name'):
    res = []
    if df.shape[0] > 0:
        grouped = df.groupby(df[by])
        for p in grouped.groups.keys():
            d = grouped.get_group(p)
            r = summarize_results(d) 
            res.append(r)
    
    return res

def run_query(query, client, label, description, invoice_month, dry_run=False):
    '''Run BQ query and return query result as dataframe, data billed / data read in GB'''

    wbs = label.split('_')[0]
    
    if (dry_run):
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        q = client.query(query, job_config=job_config)
        
        # return empty values
        df = None
        summary_proj = []
        summary_wbs = []
   
    else:
        q = client.query(query)
        df = q.to_dataframe()

        # add project labels and description
        df['label'] = label
        df['wbs'] = wbs
        df['description'] = description
        
        # filter by invoice month
        ids = df['invoice'].apply(lambda x: x['month'] == invoice_month)
        df = df[ids]

        # calculate costs per WBS and per GCP project
        summary_proj = calculate(df, by='name')
        summary_wbs = calculate(df, by='wbs')

        # if no data was extracted
        if len(summary_proj) == 0 and len(summary_wbs) == 0:
            empty_record = {
                'bq billing label': label,
                'wbs': wbs,
                'description': description,
                'BQ entries found for a given time frame': False,
                'project_id': pd.NA,
                'project_name': pd.NA,
                'cost': 0,
                'discount': 0,
                'total_cost': 0
            }
            summary_proj = summary_wbs = [empty_record]
        
    log.info(f"\t\tTOTAL BYTES BILLED FOR THE QUERY: {q.total_bytes_billed} ({q.total_bytes_billed * 1e-09} GB)")
    
    return summary_proj, summary_wbs, q.total_bytes_billed, q.total_bytes_processed, df

def get_metadata(bucket_name, fname):
    '''Get metadata file from the GCS bucket'''
    client_gsc = storage.Client()
    bucket = client_gsc.get_bucket(bucket_name)
    blob = bucket.get_blob(fname)
    data = blob.download_as_string()
    dat = pd.read_csv(io.StringIO(data.decode()), sep='\t')

    return dat

def get_month_last_day(year, month):
    '''Get last day of the given month in a given year'''
    cal = calendar.monthcalendar(year, month)
    days = [d for w in cal for d in w  if d != 0]
    days.reverse()
    end = days[0]

    return end

def get_query(table_name, start_date, end_date, label):
    '''
    Get cost and discount queries given 
    - BQ table name
    - Start date
    - End date
    - Project label
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

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def save_df(dat, label, invoice_month, dirout):
    fout = f'{dirout}/billing.label.{label}.{invoice_month}.csv'
    if dat is not None:
        dat.to_csv(fout, sep=',', index=False)


def main():

    # parse the arguments
    parser = argparse.ArgumentParser(description='Script prepares billing report.')

    # parameters used for modules and runs specifications
    parser.add_argument('-p', '--project', help='PROJECT ID in which BQ billing table is stored.', required=True)
    parser.add_argument('-d', '--dataset', help='BigQuery dataset name (e.g. fimm_billing_data).', required=True)
    parser.add_argument('-t', '--table', help='BigQuery table name (e.g. gcp_billing_export_v1_015819_A39FE2_F94565).', required=True)
    parser.add_argument('-md', '--metadata', help='Metadata file storing project labels and description.', required=True)
    parser.add_argument('-b', '--bucket', help='Extract metadata from the given bucket omiting gs:// prefix.', required=True)
    parser.add_argument('-y', '--year', help='Extract records for a given year.', required=True)
    parser.add_argument('-m', '--month', help='Extract records for a given month (number)', required=True)
    parser.add_argument('-dry', '--dry-run', 
                        default=True, required=False, type=str2bool,
                        help="Run queries in the DRY_RUN mode, i.e. no actual queries are executed. Used for estimating costs of sql queries")
    parser.add_argument('-o', '--dirout', required=False, help="Path to output file for saving report - not required if running with `--dry-run True`.")
    parser.add_argument('-s', '--save', default=False, required=False, type=str2bool,
                        help="Save the extracted data frame to a separate CSV file.")

    # initialise variables
    args = vars(parser.parse_args())
    project_id = args['project']
    dataset_id = args['dataset']
    table_id = args['table']
    bucket_name = args['bucket']
    metadata_fname = args['metadata']
    year = int(args['year'])
    month = int(args['month'])
    dirout = args['dirout']
    dry_run = str2bool(args['dry_run'])
    chars = 40

    if dry_run:
        log.info('\nActivated dry run - NO SLQ queries will be executed')
    
    if not dry_run and dirout is None:
        log.error(f'\nPlease, provide output directory when running with `--dry-run False` option!\n')
        sys.exit()
    
    log.info('\n' + ('').center(50, '='))
    log.info("STARTING BILLING REPORT PREPARATION")

    # initialize and bq table name and bucket name
    t_name = f"{project_id}.{dataset_id}.{table_id}"
    if bucket_name.startswith('gs://'):
        bucket_name = bucket_name.replace('gs://', '')
    
    # init bq client
    client_bq = bigquery.Client(project=project_id)

    # read metadata from the bucket
    try:    
        m = get_metadata(bucket_name, metadata_fname)
    except Exception as e:
        log.error(f'Error ocurred while fetching the metadata : {e}')
        sys.exit()
    else:
        log.info('Succesfully read metadata file')

    # get formatted start and end dates
    start_day = get_month_last_day(year, month - 1)
    invoice_month = f'{year}{month:02d}'

    # format start and end dates
    start_date = f'{year}-{(month - 1):02d}-{start_day:02d}'
    end_date = f'{year}-{(month + 1):02d}-01'
    log.info(f"\nExtracting records from {start_date} to {end_date}")

    # go thruogh metadata entries
    rows_label = []
    rows_proj = []
    total_bytes_billed = 0
    total_bytes_processed = 0
    for i in range(m.shape[0]):
        billing_label = m.iloc[i]['label']
        description = m.iloc[i]['description']
        log.info(f"\tExtracting costs for the project: {m.iloc[i]['description']}, {billing_label}")

        # prepare bq queries based on project id
        query = get_query(
            t_name, start_date, end_date, billing_label
        )

        # calculate costs for the specified period of time
        try:
            proj, wbs, bytes_billed, bytes_processed, dat = run_query(
                query, 
                client_bq,
                label = billing_label, 
                description = description,
                invoice_month = invoice_month,
                dry_run = dry_run
            )
        except Exception as e:
            log.info(f"Something went wrong while extracting costs for \
                     the project with label {billing_label} :: {e}")
            proj, wbs, bytes_billed, bytes_processed, dat = [], [], 0, 0, None

        total_bytes_billed += bytes_billed
        total_bytes_processed += bytes_processed

        # save table if specified 
        if args['save']:
            save_df(dat, billing_label, invoice_month, dirout)
        
        # append extracted values to the final report
        rows_proj = rows_proj + proj
        rows_label = rows_label + wbs

    # second round of summary
    dpr = pd.DataFrame(rows_proj)
    dlab = pd.DataFrame(rows_label)
    
    if not dry_run:

        # total costs
        tot_proj = dpr['total_cost'].sum()
        tot_wbs = dlab['total_cost'].sum()
    
        # aggregate by wbs
        dwbs = pd.concat([
            dlab[['wbs', 'bq billing label', 'description']].groupby('wbs').agg(lambda x: ', '.join(set(list(x)))),
            dlab[['wbs', 'cost', 'discount', 'total_cost']].groupby('wbs').sum()
        ], axis = 1)

        # re-order columns
        dwbs['wbs'] = dwbs.index
        cols_ord = ['wbs', 'bq billing label', 'description', 'cost', 'discount', 'total_cost']
        dwbs = dwbs[cols_ord]

        # add total sums
        dpr = pd.concat([dpr, pd.DataFrame({'TOTAL COST SUM': tot_proj}, index = [0])], ignore_index=True)
        dwbs = pd.concat([dwbs, pd.DataFrame({'TOTAL COST SUM': tot_wbs}, index = [0])], ignore_index=True)
            
        fout = f"{dirout}/report_{invoice_month}.xlsx"

        with pd.ExcelWriter(fout) as writer:
            dpr.to_excel(writer, sheet_name="by GCP project", index=False)
            dwbs.to_excel(writer, sheet_name="by WBS", index=False)
            log.info(f'\nSaved report to {fout}')
    
    else:
        tot_proj = pd.NA
        tot_wbs = pd.NA
    
    log.info('\n' + ('').center(50, '='))
    log.info('SCANNING OF THE MONTH IS COMPLETED')

    total_processed_gb = round(total_bytes_processed * 1e-9, 2)
    total_billed_gb = round(float(total_bytes_billed) * 1e-9, 2)
    total_processed_tb = total_bytes_processed * 1e-12
    total_billed_tb = float(total_bytes_billed) * 1e-12

    total_processed_cost = total_processed_tb * 5
    total_billed_cost = float(total_billed_tb) * 5
    
    log.info(f'\n{"Total costs (GCP projects):".ljust(chars)}{tot_proj}')
    log.info(f'{"Total costs (WBS):".ljust(chars)}{tot_wbs}')
    log.info(f'\n{"(BQ) Total data processed:".ljust(chars)}{total_processed_gb} GB')
    log.info(f'{"(BQ) Total data billed:".ljust(chars)}{total_billed_gb} GB')
    log.info(f'{"(BQ) Estimated compute cost of queries:".ljust(chars)}{total_processed_cost} $')
    log.info(f'{"(BQ) Billed compute cost of queries:".ljust(chars)}{total_billed_cost} $\n')

        
if __name__ == '__main__':
    main()
