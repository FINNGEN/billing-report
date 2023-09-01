# Generate Monthly Billing Report

Script for generating monthy billing report for the FIMM projects. Script reads metadata file from the google cloud storage bucket and extracts cost records from the BQ table for a given project by label (WBS). Metadata is a tab-separated CSV file with two columns, `label` (i.e. WBS) and `description`:
```
label	description
111111	Some description about the project 1
222222	Some description about the project 2
```

Run the following to authenticate to google cloud sdk and install the packages:

1. Authenticate: `gcloud auth application-default login`

2. Install python packages: `pip3 install -r scripts/requirements.txt`

## Version 1: extract only specified projects marked by billing label in BQ

This version of the tool extracts only projects with the billing label specified in the provided input metadata. Generates summary of the costs on two levels: 
- Summary by project_id
- Summary by WBS


Run billing report script ver1: extract only billing labels that are specified in the metadata:

Estimate the costs implied by the script for extracting the data from BQ (no queries are actually excuted):
```
scripts/billing_report.py \
--project PROJECT_ID_CONTAINING_BQ_TABLE \
--dataset BQ_DATASET_NAME \
--table BQ_TABLE_NAME \
--metadata METADATA_CSV \
--bucket GCP_BUCKET_WITH_METADATA_FILE \
--year 2023 \
--month 7 \
--dry-run True 
```

Once you familiarize yourself with the expected costs, run the following command:
```
scripts/billing_report.py \
--project PROJECT_ID_CONTAINING_BQ_TABLE \
--dataset BQ_DATASET_NAME \
--table BQ_TABLE_NAME \
--metadata METADATA_CSV \
--bucket GCP_BUCKET_WITH_METADATA_FILE \
--year 2023 \
--month 7 \
--dry-run False

```

**V1 USAGE**

```
Script prepares billing report.

options:
  -h, --help            show this help message and exit
  -p PROJECT, --project PROJECT
                        PROJECT ID in which BQ billing table is stored.
  -d DATASET, --dataset DATASET
                        BigQuery dataset name (e.g. fimm_billing_data).
  -t TABLE, --table TABLE
                        BigQuery table name (e.g. gcp_billing_export_v1_015819_A39FE2_F94565).
  -md METADATA, --metadata METADATA
                        Metadata file storing project labels and description.
  -b BUCKET, --bucket BUCKET
                        Extract metadata from the given bucket omiting gs:// prefix.
  -y YEAR, --year YEAR  Extract records for a given year.
  -m MONTH, --month MONTH
                        Extract records for a given month (number)
  -dry DRY_RUN, --dry-run DRY_RUN
                        Run queries in the DRY_RUN mode, i.e. no actual queries are executed. Used for estimating costs of sql queries
  -o DIROUT, --dirout DIROUT
                        Path to output file for saving report - not required if running with `--dry-run True`.
  -s SAVE, --save SAVE  Save the extracted data frame to a separate CSV file.

```

## Version 2: extract all projects from BQ which includes unlabled projects as well

This version of the tool extracts **all** records for the given month and gives the summary of the costs on three levels: 
- Summary by project_id
- Summary by billing label (e.g. "<WBS_NUMBER>_<SPECIFIC_LABEL>") - if there is no label, the costs for these unlabeled projects will be calculated in the separate row of the resulted excel table
- Summary by WBS

Output excel table will have 3 sheets: 'by GCP project', 'by billing label', 'by WBS'. 

The metadata file is not required in this case. Run the following command (specify `-dry-run True` as in the other version if needed):

```
scripts/billing_report_full.py \
--project PROJECT_ID_CONTAINING_BQ_TABLE \
--dataset BQ_DATASET_NAME \
--table BQ_TABLE_NAME \
--year 2023 \
--month 7 \
--dry-run False
```

The output of the script (collapsed):
```
Prepared 33 dates to extract the data
Batch 0: Start processing date 2023-06-30
Batch 2: Start processing date 2023-07-02
...
[BQ] Total bytes billed (date 2023-07-10): 22020096 (0.022 GB)
[BQ] Total bytes processed (date 2023-07-10): 21919700 (0.0219 GB)
...

Saved report to <PATH>/report_202307.xlsx

Totals:
        Total costs (GCP projects):             68269.97126800001
        Total costs (Billing Label):            68269.97126800001
        Total costs (WBS):                      68269.971268

==================================================
SCANNING IS COMPLETED

(BQ) Total data processed:              1.88 GB
(BQ) Total data billed:                 1.9 GB
(BQ) Estimated compute cost of queries: 0.009407569335000001 $
(BQ) Billed compute cost of queries:    0.009484369920000001 $


Total processing time: 10.97 mins
```

**V2 USAGE**

```
Script prepares monthly billing report (full version).

options:
  -h, --help            show this help message and exit
  --p PROJECT_ID, --project PROJECT_ID
                        PROJECT ID in which BQ billing table is stored. (default: None)
  -d DATASET_ID, --dataset DATASET_ID
                        BigQuery dataset name (e.g. fimm_billing_data). (default: None)
  -t TABLE_ID, --table TABLE_ID
                        BigQuery table name (e.g. gcp_billing_export_v1_015819_A39FE2_F94565). (default: None)
  -y YEAR, --year YEAR  Extract records for a given year. (default: None)
  -m MONTH, --month MONTH
                        Extract records for a given month (number) (default: None)
  -o DIROUT, --dirout DIROUT
                        Path to output file for saving report - not required if running with `--dry-run True`. (default: /Users/sanastas/Projects/sb_billing/billing-report)
  -dry DRY_RUN, --dry-run DRY_RUN
                        Run queries in the DRY_RUN mode, i.e. no actual queries are executed. Used for estimating costs of sql queries (default: True)

```
