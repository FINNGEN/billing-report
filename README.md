# Generate Monthly Billing Report

Script for generating monthy billing report for the FIMM projects. Script reads metadata file from the google cloud storage bucket and extracts cost records from the BQ table for a given project by label (WBS). Metadata is a tab-separated CSV file with two columns, `label` (i.e. WBS) and `description`:
```
label	description
111111	Some description about the project 1
222222	Some description about the project 2
```

The script creates an output EXCEL table with 4 separate sheets: 
- 'by GCP project' - summary of costs by project_id
- 'by billing label' - summary of costs by billing label (e.g. "<WBS_NUMBER>_<SPECIFIC_LABEL>") - if there is no label, the costs for these unlabeled projects will be calculated in the separate row of the resulted excel table
- 'by WBS' - summary by WBS
- 'errors' - this sheet is addeed to the report in case a project with PROJECT_ID has entries in the BQ with multiple billing_labels - in this case the project_id as well as all projects with the umbigous billing_labels **WILL BE EXCLUDED FROM THE REPORT**.


Run the following to authenticate to google cloud sdk and install the packages:

1. Authenticate: `gcloud auth application-default login`

2. Install git and python on your machine if not installed (linux): `sudo apt update & sudo apt install python3-pip git`

3. Clone the git repo: `git clone https://github.com/FINNGEN/billing-report.git`

4. Install python packages: `cd billing-report && pip3 install -r scripts/requirements.txt`

5. Run the script (below)

**Version 1:** extract only specified projects marked by billing label in BQ

This version of the tool extracts only projects with the billing label specified in the provided input metadata. Run billing report script ver1: extract only billing labels that are specified in the metadata (flag `--mode metadata_billing_label`):

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
--mode metadata_billing_label \
--dry-run True 
```

Once you familiarize yourself with the expected costs, run command disabling the dry-run (`--dry-run False`).


**Version 2:** extract all projects from BQ which includes unlabled projects as well

This version of the tool extracts **all** records for the given month. The metadata file is required in this case just for matching the description to the projects if provided. Run the following command (specify `--mode full`):

```
scripts/billing_report_full.py \
--project PROJECT_ID_CONTAINING_BQ_TABLE \
--dataset BQ_DATASET_NAME \
--table BQ_TABLE_NAME \
--year 2023 \
--month 7 \
--mode full \
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

## USAGE

```
Script prepares monthly billing report.

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
  -mode {full,metadata_billing_label}, --mode {full,metadata_billing_label}
                        Choose the mode to run: full (extracts all available data from the BQ table for a given month) or selected (extracts records for billing labels specified in the
                        metadata file). Default: "full" (default: full)
  -md METADATA, --metadata METADATA
                        Metadata file storing project labels and description. (default: None)
  -b BUCKET, --bucket BUCKET
                        Extract metadata from the given bucket omiting gs:// prefix. (default: None)

```

## Running from the dedicated setup environment

Run simply by calling:
```
billing --project PROJECT_ID_CONTAINING_BQ_TABLE \
--dataset BQ_DATASET_NAME \
--table BQ_TABLE_NAME \
--metadata METADATA_CSV \
--bucket GCP_BUCKET_WITH_METADATA_FILE \
--year 2023 \
--month 7 \
--mode metadata_billing_label \
--dry-run True 
```
