# Generate Monthly Billing Report

Script for generating monthy billing report for the FIMM projects. Script reads metadata file from the google cloud storage bucket and extracts cost records from the BQ table for a given project by label (WBS). Metadata is a tab-separated CSV file with two columns, `label` (i.e. WBS) and `description`:
```
label	description
111111	SANDBOX_PROJECT_1
222222	SANDBOX_PROJECT_2
```

## Run the script

1. Authenticate: `gcloud auth application-default login`

2. Install python packages: `pip3 install -r scripts/requirements.txt`

3. Run billing report script:

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

## Usage

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
