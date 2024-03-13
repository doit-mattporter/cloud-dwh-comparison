#!/usr/bin/env bash

# Extract the [GCPConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[GCPConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_gcp_config.sh
source temp_gcp_config.sh

SOURCE_URI="gs://${OUTPUT_BUCKET}/merged/gnomad_with_dbnsfp_annotations.parquet/*.parquet"

START_TIME=$(date +%s)

# Check if the dataset exists
if ! bq --headless --format=none show ${BIGQUERY_PROJECT}:${BIGQUERY_DATASET}; then
    echo "Dataset ${BIGQUERY_DATASET} does not exist. Creating dataset..."
    bq mk --dataset ${BIGQUERY_PROJECT}:${BIGQUERY_DATASET}
fi

# Load the Parquet file into BigQuery
TABLE_URI=${BIGQUERY_PROJECT}:${BIGQUERY_DATASET}.gnomad_with_dbnsfp_annotations
bq load \
    --source_format=PARQUET \
    --clustering_fields=chromosome,start_position,refallele,altallele \
    "${TABLE_URI}" ${SOURCE_URI}

# Get the size of the table in GBs
TABLE_SIZE_BYTES=$(bq show --format=prettyjson "${TABLE_URI}" | grep "numBytes" | grep -o '[0-9]\+')
TABLE_SIZE_GB=$(echo "scale=2; $TABLE_SIZE_BYTES / (1024 * 1024 * 1024)" | bc)

# BigQuery storage cost: $0.02 per GB per month
# Adjust the rate if BigQuery's pricing changes
STORAGE_COST_PER_MONTH=$(echo "scale=2; $TABLE_SIZE_GB * 0.02" | bc)

echo "Estimated monthly storage cost for ${TABLE_URI}: \$${STORAGE_COST_PER_MONTH}"


SOURCE_URI="gs://${OUTPUT_BUCKET}/dbNSFP/dbNSFP4.6_gene_complete.parquet/*.parquet"

# Check if the dataset exists
if ! bq --headless --format=none show ${BIGQUERY_PROJECT}:${BIGQUERY_DATASET}; then
    echo "Dataset ${BIGQUERY_DATASET} does not exist. Creating dataset..."
    bq mk --dataset ${BIGQUERY_PROJECT}:${BIGQUERY_DATASET}
fi

# Load the Parquet file into BigQuery
TABLE_URI=${BIGQUERY_PROJECT}:${BIGQUERY_DATASET}.dbNSFP_gene
bq load \
    --source_format=PARQUET \
    --clustering_fields=Gene_name \
    "${TABLE_URI}" ${SOURCE_URI}

# Get the size of the table in GBs
TABLE_SIZE_BYTES=$(bq show --format=prettyjson "${TABLE_URI}" | grep "numBytes" | grep -o '[0-9]\+')
TABLE_SIZE_GB=$(echo "scale=2; $TABLE_SIZE_BYTES / (1024 * 1024 * 1024)" | bc)

# BigQuery storage cost: $0.02 per GB per month
# Adjust the rate if BigQuery's pricing changes
STORAGE_COST_PER_MONTH=$(echo "scale=2; $TABLE_SIZE_GB * 0.02" | bc)

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "Estimated monthly storage cost for ${TABLE_URI}: \$${STORAGE_COST_PER_MONTH}"

echo "Total script runtime: $DURATION seconds."
