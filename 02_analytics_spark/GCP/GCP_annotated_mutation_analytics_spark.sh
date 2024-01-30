#!/usr/bin/env bash

# Extract the [GCPConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[GCPConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_gcp_config.sh
source temp_gcp_config.sh

# Upload Dataproc bootstrapping script and step script to GCS
echo '#!/bin/bash' > dataproc_tsv_conversion_bootstrap.sh
echo '/opt/conda/default/bin/pip3 install google-cloud-storage' >> dataproc_tsv_conversion_bootstrap.sh
gcloud storage cp dataproc_tsv_conversion_bootstrap.sh gs://$DATAPROC_DATA_BUCKET/scripts/
gcloud storage cp GCP_annotated_mutation_analytics_spark.py gs://$DATAPROC_DATA_BUCKET/scripts/

METASTORE_URI=$(gcloud metastore services describe my-metastore-service --location=us-central1 --format="value(name)")

gcloud dataproc clusters create cluster-genomic-dataprep \
    --max-idle 300s \
    --dataproc-metastore $METASTORE_URI \
    --region us-central1 \
    --zone us-central1-c \
    --master-machine-type n2-standard-4 \
    --master-boot-disk-size 50 \
    --num-workers 2 \
    --worker-machine-type n2-highmem-64 \
    --num-secondary-workers 28 \
    --secondary-worker-type preemptible \
    --image-version 2.1-debian11 \
    --enable-component-gateway \
    --optional-components JUPYTER,DOCKER \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --initialization-actions gs://${DATAPROC_DATA_BUCKET}/scripts/dataproc_genomics_bootstrap.sh

gcloud dataproc jobs submit pyspark gs://$DATAPROC_DATA_BUCKET/scripts/GCP_annotated_mutation_analytics_spark.py \
    --cluster=cluster-genomic-dataprep \
    --region=us-central1 \
    -- $OUTPUT_BUCKET
