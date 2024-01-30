#!/usr/bin/env bash

# Extract the [GCPConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[GCPConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_gcp_config.sh
source temp_gcp_config.sh

# Upload Dataproc bootstrapping script and step script to GCS
echo '#!/bin/bash' > dataproc_genomics_bootstrap.sh
# echo '/opt/conda/default/bin/conda install -c conda-forge gcc==13.1.0' >> dataproc_genomics_bootstrap.sh
echo 'sudo apt-get update' >> dataproc_genomics_bootstrap.sh
echo 'sudo apt-get install -y gcc' >> dataproc_genomics_bootstrap.sh
echo '/opt/conda/default/bin/pip3 install glow.py google-cloud-storage' >> dataproc_genomics_bootstrap.sh
gcloud storage cp dataproc_genomics_bootstrap.sh gs://$DATAPROC_DATA_BUCKET/scripts/
gcloud storage cp 01b_GCP_convert_vcfs_to_parquet.py gs://$DATAPROC_DATA_BUCKET/scripts/

# Run the following to prepare an autoscaling Dataproc cluster which is able to run Glow
gcloud metastore services create my-metastore-service \
    --location=us-central1 \
    --tier=DEVELOPER \
    --hive-metastore-version=3.1.2 \
    --network=default

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
    --worker-boot-disk-size 100 \
    --num-secondary-workers 23 \
    --secondary-worker-type preemptible \
    --image-version 2.1-debian11 \
    --enable-component-gateway \
    --optional-components JUPYTER,DOCKER \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --initialization-actions gs://${DATAPROC_DATA_BUCKET}/scripts/dataproc_genomics_bootstrap.sh \
    --properties=spark:spark.jars.packages=io.projectglow:glow-spark3_2.12:1.2.1

gcloud dataproc jobs submit pyspark gs://$DATAPROC_DATA_BUCKET/scripts/01b_GCP_convert_vcfs_to_parquet.py \
    --cluster=cluster-genomic-dataprep \
    --region=us-central1 \
    -- $OUTPUT_BUCKET \
    --jars io.projectglow:glow-spark3_2.12:1.2.1 \
    --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec
