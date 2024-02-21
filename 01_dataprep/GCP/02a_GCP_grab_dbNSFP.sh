#!/usr/bin/env bash
# Run this on an c3d-standard-8 with 2 TB of balanced PD storage

# Extract the [GCPConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[GCPConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_gcp_config.sh
source temp_gcp_config.sh

# Upload Dataproc bootstrapping script and step script to GCS
echo '#!/bin/bash' > dataproc_tsv_conversion_bootstrap.sh
echo '/opt/conda/default/bin/pip3 install google-cloud-storage' >> dataproc_tsv_conversion_bootstrap.sh
gcloud storage cp dataproc_tsv_conversion_bootstrap.sh gs://$DATAPROC_DATA_BUCKET/scripts/
gcloud storage cp 02b_GCP_convert_tsvs_to_parquet.py gs://$DATAPROC_DATA_BUCKET/scripts/

# Grab zipped dbNSFP database, upload its unzipped TSVs to GCS, and then launch an EMR cluster that converts those TSVs to Parquet
sudo apt-get install -y unzip
wget https://dbnsfp.s3.amazonaws.com/dbNSFP4.4a.zip
unzip dbNSFP4.4a.zip
for fn in dbNSFP4.4a_variant.chr*.gz
do
    gunzip $fn &
done
gunzip dbNSFP4.4_gene.complete.gz
wait
for fn in dbNSFP4.4a_variant.chr*
do
    mv $fn "$fn.tsv" &
done
mv dbNSFP4.4_gene.complete dbNSFP4.4_gene_complete.tsv
wait
gcloud storage cp dbNSFP4.4a_variant.chr*.tsv gs://$OUTPUT_BUCKET/dbNSFP/
gcloud storage cp dbNSFP4.4_gene_complete.tsv gs://$OUTPUT_BUCKET/dbNSFP/
rm -f dbNSFP4* search_dbNSFP4* try* LICENSE.txt


# Convert the TSVs to Parquet files
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
    --num-secondary-workers 5 \
    --secondary-worker-type preemptible \
    --image-version 2.1-debian11 \
    --enable-component-gateway \
    --optional-components JUPYTER,DOCKER \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --initialization-actions gs://${DATAPROC_DATA_BUCKET}/scripts/dataproc_tsv_conversion_bootstrap.sh

gcloud dataproc jobs submit pyspark gs://$DATAPROC_DATA_BUCKET/scripts/02b_GCP_convert_tsvs_to_parquet.py \
    --cluster=cluster-genomic-dataprep \
    --region=us-central1 \
    -- $OUTPUT_BUCKET
