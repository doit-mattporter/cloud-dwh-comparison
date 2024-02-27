#!/usr/bin/env bash

# Assume /tmp/gcp_config.sh is downloaded by the GCE startup script
source /tmp/gcp_config.sh

# Grab zipped dbNSFP database, upload its unzipped TSVs to GCS, and then launch a Dataproc cluster that converts those TSVs to Parquet
sudo apt-get install -y unzip
wget https://dbnsfp.s3.amazonaws.com/dbNSFP4.6a.zip
unzip dbNSFP4.6a.zip
for fn in dbNSFP4.6a_variant.chr*.gz
do
    gunzip $fn &
done
gunzip dbNSFP4.6_gene.complete.gz
wait
for fn in dbNSFP4.6a_variant.chr*
do
    mv $fn "$fn.tsv" &
done
mv dbNSFP4.6_gene.complete dbNSFP4.6_gene_complete.tsv
wait
gcloud storage cp dbNSFP4.6a_variant.chr*.tsv gs://$OUTPUT_BUCKET/dbNSFP/
gcloud storage cp dbNSFP4.6_gene_complete.tsv gs://$OUTPUT_BUCKET/dbNSFP/
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
    --worker-boot-disk-size 100 \
    --num-secondary-workers 6 \
    --secondary-worker-type preemptible \
    --image-version 2.1-debian11 \
    --enable-component-gateway \
    --optional-components JUPYTER,DOCKER \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --initialization-actions gs://${DATAPROC_DATA_BUCKET}/scripts/dataproc_tsv_conversion_bootstrap.sh

gcloud dataproc jobs submit pyspark gs://$DATAPROC_DATA_BUCKET/scripts/02b_GCP_convert_tsvs_to_parquet.py \
    --cluster=cluster-genomic-dataprep \
    --region=us-central1 \
    --async \
    -- $OUTPUT_BUCKET

# Terminate this GCE machine
INSTANCE_NAME=$(curl -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name")
ZONE=$(curl -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/zone" | cut -d'/' -f4)

gcloud compute instances delete $INSTANCE_NAME --zone=$ZONE --quiet
