#!/usr/bin/env bash

# Extract the [GCPConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[GCPConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > gcp_config.sh
source gcp_config.sh

# Upload to GCS:
# (1) (2) The config file and bootstrap script for a GCE machine that will grab dbNSFP and kick off Dataproc
# (3) (4) The Dataproc bootstrap and step scripts for converting dbNSFP flat files to Parquet format
echo '#!/bin/bash' > dataproc_tsv_conversion_bootstrap.sh
echo '/opt/conda/default/bin/pip3 install google-cloud-storage' >> dataproc_tsv_conversion_bootstrap.sh
gcloud storage cp gcp_config.sh gs://$DATAPROC_DATA_BUCKET/scripts/
gcloud storage cp 02a_GCP_grab_dbNSFP_bootstrap.sh gs://$DATAPROC_DATA_BUCKET/scripts/
gcloud storage cp dataproc_tsv_conversion_bootstrap.sh gs://$DATAPROC_DATA_BUCKET/scripts/
gcloud storage cp 02b_GCP_convert_tsvs_to_parquet.py gs://$DATAPROC_DATA_BUCKET/scripts/

# Generate a startup script that downloads the config and the bootstrap script, then executes it
cat <<EOF >02a_GCP_grab_dbNSFP_startup_script.sh
#!/bin/bash
source /tmp/gcp_config.sh
echo DATAPROC_DATA_BUCKET = $DATAPROC_DATA_BUCKET
echo OUTPUT_BUCKET = $OUTPUT_BUCKET

# Download the config and bootstrap script from GCS
gsutil cp gs://${DATAPROC_DATA_BUCKET}/scripts/gcp_config.sh /tmp/gcp_config.sh
gsutil cp gs://${DATAPROC_DATA_BUCKET}/scripts/02a_GCP_grab_dbNSFP_bootstrap.sh /tmp/02a_GCP_grab_dbNSFP_bootstrap
chmod +x /tmp/02a_GCP_grab_dbNSFP_bootstrap

# Run the bootstrap script
/tmp/02a_GCP_grab_dbNSFP_bootstrap
EOF

# Upload to GCS the GCE instance startup script for grabbing dbNSFP
gcloud storage cp 02a_GCP_grab_dbNSFP_startup_script.sh gs://${DATAPROC_DATA_BUCKET}/scripts/

# Create a c3d-standard-8 VM instance with 2 TB of balanced PD storage and execute the bootstrap script
# This will kick off grabbing dbNSFP, uploading it to GCS, and then spinning up an ephemeral Dataproc cluster
# to convert dbNSFP from flat files to a Parquet file
gcloud compute instances create dbnsfp-prep-instance \
    --machine-type=c3d-standard-8 \
    --boot-disk-size=2TB \
    --boot-disk-type=pd-balanced \
    --metadata=startup-script-url=gs://${DATAPROC_DATA_BUCKET}/scripts/02a_GCP_grab_dbNSFP_startup_script.sh \
    --zone=us-central1-a \
    --scopes=https://www.googleapis.com/auth/cloud-platform
