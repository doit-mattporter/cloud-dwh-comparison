#!/usr/bin/env bash
# Run this on a c7g.2xlarge with 2 TB of local storage

# Extract the [AWSConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[AWSConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > aws_config.ini
source aws_config.ini

# Upload EMR bootstrapping script and step script to S3
echo '#!/bin/bash' > emr_pyarrow_bootstrap.sh
echo 'pip3 install boto3 pyarrow' >> emr_pyarrow_bootstrap.sh
aws s3 cp aws_config.ini s3://$EMR_DATA_BUCKET/scripts/
aws s3 cp emr_pyarrow_bootstrap.sh s3://$EMR_DATA_BUCKET/scripts/
aws s3 cp 02a_AWS_grab_dbNSFP_bootstrap.sh s3://$EMR_DATA_BUCKET/scripts/
aws s3 cp 02b_AWS_convert_tsvs_to_parquet.py s3://$EMR_DATA_BUCKET/scripts/

# Generate a startup script that downloads the config and the bootstrap script, then executes it
cat <<EOF >02a_AWS_grab_dbNSFP_startup_script.sh
#!/bin/bash

# Download the config and bootstrap script from S3
aws s3 cp s3://${EMR_DATA_BUCKET}/scripts/aws_config.ini /tmp/aws_config.ini
aws s3 cp s3://${EMR_DATA_BUCKET}/scripts/02a_AWS_grab_dbNSFP_bootstrap.sh /tmp/02a_AWS_grab_dbNSFP_bootstrap.sh
chmod +x /tmp/02a_AWS_grab_dbNSFP_bootstrap.sh

source /tmp/aws_config.ini
echo EMR_MASTER_SG = $EMR_MASTER_SG
echo EMR_SLAVE_SG = $EMR_SLAVE_SG
echo EMR_DATA_BUCKET = $EMR_DATA_BUCKET
echo OUTPUT_BUCKET = $OUTPUT_BUCKET

# Run the bootstrap script
/tmp/02a_AWS_grab_dbNSFP_bootstrap.sh
EOF

# Upload the EC2 instance startup script (user data) for grabbing dbNSFP to S3
# aws s3 cp 02a_AWS_grab_dbNSFP_startup_script.sh s3://${EMR_DATA_BUCKET}/scripts/

# Create a c7g.2xlarge VM instance with 2 TB of gp3 EBS storage and execute the bootstrap script
# This will kick off grabbing dbNSFP, uploading it to GCS, and then spinning up an ephemeral Dataproc cluster
# to convert dbNSFP from flat files to a Parquet file
aws ec2 run-instances \
    --image-id ami-0f93c02efd1974b8b \
    --instance-type c7g.2xlarge \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=2048,VolumeType=gp3}' \
    --key-name $EC2_KEYPAIR \
    --security-group-ids $SECURITY_GROUP_ID \
    --subnet-id $EC2_SUBNET \
    --iam-instance-profile Name=$IAM_INSTANCE_PROFILE_NAME_WITH_S3_ACCESS \
    --user-data file://02a_AWS_grab_dbNSFP_startup_script.sh \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=dbnsfp-prep-instance}]" \
    --count 1
