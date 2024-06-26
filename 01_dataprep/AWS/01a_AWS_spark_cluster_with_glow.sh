#!/usr/bin/env bash

# Extract the [AWSConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[AWSConfig\]/ {flag=1; next} /^\[/ {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > aws_config.ini
source aws_config.ini

# Upload EMR bootstrapping script and step script to S3
echo '#!/bin/bash' > emr_genomics_bootstrap.sh
echo 'pip3 install glow.py pyarrow' >> emr_genomics_bootstrap.sh
aws s3 cp emr_genomics_bootstrap.sh s3://$EMR_DATA_BUCKET/scripts/
aws s3 cp 01b_AWS_convert_vcfs_to_parquet.py s3://$EMR_DATA_BUCKET/scripts/

CLUSTER_ID=$(aws emr create-cluster \
    --name "cluster-genomic-dataprep" \
    --release-label emr-6.11.0 \
    --applications Name=Spark \
    --use-default-roles \
    --instance-groups '[{"Name":"Primary","InstanceGroupType":"MASTER","InstanceCount":1,"InstanceType":"r7g.xlarge","EbsConfiguration":{"EbsOptimized":true,"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":50,"VolumeType":"gp3"},"VolumesPerInstance":1}]}} ,{"Name":"Core","InstanceGroupType":"CORE","InstanceCount":25,"InstanceType":"r7g.16xlarge","EbsConfiguration":{"EbsOptimized":true,"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp3"},"VolumesPerInstance":1}]},"BidPrice":"OnDemandPrice"}]' \
    --ec2-attributes KeyName=$EC2_KEYPAIR,SubnetId=$EC2_SUBNET,EmrManagedMasterSecurityGroup=$EMR_MASTER_SG,EmrManagedSlaveSecurityGroup=$EMR_SLAVE_SG \
    --applications Name=Spark \
    --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --bootstrap-actions "Path=s3://$EMR_DATA_BUCKET/scripts/emr_genomics_bootstrap.sh" \
    --steps "Type=Spark,Name=\"Run 01b_AWS_convert_vcfs_to_parquet.py\",ActionOnFailure=TERMINATE_CLUSTER,Args=[--packages,io.projectglow:glow-spark3_2.12:1.2.1,--conf,spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec,\"s3://$EMR_DATA_BUCKET/scripts/01b_AWS_convert_vcfs_to_parquet.py\",\"$OUTPUT_BUCKET\"]" \
    --log-uri "s3://$EMR_DATA_BUCKET/emr_logs/" \
    --auto-terminate \
    --visible-to-all-users \
    --region us-east-1 \
    --query 'ClusterId' \
    --output text)

aws --region=us-east-1 emr put-auto-termination-policy \
    --cluster-id $CLUSTER_ID \
    --auto-termination-policy IdleTimeout=60
