#!/usr/bin/env bash

# Extract the [AWSConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[AWSConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_aws_config.ini
source temp_aws_config.ini

# Upload EMR bootstrapping script and step script to S3
echo '#!/bin/bash' > emr_pyarrow_bootstrap.sh
echo 'pip3 install pyarrow' >> emr_pyarrow_bootstrap.sh
aws s3 cp emr_pyarrow_bootstrap.sh s3://$EMR_DATA_BUCKET/scripts/
aws s3 cp 03_AWS_annotated_mutation_analytics_spark_union_dbs.py s3://$EMR_DATA_BUCKET/scripts/

# Run analytics
CLUSTER_ID=$(aws emr create-cluster \
    --name "cluster-genomic-analytics" \
    --release-label emr-7.0.0 \
    --applications Name=Spark \
    --use-default-roles \
    --instance-groups '[{"Name":"Primary","InstanceGroupType":"MASTER","InstanceCount":1,"InstanceType":"r7g.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":50,"VolumeType":"gp3"},"VolumesPerInstance":1}],"EbsOptimized":true}},{"Name":"Core","InstanceGroupType":"CORE","InstanceCount":30,"InstanceType":"r7g.16xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp3"},"VolumesPerInstance":1}],"EbsOptimized":true},"BidPrice":"OnDemandPrice"}]' \
    --ec2-attributes KeyName=$EC2_KEYPAIR,SubnetId=$EC2_SUBNET,EmrManagedMasterSecurityGroup=$EMR_MASTER_SG,EmrManagedSlaveSecurityGroup=$EMR_SLAVE_SG \
    --applications Name=Spark \
    --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --bootstrap-actions Path=s3://$EMR_DATA_BUCKET/scripts/emr_pyarrow_bootstrap.sh \
    --steps "Type=Spark,Name=\"Run 03_AWS_annotated_mutation_analytics_spark_union_dbs.py\",ActionOnFailure=CONTINUE,Args=[\"s3://$EMR_DATA_BUCKET/scripts/03_AWS_annotated_mutation_analytics_spark_union_dbs.py\",\"$OUTPUT_BUCKET\"]" \
    --log-uri s3://$EMR_DATA_BUCKET/emr_logs/ \
    --visible-to-all-users \
    --region us-east-1 \
    --query 'ClusterId' \
    --output text)

aws --region=us-east-1 emr put-auto-termination-policy \
    --cluster-id $CLUSTER_ID \
    --auto-termination-policy IdleTimeout=60
