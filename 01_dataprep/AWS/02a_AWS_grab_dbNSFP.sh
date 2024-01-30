#!/usr/bin/env bash
# Run this on a c7g.2xlarge with 2 TB of local storage

# Extract the [AWSConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[AWSConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_aws_config.sh
source temp_aws_config.sh

EMR_MASTER_SG_WITH_SSH_ENABLED=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-master --query 'SecurityGroups[0].GroupId' --output text)
EMR_SLAVE_SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-slave --query 'SecurityGroups[0].GroupId' --output text)

# Upload EMR bootstrapping script and step script to S3
echo '#!/bin/bash' > emr_pyarrow_bootstrap.sh
echo 'pip3 install pyarrow' >> emr_pyarrow_bootstrap.sh
aws s3 cp emr_pyarrow_bootstrap.sh s3://$EMR_DATA_BUCKET/scripts/
aws s3 cp 02b_AWS_convert_tsvs_to_parquet.py s3://$EMR_DATA_BUCKET/scripts/

# Grab zipped dbNSFP database, upload its unzipped TSVs to S3, and then launch an EMR cluster that converts those TSVs to Parquet
aws s3 cp s3://dbnsfp/dbNSFP4.4a.zip .
unzip dbNSFP4.4a.zip
for fn in dbNSFP4.4a_variant.chr*.gz
do
    gunzip $fn &
done
gunzip dbNSFP4.4_gene.complete.gz &
wait
for fn in dbNSFP4.4a_variant.chr*
do
    mv $fn "$fn.tsv" &
done
mv dbNSFP4.4_gene.complete dbNSFP4.4_gene_complete.tsv
rm -f dbNSFP*.gz*
wait
for fn in dbNSFP4.4a_variant.chr*.tsv
do
    aws s3 cp $fn s3://$EMR_DATA_BUCKET/dbNSFP/ &
done
aws s3 cp dbNSFP4.4_gene_complete.tsv s3://$EMR_DATA_BUCKET/dbNSFP/ &
wait

# Convert the TSVs to Parquet files
CLUSTER_ID=$(aws emr create-cluster \
    --name "cluster-genomic-dataprep" \
    --release-label emr-6.14.0 \
    --applications Name=Spark \
    --use-default-roles \
    --instance-groups '[{"Name":"Primary","InstanceGroupType":"MASTER","InstanceCount":1,"InstanceType":"r7g.xlarge","EbsConfiguration":{"EbsOptimized":true}},{"Name":"Core","InstanceGroupType":"CORE","InstanceCount":7,"InstanceType":"r7g.16xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"BidPrice":"OnDemandPrice"}]' \
    --ec2-attributes KeyName=$EC2_KEYPAIR,SubnetId=$EC2_SUBNET,EmrManagedMasterSecurityGroup=$EMR_MASTER_SG_WITH_SSH_ENABLED,EmrManagedSlaveSecurityGroup=$EMR_SLAVE_SG \
    --applications Name=Spark \
    --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --bootstrap-actions Path=s3://$EMR_DATA_BUCKET/scripts/emr_pyarrow_bootstrap.sh \
    --steps "Type=Spark,Name=\"Run 02b_AWS_convert_tsvs_to_parquet.py\",ActionOnFailure=TERMINATE_CLUSTER,Args=[\"s3://$EMR_DATA_BUCKET/scripts/02b_AWS_convert_tsvs_to_parquet.py\",\"$OUTPUT_BUCKET\"]]" \
    --log-uri s3://$EMR_DATA_BUCKET/emr_logs/ \
    --visible-to-all-users \
    --region us-east-1 \
    --query 'ClusterId' \
    --output text)

aws --region=us-east-1 emr put-auto-termination-policy \
    --cluster-id $CLUSTER_ID \
    --auto-termination-policy IdleTimeout=60
