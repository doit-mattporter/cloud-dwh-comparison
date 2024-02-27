#!/usr/bin/env bash

# Assume /tmp/aws_config.sh is downloaded by the EC2 startup script
source /tmp/aws_config.sh

# Grab zipped dbNSFP database, upload its unzipped TSVs to S3, and then launch an EMR cluster that converts those TSVs to Parquet
sudo yum install -y unzip
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
for fn in dbNSFP4.6a_variant.chr*tsv
do
    aws s3 cp "$fn" s3://$OUTPUT_BUCKET/dbNSFP/ &
done
aws s3 cp dbNSFP4.6_gene_complete.tsv s3://$OUTPUT_BUCKET/dbNSFP/
wait
rm -f dbNSFP4* search_dbNSFP4* try* LICENSE.txt

# Convert the TSVs to Parquet files
CLUSTER_ID=$(aws emr create-cluster \
    --name "cluster-genomic-dataprep" \
    --release-label emr-6.14.0 \
    --applications Name=Spark \
    --use-default-roles \
    --instance-groups '[{"Name":"Primary","InstanceGroupType":"MASTER","InstanceCount":1,"InstanceType":"r7g.xlarge","EbsConfiguration":{"EbsOptimized":true}},{"Name":"Core","InstanceGroupType":"CORE","InstanceCount":8,"InstanceType":"r7g.16xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"BidPrice":"OnDemandPrice"}]' \
    --ec2-attributes KeyName=$EC2_KEYPAIR,SubnetId=$EC2_SUBNET,EmrManagedMasterSecurityGroup=$EMR_MASTER_SG,EmrManagedSlaveSecurityGroup=$EMR_SLAVE_SG \
    --applications Name=Spark \
    --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --bootstrap-actions Path=s3://$EMR_DATA_BUCKET/scripts/emr_pyarrow_bootstrap.sh \
    --steps "Type=Spark,Name=\"Run 02b_AWS_convert_tsvs_to_parquet.py\",ActionOnFailure=TERMINATE_CLUSTER,Args=[\"s3://$EMR_DATA_BUCKET/scripts/02b_AWS_convert_tsvs_to_parquet.py\",\"$OUTPUT_BUCKET\"]" \
    --log-uri s3://$EMR_DATA_BUCKET/emr_logs/ \
    --visible-to-all-users \
    --region us-east-1 \
    --query 'ClusterId' \
    --output text)

aws --region=us-east-1 emr put-auto-termination-policy \
    --cluster-id $CLUSTER_ID \
    --auto-termination-policy IdleTimeout=60

# Self-termination of the EC2 instance
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids $INSTANCE_ID
