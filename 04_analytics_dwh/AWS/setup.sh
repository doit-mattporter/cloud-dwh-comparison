#!/usr/bin/env bash
# YOU WILL NEED TO SET UP REDSHIFT SERVERLESS IN THE WEB CONSOLE!
# THE AWS CLI AND PYTHON SDKS DO NOT CURRENTLY SUPPORT INITIALIZING REDSHIFT SERVERLESS
# SELECT THE AI-POWERED 'Price-performance targets' workgroup option with a value '50'/'Balanced' workload target

# First, create your Redshift serverless instance with these values:
# workgroup-name='default-workgroup-ai'
# namespace-name='default-ai'
# endpoint-name='us-east-1-endpoint-ai'
# Set a base capacity of 16 RPUs and a max capacity of 128 (to enable auto-scaling for loading gnomAD data).
# Note that the default base capacity is 128; this is too expensive for a demo. 128+ is intended for PB-scale datasets.

# Extract the [AWSConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[AWSConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_aws_config.sh
source temp_aws_config.sh

# Create a us-east-1 endpoint
if aws redshift-serverless get-endpoint-access --endpoint-name $REDSHIFT_ENDPOINT_NAME --region us-east-1 --no-cli-pager 2>/dev/null; then
    echo "Endpoint $REDSHIFT_ENDPOINT_NAME already exists."
else
    echo "Creating endpoint $REDSHIFT_ENDPOINT_NAME..."
    # Create a us-east-1 endpoint
    aws redshift-serverless create-endpoint-access --region us-east-1 --subnet-ids $REDSHIFT_SUBNET_IDS_FOR_US_EAST_1 --workgroup-name default-workgroup-ai --endpoint-name $REDSHIFT_ENDPOINT_NAME --no-cli-pager
    # Verify endpoint creation was successful:
fi
    aws redshift-serverless get-endpoint-access --endpoint-name $REDSHIFT_ENDPOINT_NAME --no-cli-pager
# You should see an "endpoint" key with an "endpointArn" key:value pair

# List databases
aws redshift-data list-databases --database dev --workgroup-name default-workgroup-ai --no-cli-pager
# You should see:
# {
#     "Databases": [
#         "awsdatacatalog",
#         "dev"
#     ]
# }
