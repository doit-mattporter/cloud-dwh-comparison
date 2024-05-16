#!/usr/bin/env bash
# While the following commands will automatically set up Redshift serverless, as of May 2024 it is not possible to utilize the AI-powered 'Price-performance targets' workgroup option with a value '50'/'Balanced' workload target unless you manually choose this for the workgroup in the web console

# Extract the [AWSConfig] section of the config file and convert it to a Bash-friendly format
awk -F '=' '/^\[AWSConfig\]/ {flag=1; next} /^\[/ && flag {flag=0} flag && /=/ {gsub(/[[:space:]]*=[[:space:]]*/, "="); print $1 "=" $2}' ../../config.ini > temp_aws_config.ini
source temp_aws_config.ini

# Create the Redshift Serverless namespace if it does not exist
if aws redshift-serverless get-namespace --namespace-name default-ai --region us-east-1 --no-cli-pager 2>/dev/null; then
    echo "Namespace default-ai already exists."
else
    echo "Creating namespace default-ai..."
    aws redshift-serverless create-namespace \
        --namespace-name default-ai \
        --region us-east-1 \
        --no-cli-pager
fi

# Create the Redshift Serverless workgroup
aws redshift-serverless create-workgroup \
    --workgroup-name default-workgroup-ai \
    --namespace-name default-ai \
    --base-capacity 16 \
    --max-capacity 128 \
    --region us-east-1 \
    --no-cli-pager

# Create a us-east-1 endpoint
if aws redshift-serverless get-endpoint-access --endpoint-name $REDSHIFT_ENDPOINT_NAME --region us-east-1 --no-cli-pager 2>/dev/null; then
    echo "Endpoint $REDSHIFT_ENDPOINT_NAME already exists."
else
    echo "Creating endpoint $REDSHIFT_ENDPOINT_NAME..."
    aws redshift-serverless create-endpoint-access --region us-east-1 --subnet-ids $REDSHIFT_SUBNET_IDS_FOR_US_EAST_1 --workgroup-name default-workgroup-ai --endpoint-name $REDSHIFT_ENDPOINT_NAME --no-cli-pager
fi
    # Verify endpoint creation was successful:
    aws redshift-serverless get-endpoint-access --region us-east-1 --endpoint-name $REDSHIFT_ENDPOINT_NAME --no-cli-pager
# You should see an "endpoint" key with an "endpointArn" key:value pair

# List databases
aws redshift-data list-databases --region us-east-1 --database dev --workgroup-name default-workgroup-ai --no-cli-pager
# You should see:
# {
#     "Databases": [
#         "awsdatacatalog",
#         "dev"
#     ]
# }
