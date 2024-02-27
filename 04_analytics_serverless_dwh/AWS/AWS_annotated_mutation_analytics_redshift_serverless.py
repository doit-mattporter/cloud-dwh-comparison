#!/usr/bin/env python
import configparser
import json
import time
from datetime import timedelta, timezone

import boto3

# Load configuration values
config = configparser.ConfigParser()
config.read("../../config.ini")
output_bucket = config["AWSConfig"]["OUTPUT_BUCKET"]
rpu_cost_per_hour = config["AWSConfig"]["RPU_COST_PER_HOUR"]
redshift_storage_per_gb_per_month = float(
    config["AWSConfig"]["REDSHIFT_STORAGE_PER_GB_PER_MONTH"]
)
total_cost = 0

# Initialize clients
glue_client = boto3.client("glue", region_name="us-east-1")
redshift_serverless_client = boto3.client(
    "redshift-serverless", region_name="us-east-1"
)
redshift_data = boto3.client("redshift-data", region_name="us-east-1")
iam_client = boto3.client("iam")

# Retrieve the Redshift serverless endpoint
response = redshift_serverless_client.list_endpoint_access(
    workgroupName="default-workgroup-ai"
)


def create_redshift_table_from_glue(glue_client, database_name, table_name):
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    columns = response["Table"]["StorageDescriptor"]["Columns"]
    parameters = response["Table"]["Parameters"]

    # Extract maxLen statistics from parameters
    max_lengths = {}
    for key, value in parameters.items():
        if "maxLen" in key:
            column_name = key.split(".")[-2].lower()
            max_lengths[column_name] = int(value)

    create_table_sql = f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ("
    for column in columns:
        column_name = column["Name"]
        value = max_lengths.get(column_name, None)
        max_length = min(value + 5, 65535) if value is not None else 65535
        redshift_type = map_data_types(column["Type"], max_length)
        create_table_sql += f'"{column_name}" {redshift_type}, '

    create_table_sql = create_table_sql.rstrip(", ") + ");"
    print(f"Creating Redshift Serverless table '{table_name}'")
    execute_query_and_get_result(create_table_sql)


def execute_query_and_get_result(query):
    global total_cost
    response = redshift_data.execute_statement(
        Database="dev", Sql=query, WorkgroupName="default-workgroup-ai"
    )
    statement_id = response["Id"]
    status_response = wait_for_query_completion(statement_id)
    runtime_seconds = status_response["Duration"]
    formatted_runtime = format_runtime(round(runtime_seconds / 1e9, 0))
    print(f"\nRuntime for the query shown below: {formatted_runtime}\n{query}\n")
    start_time = status_response["CreatedAt"]
    end_time = status_response["UpdatedAt"]
    cost = get_query_cost(statement_id, start_time, end_time)
    total_cost += cost
    try:
        result_response = redshift_data.get_statement_result(Id=statement_id)
        return result_response, start_time, end_time
    except redshift_data.exceptions.ResourceNotFoundException as e:
        print("No result to fetch for query")
        return {"Id": statement_id}, start_time, end_time


def format_runtime(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_runtime = f"{hours}h{minutes}m{seconds}s"
    return formatted_runtime


def get_query_cost(statement_id, start_time, end_time):
    global rpu_cost_per_hour
    if end_time.second > 0 or end_time.microsecond > 0:
        end_time += timedelta(minutes=1)

    trimmed_start_time_str = start_time.astimezone(timezone.utc).strftime(
        "%Y-%m-%d %H:%M"
    )
    trimmed_end_time_str = end_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")

    query = f"""
    SELECT
        (SUM(charged_seconds)/3600::double precision) * {rpu_cost_per_hour} as cost_incurred
    FROM
        sys_serverless_usage
    WHERE
        start_time >= '{trimmed_start_time_str}' AND
        end_time <= '{trimmed_end_time_str}'
    """
    print(f"Checking query cost with this query:\n{query}")
    response = redshift_data.execute_statement(
        Database="dev", Sql=query, WorkgroupName="default-workgroup-ai"
    )
    statement_id = response["Id"]
    wait_for_query_completion(statement_id)
    result_response = redshift_data.get_statement_result(Id=statement_id)
    null_results = result_response["Records"][0][0].get("isNull")
    cost_incurred = (
        0 if null_results else result_response["Records"][0][0]["doubleValue"]
    )
    print(f"Cost to run statement_id {statement_id}: ${cost_incurred:,.2f}")
    # Since Redshift Serverless bins cost reporting by the minute, we want to make sure each query is run in its own minute window
    time.sleep(120)
    return cost_incurred


def load_data_to_redshift(table_name, s3_path, role_arn):
    # Load data into table
    load_query = f"""
    COPY public.{table_name}
    FROM '{s3_path}'
    IAM_ROLE '{role_arn}'
    FORMAT AS PARQUET
    REGION 'us-east-1'
    """
    print(f"Loading data into Redshift serverless table '{table_name}'")
    execute_query_and_get_result(load_query)

    # Verify data loaded successfully
    row_count_query = f"SELECT COUNT(*) FROM public.{table_name}"
    result_response = execute_query_and_get_result(row_count_query)
    row_count = result_response[0]["Records"][0][0]["longValue"]
    print(f"Number of rows in {table_name}: {row_count}")
    if row_count > 0:
        print("Data loaded successfully")
    else:
        raise Exception(f"Data load failed for {table_name}")


def manage_iam_role(role_name, redshift_serverless_client):
    """Create an IAM role with the permissions needed to load S3 Parquet files into Redshift"""
    try:
        role_response = iam_client.get_role(RoleName=role_name)
        role_arn = role_response["Role"]["Arn"]
        print(
            f"Role {role_name} already exists with ARN: {role_arn}. Skipping role creation."
        )
    except iam_client.exceptions.NoSuchEntityException:
        assume_role_policy_document = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "redshift.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        create_role_response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy_document,
            Description="IAM role with permissions to access S3 and Redshift Serverless",
        )
        inline_policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket",
                            "redshift:Get*",
                            "redshift:Describe*",
                            "redshift:ExecuteStatement",
                        ],
                        "Resource": "*",
                    }
                ],
            }
        )
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="InlinePolicyForRedshiftS3",
            PolicyDocument=inline_policy,
        )
        role_arn = create_role_response["Role"]["Arn"]
    return role_arn


def map_data_types(athena_type, max_length=None):
    if athena_type == "string":
        return f"VARCHAR({max_length})"
    elif athena_type == "double":
        return "FLOAT8"
    else:
        return athena_type.upper()


def print_records(result_response):
    for record in result_response[0]["Records"]:
        values = []
        for field in record:
            if field.get("stringValue"):
                values.append(field["stringValue"])
            elif field.get("longValue"):
                values.append(str(field["longValue"]))
            elif field.get("doubleValue"):
                values.append(str(field["doubleValue"]))
            elif field.get("booleanValue"):
                values.append(str(field["booleanValue"]))
            else:
                values.append("NULL")
        print("\t".join(values))


def wait_for_query_completion(statement_id):
    while True:
        time.sleep(5)
        status_response = redshift_data.describe_statement(Id=statement_id)
        status = status_response["Status"]
        if status in ["FINISHED", "FAILED", "ABORTED"]:
            print(f"Query status: {status}.")
            if status != "FINISHED":
                print(f"Query error: {status_response['Error']}")
            return status_response
        print(f"Query status: {status}. Waiting for completion...")


create_redshift_table_from_glue(glue_client, "default", "dbnsfp_gene")
create_redshift_table_from_glue(
    glue_client, "default", "gnomad_with_dbnsfp_annotations"
)

# Create an IAM role with the permissions needed to load S3 Parquet files into Redshift
role_arn = manage_iam_role("RedshiftServerlessS3AccessRole", redshift_serverless_client)
redshift_serverless_client.update_namespace(
    namespaceName="default-ai",
    defaultIamRoleArn=role_arn,
    iamRoles=[role_arn],
)

# Load gene data into Redshift
load_data_to_redshift(
    "dbnsfp_gene",
    f"s3://{output_bucket}/dbNSFP/dbNSFP4.6_gene_complete.parquet",
    role_arn,
)
# Load variant annotation data into Redshift
load_data_to_redshift(
    "gnomad_with_dbnsfp_annotations",
    f"s3://{output_bucket}/merged/gnomad_with_dbnsfp_annotations.parquet",
    role_arn,
)

# Run somewhat computationally intensive queries and determine their cost
with open("AWS_annotated_mutation_analytics_redshift_serverless.sql", "r") as f:
    sql_commands = f.read().split(";")

# Execute each SQL command
for i, sql in enumerate(cmd for cmd in sql_commands if cmd.strip()):
    # print(f"{sql}\n\n")
    result_response = execute_query_and_get_result(sql)
    print_records(result_response)

# Determine the total size and monthly storage cost of the table
size_query = """
SELECT SUM(size)/1024 AS total_size_in_gb
FROM SVV_TABLE_INFO;
"""
try:
    result_response = execute_query_and_get_result(size_query)
    if result_response and result_response[0]["Records"]:
        total_size_gb = result_response[0]["Records"][0][0]["longValue"]
        monthly_cost = total_size_gb * redshift_storage_per_gb_per_month
        print(f"Monthly cost to store all data tables: ${monthly_cost:.2f}")
    else:
        print("No data found.")
except Exception as e:
    print(f"Error executing query: {e}")
