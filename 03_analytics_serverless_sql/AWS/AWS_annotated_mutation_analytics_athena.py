#!/usr/bin/env python
import configparser
import time

import boto3

# Load configuration values
config = configparser.ConfigParser()
config.read("../../config.ini")
athena_output_bucket_path = config["AWSConfig"]["ATHENA_OUTPUT_BUCKET_PATH"]
glue_database = config["AWSConfig"]["GLUE_DATABASE"]
cost_per_tb = config["AWSConfig"]["ATHENA_COST_PER_TB"]  # Cost per TB scanned in USD

client = boto3.client("athena", region_name="us-east-1")

with open("AWS_annotated_mutation_analytics_athena.sql", "r") as f:
    sql_commands = f.read().split(";")

total_data_processed = 0

start_time = time.time()

# Execute each SQL command and report on its cost
for i, sql in enumerate(cmd for cmd in sql_commands if cmd.strip()):
    print(f"Executing Query {i+1}\n{sql}\n")
    response = client.start_query_execution(
        QueryString=sql.strip(),
        QueryExecutionContext={"Database": glue_database},
        ResultConfiguration={"OutputLocation": athena_output_bucket_path},
    )
    query_id = response["QueryExecutionId"]
    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)
        if status["QueryExecution"]["Status"]["State"] in [
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        ]:
            break
        time.sleep(1)
    bytes_processed = int(status["QueryExecution"]["Statistics"]["DataScannedInBytes"])
    query_cost = (bytes_processed / 1e12) * cost_per_tb
    total_data_processed += bytes_processed
    print(
        f"Query {i+1} processed {bytes_processed / 1e9:.2f} GB of data. Cost: ${query_cost:.2f}"
    )

end_time = time.time()

total_cost = (total_data_processed / 1e12) * cost_per_tb

elapsed_time = end_time - start_time
print(f"Total time elapsed: {elapsed_time:.2f} seconds")
print(f"Total data processed: {total_data_processed / 1e9:.2f} GB")
print(f"Total cost: ${total_cost:.2f}")
