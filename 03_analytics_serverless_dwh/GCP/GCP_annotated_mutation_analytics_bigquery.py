#!/usr/bin/env python
import configparser
import time

import pandas as pd
from google.cloud import bigquery

# Load configuration values
config = configparser.ConfigParser()
config.read("../../config.ini")
dataset_bucket = config["GCPConfig"]["OUTPUT_BUCKET"]
bigquery_project = config["GCPConfig"]["BIGQUERY_PROJECT"]
bigquery_dataset = config["GCPConfig"]["BIGQUERY_DATASET"]
cost_per_tb = float(
    config["GCPConfig"]["BIGQUERY_COST_PER_TB"]
)  # Cost per TB processed in USD for BigQuery on-demand

client = bigquery.Client(project=bigquery_project)

with open("GCP_annotated_mutation_analytics_bigquery.sql", "r") as f:
    sql_commands = f.read().split(";")

sql_commands = [
    x.replace("`PROJECT_ID.DATASET.", f"`{bigquery_project}.{bigquery_dataset}.")
    for x in sql_commands
]

total_bytes_processed = 0
total_query_time = 0


def format_runtime(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_runtime = f"{hours}h{minutes}m{seconds:.2f}s"
    return formatted_runtime


# Execute each SQL command and report on its cost
for i, sql in enumerate(cmd for cmd in sql_commands if cmd.strip()):
    print(f"Executing Query {i+1}\n{sql}\n")
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    start_time = time.time()
    query_job = client.query(sql.strip(), job_config=job_config)
    try:
        results = query_job.result()
        end_time = time.time()
        query_runtime = end_time - start_time
        total_query_time += query_runtime
        df = results.to_dataframe()
        bytes_processed = query_job.total_bytes_processed
        total_bytes_processed += bytes_processed
        query_cost = (bytes_processed / 1e12) * cost_per_tb
        print(
            f"Query {i+1} processed {bytes_processed / 1e9:.2f} GB of data. Cost: ${query_cost:.2f}\n"
        )
        print(df.head(10))
    except Exception as e:
        print(f"Error in query {i+1}: {e}")

total_cost = (total_bytes_processed / 1e12) * cost_per_tb

print(f"Total query runtime: {format_runtime(total_query_time)}")
print(f"Total data processed: {total_bytes_processed / 1e9:.2f} GB")
print(f"Total cost: ${total_cost:.2f}")
