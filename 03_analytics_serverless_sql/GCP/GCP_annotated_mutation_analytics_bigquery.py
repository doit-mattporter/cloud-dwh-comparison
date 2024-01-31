#!/usr/bin/env python
from google.cloud import bigquery
import configparser
import pandas as pd
import time

# Load configuration values
config = configparser.ConfigParser()
config.read("../../config.ini")
dataset_bucket = config["GCPConfig"]["OUTPUT_BUCKET"]
bigquery_project = config["GCPConfig"]["BIGQUERY_PROJECT"]
bigquery_dataset = config["GCPConfig"]["BIGQUERY_DATASET"]
cost_per_tb = float(config["GCPConfig"]["BIGQUERY_COST_PER_TB"])  # Cost per TB processed in USD for BigQuery on-demand

client = bigquery.Client(project=bigquery_project)

with open("GCP_annotated_mutation_analytics_bigquery.sql", "r") as f:
    sql_commands = f.read().split(";")

sql_commands = [x.replace("`PROJECT_ID.DATASET.", f"`{bigquery_project}.{bigquery_dataset}.") for x in sql_commands]

start_time = time.time()
total_bytes_processed = 0

# Execute each SQL command and report on its cost
for i, sql in enumerate(cmd for cmd in sql_commands if cmd.strip()):
    print(f"Executing Query {i+1}\n{sql}\n")
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    query_job = client.query(sql.strip(), job_config=job_config)
    try:
        results = query_job.result()
        df = results.to_dataframe()
        bytes_processed = query_job.total_bytes_processed
        total_bytes_processed += bytes_processed
        query_cost = (bytes_processed / 1e12) * cost_per_tb
        print(f"Query {i+1} processed {bytes_processed / 1e9:.2f} GB of data. Cost: ${query_cost:.2f}\n")
        print(df.head(10))
    except Exception as e:
        print(f"Error in query {i+1}: {e}")

end_time = time.time()

total_cost = (total_bytes_processed / 1e12) * cost_per_tb

elapsed_time = end_time - start_time
print(f"Total time elapsed: {elapsed_time:.2f} seconds")
print(f"Total data processed: {total_bytes_processed / 1e9:.2f} GB")
print(f"Total cost: ${total_cost:.2f}")
