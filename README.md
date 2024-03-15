# cloud-dwh-comparison
This code base pairs with the following blog post: [GCP vs AWS Data Warehousing and Analytics: Which Service to Pick?](https://medium.com/p/acfc250c1096).

Presented below are the instructions required to replicate the findings and benchmarks presented in the blog. Execution of these scripts assume that they are being run in an AWS account / GCP playground where the AWS CLI / gcloud CLI have been authenticated with an IAM entity that possesses Admin/Project Owner IAM roles over the account / playground.

In order to capture the costs for each step, only run benchmarking scripts within a dedicated playground AWS account or GCP playground.

I recommend running only one step a day per cloud environment, and on the following day using the [DoiT Navigator's Reporting capability](https://help.doit.com/docs/cloud-analytics/reports) to not only determine total spend per step but also easily dive into the specific services and SKUs contributing to that spend.

A detailed explanation of what resources are being spun up by each step and contributing to each step's costs are detailed in `Appendix.md`.


## config.ini

Prior to running *any* of the following scripts, you must update the top-level `config.ini` file so that all variables have a valid value set.

Once these values are filled in, the scripts you will run in the following steps will automate the benchmarking process.

## Part 1: Data acquisition and transformation

* AWS
  * Run `01_dataprep/AWS/01a_AWS_spark_cluster_with_glow.sh` to convert gnomAD bgzipped VCFs to Parquet with AWS EMR.
  * Run `01_dataprep/AWS/02a_AWS_grab_dbNSFP.sh` to first retrieve a large dbNSFP zip file and upload its unzipped contents to S3 using an EC2 instance, then convert those flat files to Parquet with AWS EMR.
  * Run `01_dataprep/AWS/03_AWS_annotated_mutation_analytics_spark_union_dbs.sh` to union together the individual chromosome-specific Parquet files created earlier for both gnomAD and dbNSFP, then join these two chromosome-union'd databases together, create some new fields, and write out a merged databases Parquet file using AWS EMR.
* GCP
  * Run `01_dataprep/GCP/01a_GCP_spark_cluster_with_glow.sh` to convert gnomAD bgzipped VCFs to Parquet with GCP Dataproc.
  * Run `01_dataprep/GCP/02a_GCP_grab_dbNSFP.sh` to first retrieve a large dbNSFP zip file and upload its unzipped contents to GCS using a Compute Engine instance, then convert those flat files to Parquet with GCP Dataproc.
  * Run `01_dataprep/GCP/03_GCP_annotated_mutation_analytics_spark_union_dbs.sh` to union together the individual chromosome-specific Parquet files created earlier for both gnomAD and dbNSFP, then join these two chromosome-union'd databases together, create some new fields, and write out a merged databases Parquet file using GCP Dataproc.

## Part 2: The Cost of 17 Complex Analytical Queries
### Option 1: Ephemeral Spark Clusters as a Data Warehouse

* AWS
  * Run `02_analytics_spark/AWS/AWS_annotated_mutation_analytics_spark_analytics.sh` to execute 17 complex genomics OLAP queries against the merged databases Parquet file created at the end of Part 1 by using AWS EMR.
* GCP
  * Run `02_analytics_spark/GCP/GCP_annotated_mutation_analytics_spark_analytics.sh` to execute 17 complex genomics OLAP queries against the merged databases Parquet file created at the end of Part 1 by using GCP Dataproc.

### Option 2: Serverless DWH offerings

* AWS
  * Run `03_analytics_serverless_dwh/AWS/AWS_annotated_mutation_analytics_athena.py` to execute 17 complex genomics OLAP queries against the merged databases Parquet file created at the end of Part 1 by using AWS Athena. This script will report:
    * Total query time
    * Total data processed
    * Total OLAP query cost
  * Run `03_analytics_serverless_dwh/AWS/redshift_serverless_setup.sh` to prepare a Redshift Serverless instance for data table loading and query execution.
    * NOTE: It is, surprisingly, not possible to initially create a Redshift Serverless instance through CLI commands or any other automated means. There are manual steps detailed in header comments in this script which you MUST first follow before running this script.
  * Run `03_analytics_serverless_dwh/AWS/AWS_annotated_mutation_analytics_redshift_serverless.py` to load the merged databases Parquet file created at the end of Part 1 and execute 17 complex genomics OLAP queries against it by using AWS Redshift Serverless. This script will report:
    * Individual query runtimes and cost (so that table load runtimes can be calculated)
    * Total analytical query time (excludes table load runtimes)
    * Total data processed
    * Total OLAP query cost
* GCP
  * Run `03_analytics_serverless_dwh/GCP/load_data_into_bigquery.sh` to load the merged databases Parquet file created at the end of Part 1 into BigQuery. This script will report:
    * Total monthly storage cost
    * Total script / table load runtime
    * (Table load operations are free in BigQuery)
  * Run `03_analytics_serverless_dwh/GCP/GCP_annotated_mutation_analytics_bigquery.py` to execute 17 complex genomics OLAP queries against the merged databases Parquet file created at the end of Part 1 by using BigQuery. This script will report:
    * Total query time
    * Total data processed
    * Total OLAP query cost