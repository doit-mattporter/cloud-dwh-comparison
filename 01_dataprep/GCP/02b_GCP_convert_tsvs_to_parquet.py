# Run this on a PySpark cluster on GCP Dataproc
# nohup spark-submit 02b_AWS_convert_tsvs_to_parquet.py &

import sys

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit
from pyspark.sql.types import StringType

# Load configuration values
if len(sys.argv) > 1:
    output_bucket = sys.argv[1]
else:
    raise ValueError(
        "No output bucket specified. Please provide an output bucket as the first argument."
    )

spark = (
    SparkSession.builder.appName("Convert_dbNSFP_TSVs_to_Parquet")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .enableHiveSupport()
    .getOrCreate()
)


def calculate_total_folder_size_mbs(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    total_size_bytes = sum([blob.size for blob in blobs])
    total_size_mb = total_size_bytes / 1024 / 1024
    return total_size_mb


def delete_files(bucket_name, files_to_delete):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    for file_path in files_to_delete:
        blob = bucket.blob(file_path)
        print(f"Deleting {file_path}...")
        blob.delete()


# Target MBs size for each Parquet filepart
target_partition_size_mb = 256

# Convert the dbNSFP dataset TSVs to Parquet
column_renaming = {
    "#chr": "chromosome",
    "pos(1-based)": "start_position",
    "ref": "refallele",
    "alt": "altallele",
}
for chrom in [x for x in range(1, 23)] + ["X", "Y"]:
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    input_path = f"gs://{output_bucket}/dbNSFP/dbNSFP4.6a_variant.chr{chrom}.tsv"
    output_parquet_path = (
        f"gs://{output_bucket}/dbNSFP/dbNSFP4_4a_variant_chr{chrom}.parquet"
    )
    print(f"Converting {input_path} to {output_parquet_path}")
    df = spark.read.csv(input_path, sep="\t", header=True)
    for old_col_name, new_col_name in column_renaming.items():
        df = df.withColumnRenamed(old_col_name, new_col_name)
    df = df.withColumn("chromosome", concat(lit("chr"), df["chromosome"]))
    df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
    df.write.parquet(
        f"gs://{output_bucket}/dbNSFP/tmp/sorted.parquet", mode="overwrite"
    )
    # Determine ideal partition count
    compressed_sample_size_mb = calculate_total_folder_size_mbs(
        output_bucket, f"dbNSFP/tmp/sorted.parquet"
    )
    ideal_num_partitions = max(
        int(compressed_sample_size_mb / target_partition_size_mb), 1
    )
    print(f"Using {ideal_num_partitions} partitions with chr{chrom}")
    spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
    # Write to GCS using ideal partition count
    df = spark.read.parquet(f"gs://{output_bucket}/dbNSFP/tmp/sorted.parquet")
    df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
    df.write.mode("overwrite").format("parquet").saveAsTable(
        f"dbNSFP_chr{chrom}", path=output_parquet_path
    )
    delete_files(output_bucket, [f"dbNSFP/dbNSFP4.6a_variant.chr{chrom}.tsv"])
    spark.sql(f"ANALYZE TABLE dbNSFP_chr{chrom} COMPUTE STATISTICS FOR ALL COLUMNS")

spark.conf.set("spark.sql.shuffle.partitions", 200)
input_path = f"gs://{output_bucket}/dbNSFP/dbNSFP4.6_gene_complete.tsv"
output_parquet_path = f"gs://{output_bucket}/dbNSFP/dbNSFP4.6_gene_complete.parquet"
print(f"Converting {input_path} to {output_parquet_path}")
df = spark.read.csv(input_path, sep="\t", header=True)
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.replace(".", "_"))

# Filter out columns with any value exceeding 65535 characters
for col_name in df.columns:
    if df.schema[col_name].dataType == StringType():
        df = df.filter(F.length(df[col_name]) <= 65535)

df = df.orderBy("Gene_name")
df.write.parquet(f"gs://{output_bucket}/dbNSFP/tmp/sorted.parquet", mode="overwrite")
# Determine ideal partition count
compressed_sample_size_mb = calculate_total_folder_size_mbs(
    output_bucket, f"dbNSFP/tmp/sorted.parquet"
)
ideal_num_partitions = max(int(compressed_sample_size_mb / target_partition_size_mb), 1)
print(f"Using {ideal_num_partitions} partitions with dbNSFP_gene")
spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
# Write to GCS using ideal partition count
df = spark.read.parquet(f"gs://{output_bucket}/dbNSFP/tmp/sorted.parquet")
df = df.orderBy("Gene_name")
df.write.mode("overwrite").format("parquet").saveAsTable(
    "dbNSFP_gene", path=output_parquet_path
)
delete_files(output_bucket, ["dbNSFP/dbNSFP4.6_gene_complete.tsv"])
spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.sql(f"ANALYZE TABLE dbNSFP_gene COMPUTE STATISTICS FOR ALL COLUMNS")


# Cleanup leftover temporary files in GCS
# This is necessary as preemptible secondary workers cannot contribute to HDFS,
# so their temporary outputs (required to avoid an excessively long query plan that crashes Spark) must go to GCS
def delete_folder(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        print(f"Deleting {blob.name}...")
        blob.delete()


temp_folders = ["sorted.parquet"]
for folder_path in temp_folders:
    delete_folder(output_bucket, f"dbNSFP/tmp/{folder_path}")

delete_folder(output_bucket, f"gnomad/tmp")
