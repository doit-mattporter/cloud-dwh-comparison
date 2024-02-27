# Run this on a PySpark cluster on AWS EMR
# nohup spark-submit 02b_AWS_convert_tsvs_to_parquet.py &
import boto3
import sys
from pyarrow import hdfs
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

# Target MBs size for each Parquet filepart
target_partition_size_mb = 256

s3_client = boto3.client('s3')

def delete_s3_file(bucket, path):
    s3_client.delete_object(Bucket=bucket, Key=path)

# Convert the dbNSFP dataset TSVs to Parquet
column_renaming = {
    "#chr": "chromosome",
    "pos(1-based)": "start_position",
    "ref": "refallele",
    "alt": "altallele",
}
hdfs_fs = hdfs.connect()
for chrom in [x for x in range(1, 23)] + ["X", "Y"]:
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    input_path = f"s3://{output_bucket}/dbNSFP/dbNSFP4.6a_variant.chr{chrom}.tsv"
    output_parquet_path = (
        f"s3://{output_bucket}/dbNSFP/dbNSFP4_4a_variant_chr{chrom}.parquet"
    )
    print(f"Converting {input_path} to {output_parquet_path}")
    df = spark.read.csv(input_path, sep="\t", header=True)
    for old_col_name, new_col_name in column_renaming.items():
        df = df.withColumnRenamed(old_col_name, new_col_name)
    df = df.withColumn("chromosome", concat(lit("chr"), df["chromosome"]))
    df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
    df.write.parquet("hdfs:///tmp/sorted.parquet", mode="overwrite")
    # Determine ideal partition count
    compressed_sample_size_mb = (
        sum([hdfs_fs.info(f)["size"] for f in hdfs_fs.ls("hdfs:///tmp/sorted.parquet")])
        / 1024**2
    )
    ideal_num_partitions = max(
        int(compressed_sample_size_mb / target_partition_size_mb), 1
    )
    print(f"Using {ideal_num_partitions} partitions with chr{chrom}")
    spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
    # Write to S3 using ideal partition count
    df = spark.read.parquet("hdfs:///tmp/sorted.parquet")
    df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
    df.write.mode("overwrite").format("parquet").saveAsTable(
        f"dbNSFP_chr{chrom}", path=output_parquet_path
    )
    tsv_file_path = f"dbNSFP/dbNSFP4.6a_variant.chr{chrom}.tsv"
    delete_s3_file(output_bucket, tsv_file_path)
    spark.sql(f"ANALYZE TABLE dbNSFP_chr{chrom} COMPUTE STATISTICS FOR ALL COLUMNS")

spark.conf.set("spark.sql.shuffle.partitions", 200)
input_path = f"s3://{output_bucket}/dbNSFP/dbNSFP4.6_gene_complete.tsv"
output_parquet_path = f"s3://{output_bucket}/dbNSFP/dbNSFP4.6_gene_complete.parquet"
print(f"Converting {input_path} to {output_parquet_path}")
df = spark.read.csv(input_path, sep="\t", header=True)
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.replace(".", "_"))

# Filter out columns with any value exceeding 65535 characters
for col_name in df.columns:
    if df.schema[col_name].dataType == StringType():
        df = df.filter(F.length(df[col_name]) <= 65535)

df = df.orderBy("Gene_name")
df.write.parquet("hdfs:///tmp/sorted.parquet", mode="overwrite")
# Determine ideal partition count
compressed_sample_size_mb = (
    sum([hdfs_fs.info(f)["size"] for f in hdfs_fs.ls("hdfs:///tmp/sorted.parquet")])
    / 1024
    / 1024
)
ideal_num_partitions = max(int(compressed_sample_size_mb / target_partition_size_mb), 1)
print(f"Using {ideal_num_partitions} partitions with dbNSFP_gene")
spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
# Write to S3 using ideal partition count
df = spark.read.parquet("hdfs:///tmp/sorted.parquet")
df = df.orderBy("Gene_name")
df.write.mode("overwrite").format("parquet").saveAsTable(
    "dbNSFP_gene", path=output_parquet_path
)
tsv_file_path = "dbNSFP/dbNSFP4.6_gene_complete.tsv"
delete_s3_file(output_bucket, tsv_file_path)
spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.sql(f"ANALYZE TABLE dbNSFP_gene COMPUTE STATISTICS FOR ALL COLUMNS")
