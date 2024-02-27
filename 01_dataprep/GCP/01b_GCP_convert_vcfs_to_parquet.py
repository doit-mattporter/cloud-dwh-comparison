# Run this on a PySpark cluster on GCP Dataproc
# Make sure to run this command to ensure Glow, a library for large-scale genomic and variant data analysis, is installed
# pip3 install glow.py
# Then launch pyspark with this command:
# nohup spark-submit --packages io.projectglow:glow-spark3_2.12:1.2.1 --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec 01b_GCP_convert_vcfs_to_parquet.py &
import glow
import sys
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode
from pyspark.sql.types import StructType

if len(sys.argv) > 1:
    output_bucket = sys.argv[1]
else:
    raise ValueError("No output bucket specified. Please provide an output bucket as the first argument.")

spark = (
    SparkSession.builder.appName("Convert_gnomAD_VCFs_to_Parquet")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.hadoop.io.compression.codecs", "io.projectglow.sql.util.BGZFCodec")
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


spark = glow.register(spark)

# Target MBs size for each Parquet filepart
target_partition_size_mb = 256

# Grab the gnomAD population frequency datasets:
# https://gnomad.broadinstitute.org/downloads#v3
for chrom in [x for x in range(1, 23)] + ["X", "Y"]:
    spark.conf.set("spark.sql.shuffle.partitions", 400)
    input_path = f"gs://gcp-public-data--gnomad/release/4.0/vcf/genomes/gnomad.genomes.v4.0.sites.chr{chrom}.vcf.bgz"
    output_parquet_path = (
        f"gs://{output_bucket}/gnomad/gnomad_genomes_v4.0_chr{chrom}.parquet"
    )
    print(f"Converting {input_path} to {output_parquet_path}")
    # First, convert the VCF to Parquet format
    # Identify and flatten any struct columns
    df = spark.read.format("vcf").load(input_path)
    struct_columns = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StructType)
    ]
    for col_name in struct_columns:
        for field in df.schema[col_name].dataType.fields:
            df = df.withColumn(
                f"{col_name}_{field.name}", col(col_name).getField(field.name)
            )
        # Drop the original struct column
        df = df.drop(col_name)
    df.write.parquet(
        f"gs://{output_bucket}/gnomad/tmp/vcf_as_parquet.parquet", mode="overwrite"
    )
    # Next, explode all ArrayType columns so that alternative alleles each get their own row (simplifies queries)
    df = spark.read.parquet(f"gs://{output_bucket}/gnomad/tmp/vcf_as_parquet.parquet")
    df = df.select(
        "*", posexplode(col("alternateAlleles")).alias("index", "alternateAllele")
    )
    array_columns = [
        f.name
        for f in df.schema.fields
        if f.dataType.simpleString().startswith("array")
        and f.name != "alternateAlleles"
    ]
    # Use the alternateAlleles-based index to select corresponding elements from all other array columns
    new_columns = [
        col(col_name).getItem(col("index")).alias(col_name)
        if col_name in array_columns
        else col(col_name)
        for col_name in df.columns
    ]
    df = df.select(*new_columns)
    df = df.drop("index")
    df = df.drop("alternateAlleles")
    df.write.parquet(
        f"gs://{output_bucket}/gnomad/tmp/exploded.parquet", mode="overwrite"
    )
    # Next, write another version of the Parquet file sorted on position
    df = spark.read.parquet(f"gs://{output_bucket}/gnomad/tmp/exploded.parquet")
    df = df.orderBy("contigName", "start", "referenceAllele", "alternateAllele")
    df.write.parquet(
        f"gs://{output_bucket}/gnomad/tmp/sorted.parquet", mode="overwrite"
    )
    # Determine ideal partition count
    compressed_sample_size_mb = calculate_total_folder_size_mbs(
        output_bucket, f"gnomad/tmp/sorted.parquet"
    )
    ideal_num_partitions = max(
        int(compressed_sample_size_mb / target_partition_size_mb), 1
    )
    print(f"Using {ideal_num_partitions} partitions with chr{chrom}")
    spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
    # Write to GCS using ideal partition count
    df = spark.read.parquet(f"gs://{output_bucket}/gnomad/tmp/sorted.parquet")
    df = df.orderBy("contigName", "start", "referenceAllele", "alternateAllele")
    df.write.mode("overwrite").format("parquet").saveAsTable(
        f"gnomad_data_chr{chrom}", path=output_parquet_path
    )
    columns_to_analyze = ", ".join(
        [
            field.name
            for field in df.schema.fields
            if not isinstance(field.dataType, StructType)
        ]
    )
    spark.sql(
        f"ANALYZE TABLE gnomad_data_chr{chrom} COMPUTE STATISTICS FOR COLUMNS {columns_to_analyze}"
    )


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


temp_folders = ["vcf_as_parquet.parquet", "exploded.parquet", "sorted.parquet"]
for folder_path in temp_folders:
    delete_folder(output_bucket, f"gnomad/tmp/{folder_path}")

delete_folder(output_bucket, f"gnomad/tmp")
