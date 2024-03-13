# Run this on a PySpark cluster on GCP Dataproc
# nohup spark-submit GCP_annotated_mutation_analytics_spark.py OUTPUT_BUCKET &
import sys

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Load configuration values
# Load configuration values
if len(sys.argv) > 1:
    output_bucket = sys.argv[1]
else:
    raise ValueError(
        "No output bucket specified. Please provide an output bucket as the first argument."
    )

spark = (
    SparkSession.builder.appName("Annotate_XX_XY_Large_Difference_Mutations")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.cbo.enabled", True)
    .config("spark.sql.cbo.joinReorder.enabled", True)
    .config("spark.sql.adaptive.enabled", True)
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


# Target MBs size for each Parquet filepart
target_partition_size_mb = 256


# Join together the population frequencies from the up-to-date version of the gnomAD dataset to their annotations in the dbNSFP dataset
df_gnomad_union = spark.read.table("gnomad_data_chr1")
for chrom in [x for x in range(2, 23)] + ["X", "Y"]:
    df_gnomad_chrom = spark.read.table(f"gnomad_data_chr{chrom}")
    df_gnomad_union = df_gnomad_union.union(df_gnomad_chrom)

df_gnomad_union = df_gnomad_union.withColumn(
    "info_af_range", F.floor(df_gnomad_union["info_af"] / 0.05) * 0.05
)
df_gnomad_union = df_gnomad_union.withColumn(
    "AF_XX_MINUS_AF_XY", df_gnomad_union["INFO_AF_XX"] - df_gnomad_union["INFO_AF_XY"]
)
df_gnomad_union = df_gnomad_union.withColumn(
    "AF_XX_MINUS_AF_XY_ABS",
    F.abs(df_gnomad_union["INFO_AF_XX"] - df_gnomad_union["INFO_AF_XY"]),
)
df_gnomad_union.createOrReplaceTempView("gnomAD")

df_dbnsfp_union = spark.read.table("dbNSFP_chr1")
for chrom in [x for x in range(2, 23)] + ["X", "Y"]:
    df_dbnsfp_chrom = spark.read.table(f"dbNSFP_chr{chrom}")
    df_dbnsfp_union = df_dbnsfp_union.union(df_dbnsfp_chrom)

df_dbnsfp_union.createOrReplaceTempView("dbNSFP")

gnomad_cols = ", ".join(
    [
        f"gnomAD.`{col}`"
        for col in df_gnomad_union.columns
        if "_af_" in col.lower()
        or col.lower().startswith("af_")
        or col.lower().endswith("_af")
    ]
)
dbnsfp_cols = ", ".join(
    [
        f"dbNSFP.`{col}`"
        for col in df_dbnsfp_union.columns
        if col not in ["chromosome", "start_position", "refallele", "altallele"]
        and not col.startswith("gnomAD")
    ]
)
cmd = f"""
SELECT
    gnomAD.contigName AS chromosome,
    gnomAD.start AS start_position,
    gnomAD.referenceAllele AS refallele,
    gnomAD.alternateAllele AS altallele,
    {gnomad_cols},
    {dbnsfp_cols}
FROM gnomAD
LEFT JOIN dbNSFP ON (gnomAD.contigName = dbNSFP.chromosome AND gnomAD.start = dbNSFP.start_position AND gnomAD.referenceAllele = dbNSFP.refallele AND gnomAD.alternateAllele = dbNSFP.altallele)
WHERE gnomAD.filters = 'PASS'
"""
df_merge = spark.sql(cmd)
df_merge.write.parquet(
    f"gs://{output_bucket}/merged/tmp/sorted.parquet", mode="overwrite"
)

# Determine ideal partition count
compressed_sample_size_mb = calculate_total_folder_size_mbs(
    output_bucket, f"merged/tmp/sorted.parquet"
)
ideal_num_partitions = max(int(compressed_sample_size_mb / target_partition_size_mb), 1)
print(f"Using {ideal_num_partitions} partitions")
spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
# Write to GCS using ideal partition count
df = spark.read.parquet(f"gs://{output_bucket}/merged/tmp/sorted.parquet")
df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
df_merge.write.mode("overwrite").format("parquet").saveAsTable(
    f"gnomad_with_dbnsfp_annotations",
    path=f"gs://{output_bucket}/merged/gnomad_with_dbnsfp_annotations.parquet",
)
spark.sql(
    f"ANALYZE TABLE gnomad_with_dbnsfp_annotations COMPUTE STATISTICS FOR ALL COLUMNS"
)


# Cleanup leftover temporary files in GCS
# This is necessary as pre-emptible secondary workers are not permitted to contribute to HDFS, and so their
# temporary outputs (required to avoid excessively long query plans that can crash Spark) must go to GCS
def delete_folder(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        print(f"Deleting {blob.name}...")
        blob.delete()


temp_folders = ["sorted.parquet"]
for folder_path in temp_folders:
    delete_folder(output_bucket, f"merged/tmp/{folder_path}")
