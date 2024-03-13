# Run this on a PySpark cluster on AWS EMR
# nohup spark-submit AWS_annotated_mutation_analytics_spark.py &
import sys

from pyarrow import hdfs
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

# Target MBs size for each Parquet filepart
target_partition_size_mb = 256
hdfs_fs = hdfs.connect()

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
df_merge.write.parquet("hdfs:///tmp/sorted.parquet", mode="overwrite")

# Determine ideal partition count
compressed_sample_size_mb = (
    sum([hdfs_fs.info(f)["size"] for f in hdfs_fs.ls("hdfs:///tmp/sorted.parquet")])
    / 1024**2
)
ideal_num_partitions = max(int(compressed_sample_size_mb / target_partition_size_mb), 1)
print(f"Using {ideal_num_partitions} partitions")
spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
# Write to S3 using ideal partition count
df = spark.read.parquet("hdfs:///tmp/sorted.parquet")
df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
df_merge.write.mode("overwrite").format("parquet").saveAsTable(
    f"gnomad_with_dbnsfp_annotations",
    path=f"s3://{output_bucket}/merged/gnomad_with_dbnsfp_annotations.parquet",
)
spark.sql(
    f"ANALYZE TABLE gnomad_with_dbnsfp_annotations COMPUTE STATISTICS FOR ALL COLUMNS"
)
