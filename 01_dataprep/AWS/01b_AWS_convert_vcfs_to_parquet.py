# Run this on a PySpark cluster on AWS EMR
# Make sure to run this command to ensure Glow, a library for large-scale genomic and variant data analysis, is installed
# pip3 install glow.py pyarrow
# Then launch pyspark with this command:
# nohup spark-submit --packages io.projectglow:glow-spark3_2.12:1.2.1 --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec 01b_AWS_convert_vcfs_to_parquet.py &
import sys
from pyarrow import hdfs
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
    .config("spark.jars.packages", "io.projectglow:glow-spark3_2.12:1.2.1")
    .config("spark.hadoop.io.compression.codecs", "io.projectglow.sql.util.BGZFCodec")
    .enableHiveSupport()
    .getOrCreate()
)

import glow

spark = glow.register(spark)

# Target MBs size for each Parquet filepart
target_partition_size_mb = 256

# Grab the gnomAD population frequency datasets:
# https://gnomad.broadinstitute.org/downloads#v3
hdfs_fs = hdfs.connect()

for chrom in [x for x in range(1, 23)] + ["X", "Y"]:
    spark.conf.set("spark.sql.shuffle.partitions", 400)
    input_path = f"s3://gnomad-public-us-east-1/release/4.0/vcf/genomes/gnomad.genomes.v4.0.sites.chr{chrom}.vcf.bgz"
    output_parquet_path = (
        f"s3://{output_bucket}/gnomad/gnomad_genomes_v4.0_chr{chrom}.parquet"
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
    df.write.parquet("hdfs:///tmp/vcf_as_parquet.parquet", mode="overwrite")
    # Next, explode all ArrayType columns so that alternative alleles each get their own row (simplifies queries)
    df = spark.read.parquet("hdfs:///tmp/vcf_as_parquet.parquet")
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
    df.write.parquet("hdfs:///tmp/exploded.parquet", mode="overwrite")
    # Next, write another version of the Parquet file sorted on position
    df = spark.read.parquet("hdfs:///tmp/exploded.parquet")
    df = df.orderBy("contigName", "start", "referenceAllele", "alternateAllele")
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
