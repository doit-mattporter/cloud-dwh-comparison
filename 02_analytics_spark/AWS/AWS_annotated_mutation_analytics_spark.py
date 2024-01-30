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
    raise ValueError("No output bucket specified. Please provide an output bucket as the first argument.")

spark = SparkSession.builder \
    .appName("Annotate_XX_XY_Large_Difference_Mutations") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.cbo.enabled", True) \
    .config("spark.sql.cbo.joinReorder.enabled", True) \
    .enableHiveSupport() \
    .getOrCreate()

# Target MBs size for each Parquet filepart
target_partition_size_mb = 256
hdfs_fs = hdfs.connect()

# Join together the population frequencies from the up-to-date version of the gnomAD dataset to their annotations in the dbNSFP dataset
df_gnomad_union = spark.read.table("gnomad_data_chr1")
for chrom in [x for x in range(2, 23)] + ["X", "Y"]:
    df_gnomad_chrom = spark.read.table(f"gnomad_data_chr{chrom}")
    df_gnomad_union = df_gnomad_union.union(df_gnomad_chrom)

df_gnomad_union = df_gnomad_union.withColumn("info_af_range", F.floor(df_gnomad_union["info_af"] / 0.05) * 0.05)
df_gnomad_union = df_gnomad_union.withColumn("AF_XX_MINUS_AF_XY", df_gnomad_union["INFO_AF_XX"] - df_gnomad_union["INFO_AF_XY"])
df_gnomad_union = df_gnomad_union.withColumn("AF_XX_MINUS_AF_XY_ABS", F.abs(df_gnomad_union["INFO_AF_XX"] - df_gnomad_union["INFO_AF_XY"]))
df_gnomad_union.createOrReplaceTempView("gnomAD")

df_dbnsfp_union = spark.read.table("dbNSFP_chr1")
for chrom in [x for x in range(2, 23)] + ["X", "Y"]:
    df_dbnsfp_chrom = spark.read.table(f"dbNSFP_chr{chrom}")
    df_dbnsfp_union = df_dbnsfp_union.union(df_dbnsfp_chrom)

df_dbnsfp_union.createOrReplaceTempView("dbNSFP")

gnomad_cols = ', '.join([f"gnomAD.`{col}`" for col in df_gnomad_union.columns if '_af_' in col.lower() or col.lower().startswith("af_") or col.lower().endswith("_af")])
dbnsfp_cols = ', '.join([f"dbNSFP.`{col}`" for col in df_dbnsfp_union.columns if col not in ["chromosome", "start_position", "refallele", "altallele"] and not col.startswith("gnomAD")])
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
ideal_num_partitions = max(
    int(compressed_sample_size_mb / target_partition_size_mb), 1
)
print(f"Using {ideal_num_partitions} partitions with chr{chrom}")
spark.conf.set("spark.sql.shuffle.partitions", ideal_num_partitions)
# Write to S3 using ideal partition count
df = spark.read.parquet("hdfs:///tmp/sorted.parquet")
df = df.orderBy("chromosome", "start_position", "refallele", "altallele")
df_merge.write.mode("overwrite").format("parquet").saveAsTable(f"gnomad_with_dbnsfp_annotations", path=f"s3://{output_bucket}/merged/gnomad_with_dbnsfp_annotations.parquet")
spark.sql(f"ANALYZE TABLE gnomad_with_dbnsfp_annotations COMPUTE STATISTICS FOR ALL COLUMNS")

# Run a bunch of analytics on this dataset
df_merge = spark.read.table("gnomad_with_dbnsfp_annotations")

# Let's see what the relative percentage representation is of mutation prevalence throughout the human population
total_count = df_merge.count()
df_merge.groupBy("info_af_range").count().withColumn("info_af_range_percentage", F.round(F.col("count") / total_count * 100, 3)).orderBy("info_af_range").show(n=50)

# Let's count how many mutations are rare (less than 1% presence in human population) on each chromosome
query = """
    SELECT
        chromosome,
        COUNT(*) as total_variant_count,
        SUM(CASE WHEN info_af <= 0.01 THEN 1 ELSE 0 END) as rare_variant_count,
        ROUND((SUM(CASE WHEN info_af <= 0.01 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as rare_variant_pct
    FROM
        gnomad_with_dbnsfp_annotations
    GROUP BY
        chromosome
"""
spark.sql(query).show()

# Let's look at the relative percentage representation of clinical significance of all mutations
total_dbnsfp_count = df_merge.filter("clinvar_clnsig IS NOT NULL").count()
df_merge.filter("clinvar_clnsig IS NOT NULL").groupBy("clinvar_clnsig").count().withColumn("clinvar_clnsig_percentage", F.round(F.col("count") / total_dbnsfp_count * 100, 3)).orderBy("clinvar_clnsig_percentage", ascending=False).show(n=100, truncate=False)

# Let's look at whether there are any mutations that are more common in one sex versus the other by at least a 5% population frequency difference
# Then, let's examine whether these mutations are more or less likely to be more prevalent in males or females, and if they have any clinical significance
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY_ABS >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
"""
df_af_xx_minus_xy = spark.sql(cmd)
print("Large XX vs. XY population frequency difference: Mutation counts by chromosome")
df_af_xx_minus_xy.groupBy("chromosome").count().orderBy("count", ascending=False).show(n=24)
print("Large XX vs. XY population frequency difference: Mutation counts by gene")
df_af_xx_minus_xy.groupBy("genename").count().orderBy("count", ascending=False).show(n=50)
print("Large XX vs. XY population frequency difference: Mutation counts by clinical pathogenicity")
df_af_xx_minus_xy.groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50)
print("Large XX vs. XY population frequency difference: Mutation counts by clinical pathogenicity, mutation is more frequent in XX")
df_af_xx_minus_xy.filter("AF_XX_MINUS_AF_XY >= 0.05").groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50)
print("Large XX vs. XY population frequency difference: Mutation counts by clinical pathogenicity, mutation is more frequent in XY")
df_af_xx_minus_xy.filter("AF_XX_MINUS_AF_XY <= -0.05").groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50)
# We see that there are a few variants that are more common in one sex than the other, but it's roughly equally distributed among males and females, and none are of clinical significance

# Let's look at whether there are mutations that are more common in one sex versus the other by at least a 5% population frequency difference, and in which this large of a difference
# is only observed among East Asian populations when compared to all other populations described in the gnomAD dataset:
# African, American, Ashkenazi Jewish, Finnish, Middle Eastern, Non-Finnish European, South Asian, and Other (unspecified population group)
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE chromosome NOT IN ('chrX', 'chrY')
  AND ABS(info_af_eas_xx - info_af_eas_xy) >= 0.05
  AND ABS(info_af_afr_xx - info_af_afr_xy) < 0.05
  AND ABS(info_af_amr_xx - info_af_amr_xy) < 0.05
  AND ABS(info_af_asj_xx - info_af_asj_xy) < 0.05
  AND ABS(info_af_fin_xx - info_af_fin_xy) < 0.05
  AND ABS(info_af_mid_xx - info_af_mid_xy) < 0.05
  AND ABS(info_af_nfe_xx - info_af_nfe_xy) < 0.05
  AND ABS(info_af_oth_xx - info_af_oth_xy) < 0.05
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
"""
df_af_xx_minus_xy_eas_unique = spark.sql(cmd)
print("Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by chromosome")
df_af_xx_minus_xy_eas_unique.groupBy("chromosome").count().orderBy("count", ascending=False).show(n=24)
print("Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by gene")
df_af_xx_minus_xy_eas_unique.groupBy("genename").count().orderBy("count", ascending=False).show(n=50, truncate=False)
print("Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by clinical pathogenicity")
df_af_xx_minus_xy_eas_unique.groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50, truncate=False)
print("Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by clinical pathogenicity, mutation is more frequent in XX")
df_af_xx_minus_xy_eas_unique.filter("info_af_eas_xx - info_af_eas_xy >= 0.05").groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50, truncate=False)
print("Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by clinical pathogenicity, mutation is more frequent in XY")
df_af_xx_minus_xy_eas_unique.filter("info_af_eas_xx - info_af_eas_xy <= -0.05").groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50, truncate=False)
# There are a few variants that are more common in one sex than the other and that are unique to the East Asian population,
# and again these are roughly equally distributed among males and females, with none of clinical significance

# Let's look at what genes contain the most variants defined as being pathogenic in their clinical significance
# We'll start by looking at the clinical significance values available:
df_merge.groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(n=50, truncate=False)

# Now we'll look for which genes contain the most pathogenic variants
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig IN ('Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic', 'Pathogenic|association', 'Pathogenic|drug_response')
"""
df_pathogenics = spark.sql(cmd)
print("Pathogenic variants: Mutation counts by gene")
df_pathogenics.groupBy("genename").count().orderBy(["count", "genename"], ascending=[False, True]).show(n=50, truncate=False)
# We see that gene USH2A tops this list with 25 pathogenic variants described
# A look at Medline Plus' entry reveals these are likely related to either Usher Syndrome: https://medlineplus.gov/genetics/gene/ush2a/#conditions
# "More than 400 mutations in the USH2A gene have been identified in people with Usher syndrome type II, which is characterized by a combination of hearing loss and vision loss associated with retinitis pigmentosa. Specifically, USH2A gene mutations cause a form of the disorder known as Usher syndrome type IIA (USH2A), which accounts for more than half of all cases of Usher syndrome type II."


# Let's continue forward by only looking at 'Pathogenic' variants (not 'Likely pathogenics'), and additionally narrow the list down to those
# that have undergone review by a panel of clinical experts, and thus validated as genuinely pathogenic.
# We'll start by looking at the clinical review values available:
df_merge.groupBy("clinvar_review").count().orderBy("count", ascending=False).show(n=50, truncate=False)

# Now we'll look for which genes contain the most pathogenic variants that have also undergone a critical clinical review:
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig = 'Pathogenic'
  AND clinvar_review = 'reviewed_by_expert_panel'
"""
df_known_pathogenics = spark.sql(cmd)
print("Known pathogenic variants: Mutation counts by gene")
df_known_pathogenics.groupBy("genename").count().orderBy(["count", "genename"], ascending=[False, True]).show(n=50, truncate=False)
# This shows a number of genes with 1 or 2 known pathogenics described. Included in this list is BRCA2, named after its relation to BReast CAncer, but also involved in several other cancers: https://medlineplus.gov/genetics/gene/brca2/#conditions

# Let's look at the minimum and maximum population frequencies for known pathogenics for each gene:
cmd = """
SELECT
  genename,
  MIN(info_af) AS known_pathogenic_af_min,
  MAX(info_af) AS known_pathogenic_af_max
FROM (
  SELECT genename, info_af FROM gnomad_with_dbnsfp_annotations
  WHERE clinvar_clnsig = 'Pathogenic'
    AND clinvar_review = 'reviewed_by_expert_panel'
)
GROUP BY genename
ORDER BY known_pathogenic_af_max DESC
"""
df_known_pathogenics_af_min_max = spark.sql(cmd)
print("Known Pathogenic variants: Minimum and maximum population frequencies by gene")
df_known_pathogenics_af_min_max.show(n=50, truncate=False)
# We can see that clinical known pathogenic variants are exceedingly rare; evolutionary pressure ensures this.
# The most common is only present in 1.67% of the population, and the least common is present in 0.00066%, or 66 in every 100,000,000 people.
