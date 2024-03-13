# Run this on a PySpark cluster on AWS EMR
# nohup spark-submit AWS_annotated_mutation_analytics_spark.py &
import sys

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

# Run a bunch of analytics on this dataset
df_merge = spark.read.table("gnomad_with_dbnsfp_annotations")

# Let's see what the relative percentage representation is of mutation prevalence throughout the human population
total_count = df_merge.count()
df_merge.groupBy("info_af_range").count().withColumn(
    "info_af_range_percentage", F.round(F.col("count") / total_count * 100, 3)
).orderBy("info_af_range").show(n=25)

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
spark.sql(query).show(n=24)

# Let's look at the relative percentage representation of clinical significance of all mutations
total_dbnsfp_count = df_merge.filter("clinvar_clnsig IS NOT NULL").count()
df_merge.filter("clinvar_clnsig IS NOT NULL").groupBy(
    "clinvar_clnsig"
).count().withColumn(
    "clinvar_clnsig_percentage", F.round(F.col("count") / total_dbnsfp_count * 100, 3)
).orderBy(
    "clinvar_clnsig_percentage", ascending=False
).show(
    n=100, truncate=False
)

# Let's look at whether there are any mutations that are more common in one sex versus the other by at least a 5% population frequency difference
# Then, let's examine whether these mutations are more or less likely to be more prevalent in males or females, and if they have any clinical significance
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY_ABS >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
"""
df_af_xx_minus_xy = spark.sql(cmd)
print("Large XX vs. XY population frequency difference: Mutation counts by chromosome")
df_af_xx_minus_xy.groupBy("chromosome").count().orderBy("count", ascending=False).show(
    n=24
)
print("Large XX vs. XY population frequency difference: Mutation counts by gene")
df_af_xx_minus_xy.groupBy("genename").count().orderBy("count", ascending=False).show(
    n=25
)
print(
    "Large XX vs. XY population frequency difference: Mutation counts by clinical pathogenicity"
)
df_af_xx_minus_xy.groupBy("clinvar_clnsig").count().orderBy(
    "count", ascending=False
).show(n=25)
print(
    "Large XX vs. XY population frequency difference: Mutation counts by clinical pathogenicity, mutation is more frequent in XX"
)
df_af_xx_minus_xy.filter("AF_XX_MINUS_AF_XY >= 0.05").groupBy(
    "clinvar_clnsig"
).count().orderBy("count", ascending=False).show(n=25)
print(
    "Large XX vs. XY population frequency difference: Mutation counts by clinical pathogenicity, mutation is more frequent in XY"
)
df_af_xx_minus_xy.filter("AF_XX_MINUS_AF_XY <= -0.05").groupBy(
    "clinvar_clnsig"
).count().orderBy("count", ascending=False).show(n=25)
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
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
"""
df_af_xx_minus_xy_eas_unique = spark.sql(cmd)
print(
    "Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by chromosome"
)
df_af_xx_minus_xy_eas_unique.groupBy("chromosome").count().orderBy(
    "count", ascending=False
).show(n=24)
print(
    "Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by gene"
)
df_af_xx_minus_xy_eas_unique.groupBy("genename").count().orderBy(
    "count", ascending=False
).show(n=25, truncate=False)
print(
    "Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by clinical pathogenicity"
)
df_af_xx_minus_xy_eas_unique.groupBy("clinvar_clnsig").count().orderBy(
    "count", ascending=False
).show(n=25, truncate=False)
print(
    "Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by clinical pathogenicity, mutation is more frequent in XX"
)
df_af_xx_minus_xy_eas_unique.filter("info_af_eas_xx - info_af_eas_xy >= 0.05").groupBy(
    "clinvar_clnsig"
).count().orderBy("count", ascending=False).show(n=25, truncate=False)
print(
    "Large XX vs. XY population frequency difference unique to East Asia: Mutation counts by clinical pathogenicity, mutation is more frequent in XY"
)
df_af_xx_minus_xy_eas_unique.filter("info_af_eas_xx - info_af_eas_xy <= -0.05").groupBy(
    "clinvar_clnsig"
).count().orderBy("count", ascending=False).show(n=25, truncate=False)
# There are a few variants that are more common in one sex than the other and that are unique to the East Asian population,
# and again these are roughly equally distributed among males and females, with none of clinical significance

# Let's look at what genes contain the most variants defined as being pathogenic in their clinical significance
# We'll start by looking at the clinical significance values available:
df_merge.groupBy("clinvar_clnsig").count().orderBy("count", ascending=False).show(
    n=25, truncate=False
)

# Now we'll look for which genes contain the most pathogenic variants
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig IN ('Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic', 'Pathogenic|association', 'Pathogenic|drug_response')
LIMIT 25
"""
df_pathogenics = spark.sql(cmd)
print("Pathogenic variants: Mutation counts by gene")
df_pathogenics.groupBy("genename").count().orderBy(
    ["count", "genename"], ascending=[False, True]
).show(n=25, truncate=False)
# We see that gene USH2A tops this list with 25 pathogenic variants described
# A look at Medline Plus' entry reveals these are likely related to either Usher Syndrome: https://medlineplus.gov/genetics/gene/ush2a/#conditions
# "More than 400 mutations in the USH2A gene have been identified in people with Usher syndrome type II, which is characterized by a combination of hearing loss and vision loss associated with retinitis pigmentosa. Specifically, USH2A gene mutations cause a form of the disorder known as Usher syndrome type IIA (USH2A), which accounts for more than half of all cases of Usher syndrome type II."


# Let's continue forward by only looking at 'Pathogenic' variants (not 'Likely pathogenics'), and additionally narrow the list down to those
# that have undergone review by a panel of clinical experts, and thus validated as genuinely pathogenic.
# We'll start by looking at the clinical review values available:
df_merge.groupBy("clinvar_review").count().orderBy("count", ascending=False).show(
    n=25, truncate=False
)

# Now we'll look for which genes contain the most pathogenic variants that have also undergone a critical clinical review:
cmd = """
SELECT * FROM gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig = 'Pathogenic'
  AND clinvar_review = 'reviewed_by_expert_panel'
LIMIT 25
"""
df_known_pathogenics = spark.sql(cmd)
print("Known pathogenic variants: Mutation counts by gene")
df_known_pathogenics.groupBy("genename").count().orderBy(
    ["count", "genename"], ascending=[False, True]
).show(n=25, truncate=False)
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
LIMIT 25
"""
df_known_pathogenics_af_min_max = spark.sql(cmd)
print("Known Pathogenic variants: Minimum and maximum population frequencies by gene")
df_known_pathogenics_af_min_max.show(n=25, truncate=False)
# We can see that clinical known pathogenic variants are exceedingly rare; evolutionary pressure ensures this.
# The most common is only present in 1.67% of the population, and the least common is present in 0.00066%, or 66 in every 100,000,000 people.

# Find the top 25 variants located within a gene that have the highest allele frequency difference across all populations
# Show those variants alongside information on what the gene function is that they reside within, as well as their clinical significance
cmd = """
WITH AF_Differences AS (
    SELECT
        chromosome,
        start_position,
        refallele,
        altallele,
        ROUND((
            COALESCE(ABS(info_af_afr - info_af_amr), 0) +
            COALESCE(ABS(info_af_afr - info_af_asj), 0) +
            COALESCE(ABS(info_af_afr - info_af_eas), 0) +
            COALESCE(ABS(info_af_afr - info_af_fin), 0) +
            COALESCE(ABS(info_af_afr - info_af_nfe), 0) +
            COALESCE(ABS(info_af_amr - info_af_asj), 0) +
            COALESCE(ABS(info_af_amr - info_af_eas), 0) +
            COALESCE(ABS(info_af_amr - info_af_fin), 0) +
            COALESCE(ABS(info_af_amr - info_af_nfe), 0) +
            COALESCE(ABS(info_af_asj - info_af_eas), 0) +
            COALESCE(ABS(info_af_asj - info_af_fin), 0) +
            COALESCE(ABS(info_af_asj - info_af_nfe), 0) +
            COALESCE(ABS(info_af_eas - info_af_fin), 0) +
            COALESCE(ABS(info_af_eas - info_af_nfe), 0) +
            COALESCE(ABS(info_af_fin - info_af_nfe), 0)
        ), 3) AS af_diff,
        genename,
        clinvar_clnsig,
        clinvar_trait,
        clinvar_review,
        clinvar_hgvs
    FROM
        gnomad_with_dbnsfp_annotations
    WHERE
        genename IS NOT NULL
    AND genename != ''
)
SELECT
    ad.chromosome,
    ad.start_position,
    ad.refallele,
    ad.altallele,
    ad.af_diff,
    g.gene_full_name,
    g.function_description,
    g.disease_description,
    g.`pathway(kegg)_full`,
    ad.clinvar_clnsig,
    ad.clinvar_trait,
    ad.clinvar_review,
    ad.clinvar_hgvs
FROM
    AF_Differences ad
JOIN
    dbNSFP_gene g
ON
    ad.genename = g.Gene_name
ORDER BY
    ad.AF_Diff DESC
LIMIT 25;
"""
df_largest_freq_diff = spark.sql(cmd)
print(
    "Largest allele frequency differences across populations alongside gene function information"
)
df_largest_freq_diff.show(n=25, truncate=False)
