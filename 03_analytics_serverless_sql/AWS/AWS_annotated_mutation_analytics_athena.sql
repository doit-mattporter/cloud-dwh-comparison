-- Relative percentage representation of mutation prevalence
WITH total_count AS (
  SELECT COUNT(*) AS cnt FROM "default"."gnomad_with_dbnsfp_annotations"
)
SELECT info_af_range, COUNT(*) AS count,
       ROUND((COUNT(*) / total_count.cnt) * 100, 3) AS info_af_range_percentage
FROM "default"."gnomad_with_dbnsfp_annotations", total_count
GROUP BY info_af_range, total_count.cnt
ORDER BY info_af_range;

-- Count of rare mutations on each chromosome
SELECT chromosome,
       COUNT(*) AS total_variant_count,
       SUM(CASE WHEN info_af <= 0.01 THEN 1 ELSE 0 END) AS rare_variant_count,
       ROUND((SUM(CASE WHEN info_af <= 0.01 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS rare_variant_pct
FROM "default"."gnomad_with_dbnsfp_annotations"
GROUP BY chromosome;

-- Relative percentage representation of clinical significance
WITH total_dbnsfp_count AS (
  SELECT COUNT(*) AS cnt FROM "default"."gnomad_with_dbnsfp_annotations"
  WHERE clinvar_clnsig IS NOT NULL
)
SELECT clinvar_clnsig, COUNT(*) AS count,
       ROUND((COUNT(*) / total_dbnsfp_count.cnt) * 100, 3) AS clinvar_clnsig_percentage
FROM "default"."gnomad_with_dbnsfp_annotations", total_dbnsfp_count
WHERE clinvar_clnsig IS NOT NULL
GROUP BY clinvar_clnsig, total_dbnsfp_count.cnt
ORDER BY clinvar_clnsig_percentage DESC;

-- Mutations more common in one sex by at least 5% frequency difference, counts by gene
SELECT genename, COUNT(*) AS count
FROM "default"."gnomad_with_dbnsfp_annotations"
WHERE ABS(AF_XX_MINUS_AF_XY) >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
GROUP BY genename
ORDER BY count DESC;

-- Mutations more common in one sex by at least 5% frequency difference, unique to East Asian populations, counts by gene
SELECT genename, COUNT(*) AS count
FROM "default"."gnomad_with_dbnsfp_annotations"
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
GROUP BY genename
ORDER BY count DESC;

-- Genes containing the most pathogenic variants
-- Note: This query will generate a result set that you can use for further analysis, similar to df_pathogenics in your PySpark code.
SELECT genename, COUNT(*) AS count
FROM "default"."gnomad_with_dbnsfp_annotations"
WHERE clinvar_clnsig IN ('Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic', 'Pathogenic|association', 'Pathogenic|drug_response')
GROUP BY genename
ORDER BY count DESC;

-- Genes containing the most pathogenic variants that have undergone critical clinical review
-- Note: This query will generate a result set that you can use for further analysis, similar to df_known_pathogenics in your PySpark code.
SELECT genename, COUNT(*) AS count
FROM "default"."gnomad_with_dbnsfp_annotations"
WHERE clinvar_clnsig = 'Pathogenic'
  AND clinvar_review = 'reviewed_by_expert_panel'
GROUP BY genename
ORDER BY count DESC;

-- Minimum and maximum population frequencies for known pathogenics for each gene
SELECT genename,
       MIN(info_af) AS known_pathogenic_af_min,
       MAX(info_af) AS known_pathogenic_af_max
FROM "default"."gnomad_with_dbnsfp_annotations"
WHERE clinvar_clnsig = 'Pathogenic'
  AND clinvar_review = 'reviewed_by_expert_panel'
GROUP BY genename
ORDER BY known_pathogenic_af_max DESC;

