-- Relative percentage representation of mutation prevalence
WITH total_count AS (
  SELECT COUNT(*) AS cnt FROM public.gnomad_with_dbnsfp_annotations
)
SELECT info_af_range, COUNT(*) AS count,
       ROUND((COUNT(*) / total_count.cnt) * 100, 3) AS info_af_range_percentage
FROM public.gnomad_with_dbnsfp_annotations, total_count
GROUP BY info_af_range, total_count.cnt
ORDER BY info_af_range
LIMIT 25;

-- Count of rare mutations on each chromosome
SELECT chromosome,
       COUNT(*) AS total_variant_count,
       SUM(CASE WHEN info_af <= 0.01 THEN 1 ELSE 0 END) AS rare_variant_count,
       ROUND((SUM(CASE WHEN info_af <= 0.01 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS rare_variant_pct
FROM public.gnomad_with_dbnsfp_annotations
GROUP BY chromosome
LIMIT 24;

-- Relative percentage representation of clinical significance
WITH total_dbnsfp_count AS (
  SELECT COUNT(*) AS cnt FROM public.gnomad_with_dbnsfp_annotations
  WHERE clinvar_clnsig IS NOT NULL
)
SELECT clinvar_clnsig, COUNT(*) AS count,
       ROUND((COUNT(*) / total_dbnsfp_count.cnt) * 100, 3) AS clinvar_clnsig_percentage
FROM public.gnomad_with_dbnsfp_annotations, total_dbnsfp_count
WHERE clinvar_clnsig IS NOT NULL
GROUP BY clinvar_clnsig, total_dbnsfp_count.cnt
ORDER BY clinvar_clnsig_percentage DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, counts by chromosome
SELECT chromosome, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY_ABS >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
GROUP BY chromosome
ORDER BY count DESC
LIMIT 24;

-- Mutations more common in one sex by at least 5% frequency difference, counts by gene
SELECT genename, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY_ABS >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
GROUP BY genename
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, counts by clinical pathogenicity
SELECT clinvar_clnsig, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY_ABS >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
GROUP BY clinvar_clnsig
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, counts by clinical pathogenicity, mutation is more frequent in XX
SELECT clinvar_clnsig, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY >= 0.05
  AND chromosome NOT IN ('chrX', 'chrY')
GROUP BY clinvar_clnsig
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, counts by clinical pathogenicity, mutation is more frequent in XY
SELECT clinvar_clnsig, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE AF_XX_MINUS_AF_XY <= -0.05
  AND chromosome NOT IN ('chrX', 'chrY')
GROUP BY clinvar_clnsig
ORDER BY count DESC
LIMIT 25;
-- We see that there are a few variants that are more common in one sex than the other, but it's roughly equally distributed among males and females, and none are of clinical significance

-- Mutations more common in one sex by at least 5% frequency difference, unique to East Asian populations, counts by chromosome
SELECT chromosome, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE chromosome NOT IN ('chrX', 'chrY')
  AND ABS(info_af_eas_xx - info_af_eas_xy) >= 0.05
  AND ABS(info_af_afr_xx - info_af_afr_xy) < 0.05
  AND ABS(info_af_amr_xx - info_af_amr_xy) < 0.05
  AND ABS(info_af_asj_xx - info_af_asj_xy) < 0.05
  AND ABS(info_af_fin_xx - info_af_fin_xy) < 0.05
  AND ABS(info_af_mid_xx - info_af_mid_xy) < 0.05
  AND ABS(info_af_nfe_xx - info_af_nfe_xy) < 0.05
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
GROUP BY chromosome
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, unique to East Asian populations, counts by gene
SELECT genename, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE chromosome NOT IN ('chrX', 'chrY')
  AND ABS(info_af_eas_xx - info_af_eas_xy) >= 0.05
  AND ABS(info_af_afr_xx - info_af_afr_xy) < 0.05
  AND ABS(info_af_amr_xx - info_af_amr_xy) < 0.05
  AND ABS(info_af_asj_xx - info_af_asj_xy) < 0.05
  AND ABS(info_af_fin_xx - info_af_fin_xy) < 0.05
  AND ABS(info_af_mid_xx - info_af_mid_xy) < 0.05
  AND ABS(info_af_nfe_xx - info_af_nfe_xy) < 0.05
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
GROUP BY genename
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, unique to East Asian populations, counts by clinical pathogenicity
SELECT clinvar_clnsig, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE chromosome NOT IN ('chrX', 'chrY')
  AND ABS(info_af_eas_xx - info_af_eas_xy) >= 0.05
  AND ABS(info_af_afr_xx - info_af_afr_xy) < 0.05
  AND ABS(info_af_amr_xx - info_af_amr_xy) < 0.05
  AND ABS(info_af_asj_xx - info_af_asj_xy) < 0.05
  AND ABS(info_af_fin_xx - info_af_fin_xy) < 0.05
  AND ABS(info_af_mid_xx - info_af_mid_xy) < 0.05
  AND ABS(info_af_nfe_xx - info_af_nfe_xy) < 0.05
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
GROUP BY clinvar_clnsig
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, unique to East Asian populations, counts by clinical pathogenicity, mutation is more frequent in XX
SELECT clinvar_clnsig, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE chromosome NOT IN ('chrX', 'chrY')
  AND info_af_eas_xx - info_af_eas_xy >= 0.05
  AND ABS(info_af_afr_xx - info_af_afr_xy) < 0.05
  AND ABS(info_af_amr_xx - info_af_amr_xy) < 0.05
  AND ABS(info_af_asj_xx - info_af_asj_xy) < 0.05
  AND ABS(info_af_fin_xx - info_af_fin_xy) < 0.05
  AND ABS(info_af_mid_xx - info_af_mid_xy) < 0.05
  AND ABS(info_af_nfe_xx - info_af_nfe_xy) < 0.05
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
GROUP BY clinvar_clnsig
ORDER BY count DESC
LIMIT 25;

-- Mutations more common in one sex by at least 5% frequency difference, unique to East Asian populations, counts by clinical pathogenicity, mutation is more frequent in XY
SELECT clinvar_clnsig, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE chromosome NOT IN ('chrX', 'chrY')
  AND info_af_eas_xx - info_af_eas_xy <= -0.05
  AND ABS(info_af_afr_xx - info_af_afr_xy) < 0.05
  AND ABS(info_af_amr_xx - info_af_amr_xy) < 0.05
  AND ABS(info_af_asj_xx - info_af_asj_xy) < 0.05
  AND ABS(info_af_fin_xx - info_af_fin_xy) < 0.05
  AND ABS(info_af_mid_xx - info_af_mid_xy) < 0.05
  AND ABS(info_af_nfe_xx - info_af_nfe_xy) < 0.05
  AND ABS(info_af_sas_xx - info_af_sas_xy) < 0.05
GROUP BY clinvar_clnsig
ORDER BY count DESC
LIMIT 25;

-- Genes containing the most pathogenic variants
SELECT genename, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig IN ('Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic', 'Pathogenic|association', 'Pathogenic|drug_response')
GROUP BY genename
ORDER BY count DESC
LIMIT 25;

-- Genes containing the most pathogenic variants that have undergone critical clinical review
SELECT genename, COUNT(*) AS count
FROM public.gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig = 'Pathogenic'
  AND clinvar_review = 'reviewed_by_expert_panel'
GROUP BY genename
ORDER BY count DESC
LIMIT 25;

-- Minimum and maximum population frequencies for known pathogenics for each gene
SELECT genename,
       MIN(info_af) AS known_pathogenic_af_min,
       MAX(info_af) AS known_pathogenic_af_max
FROM public.gnomad_with_dbnsfp_annotations
WHERE clinvar_clnsig = 'Pathogenic'
  AND clinvar_review = 'reviewed_by_expert_panel'
GROUP BY genename
ORDER BY known_pathogenic_af_max DESC
LIMIT 25;

-- Find the top 25 variants located within a gene that have the highest allele frequency difference across all populations
-- Show those variants alongside information on what the gene function is that they reside within, as well as their clinical significance
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
        public.gnomad_with_dbnsfp_annotations
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
    g."pathway(kegg)_full",
    ad.clinvar_clnsig,
    ad.clinvar_trait,
    ad.clinvar_review,
    ad.clinvar_hgvs
FROM
    AF_Differences ad
JOIN
    public.dbnsfp_gene g
ON
    ad.genename = g.Gene_name
ORDER BY
    ad.AF_Diff DESC
LIMIT 25;
