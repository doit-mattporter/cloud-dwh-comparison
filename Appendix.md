# Appendix
The DoiT Navigator's Reporting capabilities were leveraged to simplify and speed up total cost determination for each Part. This platform unifies spending across all cloud providers and enables easy creation of reports with complex grouping and filtering criteria. For example, a breakdown of cloud spend by day, cloud provider, service, and SKU can be obtained within seconds with relative ease; this type of report was frequently leveraged to effortlessly gather the spending data reported in this article.

## Part 1: Data acquisition and transformation

On AWS, gnomAD: The EMR cluster v6.14.0 (the latest version compatible with the Maven project 'Glow') utilized 25x r7g.16xlarge preemptible machines with 100 GB gp3 EBS disks and an r7g.xlarge on-demand master with a 50 GB gp3 EBS disk. The total cost includes EC2 and EMR charges for spot machines, on-demand master, external IPs, and EBS storage, as well as S3 Regional Storage Class A and B operations.

On AWS, dbNSFP: Identical to the setup described above, but with 8x r7g.16xlarge spot machines instead of 25x. Also, the cost associated with a c7g.2xlarge machine with 2 TBs of gp3 EBS storage was included, as this machine was used to retrieve the dbNSFP zip file, unzip it, and upload its flat files to GCS.

On GCP, gnomAD: The Dataproc cluster v2.1-debian11 (the latest version compatible with the Maven project 'Glow') utilized 23x n2-highmem-64 preemptible machines, 2x n2-highmem-64 on-demand worker machines (as required by Dataproc) with 100 GB standard Persistent Disks (PDs), and an n2-standard-4 on-demand master with a 50 GB standard PD. The total cost includes Compute Engine and Dataproc charges for preemptible workers, on-demand workers and master, external IPs, and Persistent Disk storage, as well as Cloud Storage Regional Storage Class A and B operations.

On GCP, dbNSFP: Identical to the setup described above, but with 6x n2-highmem-64 preemptible machines instead of 23x. Also, the cost associated with a c3d-standard-8 machine with 2 TBs of balanced PD storage was included, as this machine was used to retrieve the dbNSFP zip file, unzip it, and upload its flat files to GCS.

## Part 2: Complex Analytics

Below is a summary of the complex analytics that the 17 queries being run to demonstrate the cost-effectiveness of DWH options are engaging in:
1. **Relative Percentage Representation of Mutation Prevalence**: Calculates the prevalence of mutations across different allele frequency ranges as a percentage of the total mutation count.
2. **Count of Rare Mutations on Each Chromosome**: Identifies mutations considered rare (allele frequency <= 1%) on each chromosome, calculating both the count and the percentage of these rare mutations relative to all mutations on the chromosome.
3. **Relative Percentage Representation of Clinical Significance**: Determines the proportion of mutations with specific clinical significances (as noted in the dbNSFP annotations) among all mutations with non-null clinical significance annotations.
4. **Sex-based Frequency Difference in Mutations**: Identifies mutations that exhibit at least a 5% frequency difference between males and females, excluding chromosomes X and Y, to focus on autosomal differences. This analysis is performed by general count, by chromosome, by gene, and by clinical pathogenicity, with further distinction between mutations more common in males (XY) and females (XX).
5. **Ethnic Specificity in Sex-based Frequency Difference**: Focuses on mutations that show a significant frequency difference between sexes exclusively in East Asian populations, with comparative exclusion based on frequency differences in other ethnic groups. This analysis is similar to the sex-based frequency difference but is specifically filtered for ethnic uniqueness in East Asian populations, evaluated by chromosome, gene, and clinical pathogenicity.
6. **Pathogenic Variants in Genes**: Counts the genes containing the most pathogenic variants, as defined by specific clinical significance labels. It also distinguishes between all pathogenic variants and those reviewed by an expert panel, offering insight into the genes most commonly associated with serious health implications.
7. **Population Frequencies for Known Pathogenic Variants**: Analyzes known pathogenic variants that have undergone critical clinical review, reporting the minimum and maximum allele frequencies for such variants within each gene.
8. **Allele Frequency Difference Across Populations**: Identifies the top 25 variants with the highest allele frequency differences across populations, alongside gene function and clinical significance. This aims to highlight genetic variability across different ethnic backgrounds and its potential clinical implications.

## Part 2, Option 1: Ephemeral Spark Clusters as a Data Warehouse

On AWS, Data Unification and Joining: The EMR cluster v7.0.0 (the latest version available) utilized 30x r7g.16xlarge preemptible machines with 100 GB gp3 EBS disks and an r7g.xlarge on-demand master with a 50 GB gp3 EBS disk. The total cost includes EC2 and EMR charges for spot machines, on-demand master, external IPs, and EBS storage, as well as S3 Regional Storage Class A and B operations.

On AWS, Analysis and Reporting: Identical to the setup described above, but with 3x r7g.16xlarge spot machines with 50 GB gp3 disks instead of 30x with 100 GB gp3 disks.

On GCP, Data Unification and Joining: The Dataproc cluster v2.1-debian11 (the latest version available) utilized 28x n2-highmem-64 preemptible machines, 2x n2-highmem-64 on-demand worker machines (as required by Dataproc) with 100 GB standard Persistent Disks (PDs), and an n2-standard-4 on-demand master with a 50 GB standard PD. The total cost includes Compute Engine and Dataproc charges for preemptible workers, on-demand workers and master, external IPs, and Persistent Disk storage, as well as Cloud Storage Regional Storage Class A and B operations.

On GCP, Analysis and Reporting: Identical to the setup described above, but with 1x n2-highmem-64 preemptible machines instead of 28x.

## Step 2, Option 2: Serverless DWH Offerings

AWS Athena and GCP BigQuery on-demand have similar pricing schemes: You pay for the TBs of data scanned in a query and per TB per month of data stored. AWS Redshift Serverless also charges per TB per month for data stored, but query cost determination is more complex as it charges based on the number of 'RPUs' or Redshift Processing Units charged to run queries. However, this value can be obtained immediately after running a query, similar to how Athena and BigQuery indicate how much data was scanned by a query after running it.

Thus, while it is possible to total up the cost of queries hitting all three of these DWHs with the DoiT Navigator's Reporting tool, given that determination of cost is straightforward when leveraging cloud provider SDKs, the scripts running queries against these three data warehouses have built-in capability for reporting on analysis and storage costs.