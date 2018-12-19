# rat-strain-loader-pipeline
Load rat strain data from VCF files into RGD database and precomputes data for use by Variant Visualizer tool

ClinVar module
-
Load ClinVar variants into VariantVisualizer tool, for assemblies GRCh37 and GRCh38
1. extract ClinVar variants from RGD tables into VCF files
2. convert VCF files into Common Format files
3. load variants from Common Format files into VARIANT_HUMAN table in RATCNxxx schema
4. run postprocessing on those variants and update VARIANT_TRANSCRIPT_HUMAN table in RATCNxxx schema

Common Format 2
-
tab-separated file, with the following columns
 1. chr
 2. position
 3. ref nucleotide
 4. var nucleotide (allele) -- 1 allele per line
 5. rsId -- if available
 6. A count - count of occurrences of A genotype (AD field in VCF files)
 7. C count (AD field in VCF files)
 8. G count (AD field in VCF files)
 9. T count (AD field in VCF files)
 10. total_depth (DP field in VCF files)
 11. hgvs_name
 12. rgd_id
 13. allele count
