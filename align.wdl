version 1.0

import "gatk4-data-processing/processing-for-variant-discovery-gatk4.wdl" as gatk4_preprocessing
import "seq-format-conversion/bam-to-unmapped-bams.wdl" as bam_conversion
import "seq-format-conversion/paired-fastq-to-unmapped-bam.wdl" as fastq_conversion
# import "util.wdl" as util

workflow align {
    input {
        File bam_inputs
        File fastq_inputs
        String project_dir

        String ref_name

        File ref_fasta
        File ref_fasta_index
        File ref_dict

        File dbSNP_vcf
        File dbSNP_vcf_index
        Array[File] known_indels_sites_VCFs
        Array[File] known_indels_sites_indices

        File ref_amb
        File ref_ann
        File ref_bwt
        File ref_pac
        File ref_sa
    }

    Array[Object] fastq_lists = read_objects(fastq_inputs)

    scatter (row in fastq_lists) {

        Array[Object] fastq_pairs = read_objects(row.fastq_list)

        scatter (pair in fastq_pairs) {
            call get_read_info {
                input:
                    fastq = pair.fastq_1_path,
                    sample = row.sample
            }
            call fastq_conversion.ConvertPairedFastQsToUnmappedBamWf as fastq_conversion {
                input:
                    sample_name = row.sample,
                    fastq_1 = pair.fastq_1_path,
                    fastq_2 = pair.fastq_2_path,
                    readgroup_name = get_read_info.info.read_group,
                    library_name = row.sample,
                    platform_unit = get_read_info.info.platform_unit,
                    run_date = row.run_date,
                    platform_name = row.platform_name,
                    sequencing_center = "'~{row.sequencing_center}'"
            }
        }

        call gatk4_preprocessing.PreProcessingForVariantDiscovery_GATK4 as fastq_processing {
            input:
                sample_name = row.sample + "." + row.tag,
                ref_name = ref_name,

                flowcell_unmapped_bams = fastq_conversion.output_unmapped_bam,
                unmapped_bam_suffix = ".txt",

                ref_fasta = ref_fasta,
                ref_fasta_index = ref_fasta_index,
                ref_dict = ref_dict,

                dbSNP_vcf = dbSNP_vcf,
                dbSNP_vcf_index = dbSNP_vcf_index,
                known_indels_sites_VCFs = known_indels_sites_VCFs,
                known_indels_sites_indices = known_indels_sites_indices,
                ref_amb = ref_amb,
                ref_ann = ref_ann,
                ref_bwt = ref_bwt,
                ref_pac = ref_pac,
                ref_sa = ref_sa
        }


    }

    Array[Object] bam_lists = read_objects(bam_inputs)

    scatter (row in bam_lists) {
        call bam_conversion.BamToUnmappedBams as bam_conversion {
            input:
                input_bam = row.path
        }

        call gatk4_preprocessing.PreProcessingForVariantDiscovery_GATK4 as bam_processing {
            input:
                sample_name = row.sample + "." + row.tag,
                ref_name = ref_name,

                flowcell_unmapped_bams = bam_conversion.output_bams,
                unmapped_bam_suffix = ".txt",

                ref_fasta = ref_fasta,
                ref_fasta_index = ref_fasta_index,
                ref_dict = ref_dict,

                dbSNP_vcf = dbSNP_vcf,
                dbSNP_vcf_index = dbSNP_vcf_index,
                known_indels_sites_VCFs = known_indels_sites_VCFs,
                known_indels_sites_indices = known_indels_sites_indices,
                ref_amb = ref_amb,
                ref_ann = ref_ann,
                ref_bwt = ref_bwt,
                ref_pac = ref_pac,
                ref_sa = ref_sa
        }
    }
}


task get_read_info {
    input {
        File fastq
        String sample
    }

    command <<<
        python <<CODE
        import subprocess

        fastq = "~{fastq}"
        sample = "~{sample}"

        if fastq.endswith(".gz"):
            sequence_id = (
                subprocess.check_output("zcat {} | head -n 1".format(fastq), shell=True)
                .decode()
                .lstrip("@")
            )
        else:
            sequence_id = (
                subprocess.check_output("head -n 1 {}".format(fastq), shell=True)
                .decode()
                .lstrip("@")
            )
        try:
            # see https://en.wikipedia.org/wiki/FASTQ_format#Illumina_sequence_identifiers
            first_identifiers, second_identifiers = sequence_id.split()
            instrument, run, flowcell, lane, tile, x, y = first_identifiers.split(":")
            # paired_end, filtered, control, index_sequence = second_identifiers.split(':')

            # https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups
            platform = ".".join([flowcell, lane])
            read_group = ".".join([sample, run, platform])
        except Exception:
            try:
                first_identifiers, paired_end_index = sequence_id.split("/")
                instrument, flowcell, tile, x, y = first_identifiers.split(":")
                platform = ".".join([flowcell, tile])
                read_group = ".".join([sample, platform])
            except Exception as e:
                raise RuntimeError("problem reading {}: {}".format(fastq, e))
        print("{}\t{}".format("platform_unit", "read_group"))
        print("{}\t{}".format(platform, read_group))
        CODE
    >>>

    output {
        Object info = read_object(stdout())

    }

    runtime {
        docker: "jfloff/alpine-python:3.8-slim"
    }
}
