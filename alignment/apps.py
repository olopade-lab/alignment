import os
from parsl import python_app
import json
import alignment


@python_app
def make_ubam_list(config, sample, tag, *ubams):
    import os

    project_dir = config["project_dir"]
    workflow_dir = os.path.join(project_dir, "processed", sample, tag, "workflow")

    if not os.path.isfile(os.path.join(workflow_dir, "unmapped_bams_list.txt")):
        with open(os.path.join(workflow_dir, "unmapped_bams_list.txt"), "w") as f:
            for bam in ubams:
                if bam is not None:
                    print(bam, file=f)
    return ubams


def setup_sample(config, sample, tag, app):
    project_dir = config["project_dir"]
    source_dir = config["source_dir"]
    cromwell_config = config["cromwell_config"]

    sample_dir = os.path.join(project_dir, "processed", sample, tag)
    workflow_dir = os.path.join(sample_dir, "workflow")
    log_dir = os.path.join(workflow_dir, "logs")
    for d in [sample_dir, workflow_dir, log_dir]:
        os.makedirs(d, exist_ok=True)

    stderr = os.path.join(os.path.join(log_dir, app + ".stderr"))
    stdout = os.path.join(os.path.join(log_dir, app + ".stdout"))

    options = {
        "use_relative_output_paths": "true",
        "final_workflow_outputs_dir": sample_dir,
        "final_workflow_log_dir": log_dir,
        "final_call_logs_dir": log_dir,
    }
    with open(os.path.join(workflow_dir, "options.json"), "w") as f:
        json.dump(options, f, indent=4)

    return (
        project_dir,
        source_dir,
        cromwell_config,
        sample_dir,
        workflow_dir,
        stderr,
        stdout,
    )


@python_app
def fastq_to_ubam(
    fastq_1, fastq_2, sample, tag, path, sequencing_center, run_date, config
):

    import os
    import subprocess
    import json
    import glob
    from alignment.apps import setup_sample

    (
        project_dir,
        source_dir,
        cromwell_config,
        sample_dir,
        workflow_dir,
        stderr,
        stdout,
    ) = setup_sample(config, sample, tag, "fastq_to_ubam")

    if os.path.isfile(os.path.join(workflow_dir, "unmapped_bams_list.txt")):
        with open(stderr, "a") as f:
            print(
                "found unmapped bams list; assuming fastq to ubam has completed successfully"
            )
        return glob.glob(os.path.join(sample_dir, "*.unmapped.bam"))

    if fastq_1.endswith(".gz"):
        sequence_id = (
            subprocess.check_output("zcat {} | head -n 1".format(fastq_1), shell=True)
            .decode()
            .lstrip("@")
        )
    else:
        sequence_id = (
            subprocess.check_output("head -n 1 {}".format(fastq_1), shell=True)
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
        readgroup = ".".join([sample, run, platform])
    except Exception:
        try:
            first_identifiers, paired_end_index = sequence_id.split("/")
            instrument, flowcell, tile, x, y = first_identifiers.split(":")
            platform = ".".join([flowcell, tile])
            readgroup = ".".join([sample, platform])
        except Exception as e:
            raise RuntimeError("problem reading {}: {}".format(fastq_1, e))
    inputs = {
        "ConvertPairedFastQsToUnmappedBamWf.readgroup_name": readgroup,
        "ConvertPairedFastQsToUnmappedBamWf.sample_name": sample,
        "ConvertPairedFastQsToUnmappedBamWf.fastq_1": fastq_1,
        "ConvertPairedFastQsToUnmappedBamWf.fastq_2": fastq_2,
        "ConvertPairedFastQsToUnmappedBamWf.library_name": sample,
        "ConvertPairedFastQsToUnmappedBamWf.platform_unit": platform,
        "ConvertPairedFastQsToUnmappedBamWf.run_date": run_date,
        "ConvertPairedFastQsToUnmappedBamWf.platform_name": "illumina",
        "ConvertPairedFastQsToUnmappedBamWf.sequencing_center": sequencing_center,
        "ConvertPairedFastQsToUnmappedBamWf.make_fofn": "false",
    }
    input_config = os.path.join(workflow_dir, "{}.json".format(readgroup))
    if os.path.isfile(input_config):
        print("fastq to ubam has already been submitted for this read group")
        return None

    with open(input_config, "w") as f:
        json.dump(inputs, f)

    executable = glob.glob(os.path.join(source_dir, "cromwell*jar"))[-1]

    # java -Dconfig.file=local.conf -jar cromwell-53.1.jar ...
    command = [
        "java",
        "-jar",
        executable,
        "run",
        os.path.join(
            source_dir, "seq-format-conversion", "paired-fastq-to-unmapped-bam.wdl"
        ),
        "-i",
        os.path.join(workflow_dir, "{}.json".format(readgroup)),
        "-o",
        os.path.join(workflow_dir, "options.json"),
    ]

    subprocess.check_output(command)

    return os.path.join(sample_dir, readgroup + ".unmapped.bam")


@python_app
def bam_to_ubam(bam, sample, tag, config):
    import json
    import os
    import subprocess
    import glob
    from alignment.apps import setup_sample

    (
        project_dir,
        source_dir,
        cromwell_config,
        sample_dir,
        workflow_dir,
        stderr,
        stdout,
    ) = setup_sample(config, sample, tag, "bam_to_ubam")

    if os.path.isfile(os.path.join(workflow_dir, "unmapped_bams_list.txt")):
        with open(stderr, "a") as f:
            print(
                "found unmapped bams list; assuming bam to ubam has completed successfully"
            )
        return glob.glob(os.path.join(sample_dir, "*.unmapped.bam"))

    inputs = {"BamToUnmappedBams.input_bam": bam}
    with open(os.path.join(workflow_dir, "bam-to-unmapped-bams.inputs.json"), "w") as f:
        json.dump(inputs, f, indent=4)

    command = [
        "java",
        "-Dconfig.file={}".format(cromwell_config) if cromwell_config else "",
        "-jar",
        glob.glob(os.path.join(source_dir, "cromwell*jar"))[-1],
        "run",
        os.path.join(source_dir, "seq-format-conversion", "bam-to-unmapped-bams.wdl"),
        "-i",
        os.path.join(workflow_dir, "bam-to-unmapped-bams.inputs.json"),
        "-o",
        os.path.join(workflow_dir, "options.json"),
    ]

    with open(stderr, "a") as f:
        print("executing command: {}".format(" ".join(command)), file=f)
    proc = subprocess.Popen(
        " ".join(command),
        stdout=open(stdout, "w"),
        stderr=open(stderr, "a"),
        shell=True,
        executable="/bin/bash",
    )
    proc.wait()

    with open(stderr, "a") as f:
        print("bam to ubam finished with return code: {}".format(proc.returncode))

    unmapped_bams = glob.glob(os.path.join(sample_dir, "*.unmapped.bam"))
    with open(os.path.join(workflow_dir, "unmapped_bams_list.txt"), "w") as f:
        for bam in unmapped_bams:
            print(bam, file=f)

    return unmapped_bams


@python_app
def localize_library(config):
    import json
    import os
    import shutil
    import subprocess

    config["source_dir"] = os.path.abspath(
        os.path.split(os.path.split(alignment.__file__)[0])[0]
    )
    for key in ["bam_inputs", "fastq_inputs", "project_dir", "cromwell_config"]:
        if (key in config) and not os.path.isabs(config[key]):
            config[key] = os.path.join(config["source_dir"], config[key])
    library = os.path.join(config["project_dir"], "library")
    os.makedirs(library, exist_ok=True)
    if os.path.isfile(os.path.join(library, "localized_config.json")):
        try:
            with open(os.path.join(library, "localized_config.json")) as f:
                localized_config = json.load(f)
                for key in config.keys():
                    if "PreProcessingForVariantDiscovery" not in key:
                        localized_config[key] = config[key]
            with open(os.path.join(library, "localized_config.json"), "w") as f:
                json.dump(localized_config, f, indent=4)
            return localized_config
        except json.decoder.JSONDecodeError:
            pass

    localized_config = config.copy()

    def copy(path):
        if os.path.isfile(os.path.join(library, os.path.basename(path))):
            return os.path.join(library, os.path.basename(path))
        elif os.path.isfile(path):
            shutil.copy(path, library)
        elif path.startswith("gs://"):
            subprocess.check_output(["gsutil", "cp", path, library])
        elif path.startswith("ftp://") or path.startswith("https://"):
            command = [
                "wget",
                path,
                "-O",
                os.path.join(library, os.path.basename(path)),
            ]
            subprocess.check_output(command)
        return os.path.join(library, os.path.basename(path))

    for key in [
        "PreProcessingForVariantDiscovery_GATK4.ref_dict",
        "PreProcessingForVariantDiscovery_GATK4.ref_fasta",
        "PreProcessingForVariantDiscovery_GATK4.ref_fasta_index",
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_alt",
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_sa",
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_amb",
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_bwt",
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_ann",
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_pac",
        "PreProcessingForVariantDiscovery_GATK4.dbSNP_vcf",
        "PreProcessingForVariantDiscovery_GATK4.dbSNP_vcf_index",
    ]:

        if key not in config or config[key] == "":
            continue
        localized_config[key] = copy(config[key])

    for key in [
        "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_VCFs",
        "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_indices",
    ]:
        localized_config[key] = []
        if key in config:
            for fn in config[key]:
                localized_config[key] += [copy(fn)]

    with open(os.path.join(library, "localized_config.json"), "w") as f:
        json.dump(localized_config, f, indent=4)

    return localized_config


# TODO singularity
@python_app
def prepare_ref_auxiliary_files(localized_config, library=None):
    import os
    import subprocess
    import json

    if library is None:
        library = os.path.join(localized_config["project_dir"], "library")

    if (
        "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_sa"
        not in localized_config
    ):
        fasta = localized_config["PreProcessingForVariantDiscovery_GATK4.ref_fasta"]
        command = [
            "docker",
            "run",
            "--rm",
            "-v {path}:{path}".format(path=library),
            "--user root",
            "biocontainers/bwa:v0.7.17_cv1",
            "bwa",
            "index",
            fasta,
        ]

        subprocess.check_output(" ".join(command), shell=True)

        for postfix in ["sa", "amb", "bwt", "ann", "pac"]:
            key = "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_{}".format(
                postfix
            )
            localized_config[key] = "{}.{}".format(fasta, postfix)

        with open(os.path.join(library, "localized_config.json"), "w") as f:
            json.dump(localized_config, f, indent=4)

    if "PreProcessingForVariantDiscovery_GATK4.ref_dict" not in localized_config:
        reference = localized_config["PreProcessingForVariantDiscovery_GATK4.ref_fasta"]
        fn = os.path.splitext(reference)[0] + ".dict"
        if not os.path.isfile(os.path.join(library, fn)):
            command = [
                "docker",
                "run",
                "--rm",
                "-v {path}:{path}".format(path=library),
                "broadinstitute/gatk:4.1.8.1",
                "java -jar gatk.jar CreateSequenceDictionary",
                "--REFERENCE {}".format(reference),
                "--OUTPUT {}".format(fn),
            ]

            subprocess.check_output(" ".join(command), shell=True)
        localized_config["PreProcessingForVariantDiscovery_GATK4.ref_dict"] = fn

    if (
        "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_indices"
        not in localized_config
        or len(
            localized_config[
                "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_indices"
            ]
        )
        == 0
    ):
        localized_config[
            "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_indices"
        ] = []
        for index, vcf in enumerate(
            localized_config[
                "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_VCFs"
            ]
        ):
            if not vcf.endswith(".gz"):
                command = [
                    "docker",
                    "run",
                    "--rm",
                    "-v {path}:{path}".format(path=library),
                    "broadinstitute/gatk:4.1.8.1",
                    "bgzip",
                    vcf,
                ]
                subprocess.check_output(" ".join(command), shell=True)
                vcf += ".gz"
                localized_config[
                    "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_VCFs"
                ][index] = vcf

            if not os.path.isfile(
                os.path.join(library, os.path.basename(vcf) + ".tbi")
            ):
                command = [
                    "docker",
                    "run",
                    "--rm",
                    "-v {path}:{path}".format(path=library),
                    "broadinstitute/gatk:4.1.8.1",
                    "tabix",
                    "-f",
                    "-p",
                    "vcf",
                    vcf,
                ]
                subprocess.check_output(" ".join(command), shell=True)
            localized_config[
                "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_indices"
            ] += [vcf + ".tbi"]

    with open(os.path.join(library, "localized_config.json"), "w") as f:
        json.dump(localized_config, f, indent=4)

    return localized_config


@python_app
def align(unmapped_bams, config, localized_config, sample, tag, clean_inputs=False):
    # https://cloud.google.com/life-sciences/docs/resources/public-datasets/reference-genomes
    import json
    import os
    import subprocess
    import glob

    if unmapped_bams is None:
        return

    print("begin aligning ", sample, tag)

    (
        project_dir,
        source_dir,
        cromwell_config,
        sample_dir,
        workflow_dir,
        stderr,
        stdout,
    ) = setup_sample(config, sample, tag, "align")

    inputs = {
        key: value
        for key, value in localized_config.items()
        if "PreProcessingForVariantDiscovery" in key
    }
    inputs["PreProcessingForVariantDiscovery_GATK4.sample_name"] = sample
    inputs["PreProcessingForVariantDiscovery_GATK4.ref_name"] = config[
        "PreProcessingForVariantDiscovery_GATK4.ref_name"
    ]
    inputs[
        "PreProcessingForVariantDiscovery_GATK4.flowcell_unmapped_bams_list"
    ] = os.path.join(workflow_dir, "unmapped_bams_list.txt")
    inputs["PreProcessingForVariantDiscovery_GATK4.unmapped_bam_suffix"] = ".bam"
    with open(
        os.path.join(
            workflow_dir, "processing-for-variant-discovery-gatk4.inputs.json"
        ),
        "w",
    ) as f:
        json.dump(inputs, f, indent=4)

    checkpoint = os.path.join(
        sample_dir,
        "{}.{}.bam.md5".format(
            sample, inputs["PreProcessingForVariantDiscovery_GATK4.ref_name"]
        ),
    )
    if os.path.isfile(checkpoint):
        return

    command = [
        "java",
        "-Dconfig.file={}".format(cromwell_config) if cromwell_config else "",
        "-jar",
        glob.glob(os.path.join(source_dir, "cromwell*jar"))[-1],
        "run",
        os.path.join(
            source_dir,
            "gatk4-data-processing",
            "processing-for-variant-discovery-gatk4.wdl",
        ),
        "-i",
        os.path.join(
            workflow_dir, "processing-for-variant-discovery-gatk4.inputs.json"
        ),
        "-o",
        os.path.join(workflow_dir, "options.json"),
    ]

    with open(stderr, "a") as f:
        print("executing command: {}".format(" ".join(command)), file=f)
    proc = subprocess.Popen(
        " ".join(command),
        stdout=open(stdout, "w"),
        stderr=open(stderr, "w"),
        shell=True,
        executable="/bin/bash",
    )
    proc.wait()

    if (clean_inputs is True) and os.path.isfile(checkpoint):
        with open(stderr, "a") as f:
            for bam in unmapped_bams:
                stat = os.stat(bam)
                links = (
                    subprocess.check_output(
                        ["find", config["source_dir"], "-inum", str(stat.st_ino)]
                    )
                    .decode()
                    .split()
                )
                print("removing links: {}".format(", ".join(links)), file=f)
                for link in links:
                    os.unlink(link)
