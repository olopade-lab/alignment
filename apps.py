from parsl import python_app

@python_app(cache=True)
def fastq_to_ubam(
        fastq_1,
        fastq_2,
        sample,
        sequencing_center,
        run_date,
        project_dir):

    import os
    import subprocess
    from parsl import File

    sample_dir = os.path.join(project_dir, 'processed', sample)
    inputs_dir = os.path.join(sample_dir, 'inputs')
    for d in [sample_dir, inputs_dir]:
        if not os.path.isdir(d):
            os.makedirs(d)

    options = {
        "use_relative_output_paths": 'true',
        "final_workflow_outputs_dir": sample_dir,
        "final_workflow_log_dir": os.path.join(sample_dir, 'workflow_logs'),
        "final_call_logs_dir": os.path.join(sample_dir, 'call_logs')
    }
    with open(os.path.join(project_dir, 'options.json'), 'w') as f:
        json.dump(options, f)

    sequence_id = subprocess.check_output('zcat {} | head -n 1'.format(fq_1), shell=True).decode().lstrip('@')
    try:
        # see https://en.wikipedia.org/wiki/FASTQ_format#Illumina_sequence_identifiers
        first_identifiers, second_identifiers = sequence_id.split()
        instrument, run, flowcell, lane, tile, x, y = first_identifiers.split(':')
        paired_end, filtered, control, index_sequence = second_identifiers.split(':')
    except Exception as e:
        raise RuntimeError('problem reading {}: {}'.format(fq_1, e))
    # https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups
    platform = '.'.join([flowcell, lane])
    readgroup = '.'.join([sample_id, run, platform])
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
        "ConvertPairedFastQsToUnmappedBamWf.make_fofn": 'false'
    }
    with open(os.path.join(inputs_dir, '{}.json'.format(readgroup)), 'w') as f:
        json.dump(options, f)

    executable = glob.glob(project_dir, 'cromwell*jar')[-1]

    # java -Dconfig.file=local.conf -jar cromwell-53.1.jar ...
    command = [
        'java',
        '-jar',
        executable,
        'run',
        os.path.join(project_dir, 'seq-format-conversion', 'paired-fastq-to-unmapped-bam.wdl'),
        '-i',
        os.path.join(inputs_dir, '{}.json'.format(readgroup)),
        '-o',
        os.path.join(project_dir, 'options.json')
    ]

    subprocess.check_output(command)

    return File(os.path.join(sample_dir, readgroup + '.unmapped.bam'))

@python_app(cache=True)
def bam_to_ubam(bam, sample, config):
    import json
    import os
    import subprocess
    import glob

    project_dir = config['project_dir']
    source_dir = config['source_dir']
    cromwell_config = config['cromwell_config']

    sample_dir = os.path.join(project_dir, 'processed', sample)
    os.makedirs(os.path.join(sample_dir, 'workflow_logs'), exist_ok=True)

    stderr = os.path.join(os.path.join(sample_dir, 'workflow_logs', 'bam_to_ubam_{}.stderr'.format(sample)))
    stdout = os.path.join(os.path.join(sample_dir, 'workflow_logs', 'bam_to_ubam_{}.stdout'.format(sample)))
    if os.path.isfile(os.path.join(sample_dir, 'unmapped_bams_list.txt')):
        with open(stderr, 'a') as f:
            print('found unmapped bams list; assuming bam to ubam has completed successfully')
        return glob.glob(os.path.join(sample_dir, '*.unmapped.bam'))

    options = {
        "use_relative_output_paths": 'true',
        "final_workflow_outputs_dir": sample_dir,
        "final_workflow_log_dir": os.path.join(sample_dir, 'workflow_logs'),
        "final_call_logs_dir": os.path.join(sample_dir, 'call_logs')
    }
    with open(os.path.join(sample_dir, 'options.json'), 'w') as f:
        json.dump(options, f, indent=4)

    inputs = {
        "BamToUnmappedBams.input_bam": bam
    }
    with open(os.path.join(sample_dir, 'bam-to-unmapped-bams.inputs.json'), 'w') as f:
        json.dump(inputs, f, indent=4)

    command = [
        'java',
        '-Dconfig.file={}'.format(cromwell_config) if cromwell_config else '',
        '-jar',
        glob.glob(os.path.join(source_dir, 'cromwell*jar'))[-1],
        'run',
        os.path.join(source_dir, 'seq-format-conversion', 'bam-to-unmapped-bams.wdl'),
        '-i',
        os.path.join(sample_dir, 'bam-to-unmapped-bams.inputs.json'),
        '-o',
        os.path.join(sample_dir, 'options.json')
    ]

    with open(stderr, 'a') as f:
        print('executing command: {}'.format(' '.join(command)), file=f)
    proc = subprocess.Popen(' '.join(command), stdout=open(stdout, 'w'), stderr=open(stderr, 'a'), shell=True, executable='/bin/bash')
    proc.wait()

    with open(stderr, 'a') as f:
        print('bam to ubam finished with return code: {}'.format(proc.returncode))

    unmapped_bams = glob.glob(os.path.join(sample_dir, '*.unmapped.bam'))
    with open(os.path.join(sample_dir, 'unmapped_bams_list.txt'), 'w') as f:
        for bam in unmapped_bams:
            print(bam, file=f)

    return unmapped_bams

@python_app
def localize_library(config):
    import json
    import os
    import shutil
    import subprocess

    library = os.path.join(config['project_dir'], 'library')
    os.makedirs(library, exist_ok=True)
    if os.path.isfile(os.path.join(library, 'localized_config.json')):
        try:
            with open(os.path.join(library, 'localized_config.json')) as f:
                localized_config = json.load(f)
                for key in config.keys():
                    if "PreProcessingForVariantDiscovery" not in key:
                        localized_config[key] = config[key]
            with open(os.path.join(library, 'localized_config.json'), 'w') as f:
                json.dump(localized_config, f, indent=4)
            return localized_config
        except json.decoder.JSONDecodeError:
            pass

    localized_config = config.copy()

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
        "PreProcessingForVariantDiscovery_GATK4.dbSNP_vcf_index"
        ]:

        if key not in config or config[key] == "":
            continue

        if os.path.isfile(config[key]):
            shutil.copy(config[key], library)
        elif config[key].startswith('gs://'):
            subprocess.check_output(['gsutil', 'cp', config[key], library])

        localized_config[key] = os.path.join(library, os.path.basename(config[key]))

    for key in [
        "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_VCFs",
        "PreProcessingForVariantDiscovery_GATK4.known_indels_sites_indices"
        ]:
        localized_config[key] = []
        for fn in config[key]:
            if os.path.isfile(fn):
                shutil.copy(fn, library)
            elif fn.startswith('gs://'):
                subprocess.check_output(['gsutil', 'cp', fn, library])
            localized_config[key] += [os.path.join(library, os.path.basename(fn))]

    with open(os.path.join(library, 'localized_config.json'), 'w') as f:
        json.dump(localized_config, f, indent=4)

    return localized_config

# TODO singularity
@python_app
def prepare_ref_auxiliary_files(localized_config, library):
    import os
    import subprocess
    import json

    if "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_sa" not in localized_config:
        fasta = localized_config["PreProcessingForVariantDiscovery_GATK4.ref_fasta"]
        command = [
            'docker',
            'run',
            '--rm',
            '-v {path}:{path}'.format(path=os.path.join(localized_config['project_dir'], 'library')),
            '--user root',
            'biocontainers/bwa:v0.7.17_cv1',
            'bwa',
            'index',
            fasta
        ]

        subprocess.check_output(' '.join(command), shell=True)

        for postfix in ["sa", "amb", "bwt", "ann", "pac"]:
            key = "PreProcessingForVariantDiscovery_GATK4.SamToFastqAndBwaMem.ref_{}".format(postfix)
            localized_config[key] = "{}.{}".format(fasta, postfix)

        with open(os.path.join(library, 'localized_config.json'), 'w') as f:
            json.dump(localized_config, f, indent=4)

    if "PreProcessingForVariantDiscovery_GATK4.ref_dict" not in localized_config:
        reference = localized_config["PreProcessingForVariantDiscovery_GATK4.ref_fasta"]
        command = [
            'docker',
            'run',
            '--rm',
            '-v {path}:{path}'.format(path=os.path.join(localized_config['project_dir'], 'library')),
            'broadinstitute/gatk:4.1.8.1',
            'java -jar gatk.jar CreateSequenceDictionary',
            '--REFERENCE {}'.format(reference),
            '--OUTPUT {}'.format(reference.rstrip('.fa') + '.dict')
        ]

        subprocess.check_output(' '.join(command), shell=True)
        localized_config["PreProcessingForVariantDiscovery_GATK4.ref_dict"] = reference + '.dict'


    with open(os.path.join(library, 'localized_config.json'), 'w') as f:
        json.dump(localized_config, f, indent=4)

    return localized_config

@python_app(cache=True)
def align(unmapped_bams, config, localized_config, sample_name, clean_inputs=False):
    # https://cloud.google.com/life-sciences/docs/resources/public-datasets/reference-genomes
    import json
    import os
    import subprocess
    import glob

    project_dir = config['project_dir']
    source_dir = config['source_dir']
    cromwell_config = config['cromwell_config']

    sample_dir = os.path.join(project_dir, 'processed', sample_name)
    os.makedirs(os.path.join(sample_dir, 'workflow_logs'), exist_ok=True)

    options = {
        "use_relative_output_paths": 'true',
        "final_workflow_outputs_dir": sample_dir,
        "final_workflow_log_dir": os.path.join(sample_dir, 'workflow_logs'),
        "final_call_logs_dir": os.path.join(sample_dir, 'call_logs')
    }
    with open(os.path.join(sample_dir, 'options.json'), 'w') as f:
        json.dump(options, f, indent=4)

    inputs = {key: value for key, value in localized_config.items() if "PreProcessingForVariantDiscovery" in key}
    inputs["PreProcessingForVariantDiscovery_GATK4.sample_name"] = sample_name
    inputs["PreProcessingForVariantDiscovery_GATK4.ref_name"] = config["PreProcessingForVariantDiscovery_GATK4.ref_name"]
    inputs["PreProcessingForVariantDiscovery_GATK4.flowcell_unmapped_bams_list"] = os.path.join(sample_dir, 'unmapped_bams_list.txt')
    inputs["PreProcessingForVariantDiscovery_GATK4.unmapped_bam_suffix"] = ".bam"
    with open(os.path.join(sample_dir, 'processing-for-variant-discovery-gatk4.inputs.json'), 'w') as f:
        json.dump(inputs, f, indent=4)

    checkpoint = os.path.join(sample_dir, '{}.{}.bam.md5'.format(
        sample_name,
        inputs["PreProcessingForVariantDiscovery_GATK4.ref_name"]
        )
    )
    if os.path.isfile(checkpoint):
        return

    command = [
        'java',
        '-Dconfig.file={}'.format(cromwell_config) if cromwell_config else '',
        '-jar',
        glob.glob(os.path.join(source_dir, 'cromwell*jar'))[-1],
        'run',
        os.path.join(source_dir, 'gatk4-data-processing', 'processing-for-variant-discovery-gatk4.wdl'),
        '-i',
        os.path.join(sample_dir, 'processing-for-variant-discovery-gatk4.inputs.json'),
        '-o',
        os.path.join(sample_dir, 'options.json')
    ]

    stderr = os.path.join(os.path.join(sample_dir, 'workflow_logs', 'align_{}.stderr'.format(sample_name)))
    stdout = os.path.join(os.path.join(sample_dir, 'workflow_logs', 'align_{}.stdout'.format(sample_name)))
    with open(stderr, 'a') as f:
        print('executing command: {}'.format(' '.join(command)), file=f)
    proc = subprocess.Popen(' '.join(command), stdout=open(stdout, 'w'), stderr=open(stderr, 'w'), shell=True, executable='/bin/bash')
    proc.wait()

    if (clean_inputs is True) and os.path.isfile(checkpoint):
        with open(stderr, 'a') as f:
            for bam in unmapped_bams:
                stat = os.stat(bam)
                links = subprocess.check_output(['find', config['source_dir'], '-inum', str(stat.st_ino)]).decode().split()
                print('removing links: {}'.format(', '.join(links)), file=f)
                for link in links:
                    os.unlink(link)
