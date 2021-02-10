This is the WGS alignment workflow corresponding to the manuscript _Whole-genome analysis of
Nigerian patients with breast cancer reveals ethnic-driven somatic evolution and distinct genomic
subtypes_ (Ansari-Pour and Zheng, et al; under review). It is based on the GATK4 [format
conversion](https://github.com/gatk-workflows/seq-format-conversion) and [best-practices
workflows](https://github.com/gatk-workflows/gatk4-data-processing). 


# Installation
First, checkout this repository:
```
git clone --recurse-submodules https://github.com/olopade-lab/alignment.git
```
A unix-based operating system, Docker, and Java 8 is required. To download the
version of cromwell corresponding to the manuscript above, run
```
bash scripts/download_cromwell.sh
```
To download the most recent cromwell release, visit the [cromwell
releases](https://github.com/broadinstitute/cromwell/releases) page.
It is recommended to run with Singularity.
To run the helper script to make input lists, Python 3 is required.

# Configuration
The [Cromwell](https://cromwell.readthedocs.io/en/stable/Configuring/)
configuration can be found [here](configs/singularity_slurm.conf).  The
workflow configuration can be found [here](configs/HumanG1Kv37.json).
There is an example config with small truncated input files for testing
[here](configs/small_test.HumanG1Kv37.json). The configuration should
specify the location of the raw input data in TSV format. Example files
are included. Both fastq and bam inputs are supported.

If using the Singularity configuration, all required images will be
downloaded once and cached. The location of the cache can be configured
by setting the environment variable `SINGULARITY_CACHEDIR` (default is
`$HOME/.singularity/cache`).

The workflow configuration is based on the GATK recommendation for b37,
found [here](https://github.com/gatk-workflows/gatk4-data-processing/blob/master/processing-for-variant-discovery-gatk4.b37.wgs.inputs.json).
We are using the human_g1k_v37 reference, which is identical to b37
except that it is missing some decoy sequences. See
[here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890711-GRCh37-hg19-b37-humanG1Kv37-Human-Reference-Discrepancies)
and
[here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890811)
for more details.


# Execution
To run the small example (this should work out-of-the-box-- just substitute your cromwell executable name below): 
```
java -jar cromwell-55.jar run align.wdl -i configs/small_test.HumanG1Kv37.json
```

To run the full workflow on Dolores using Singularity:
```
java -Dconfig.file=configs/singularity_slurm.conf -jar cromwell-55.jar run align.wdl -i configs/HumanG1Kv37.json
```
Note this must be run from a machine with Singularity available (any of
the compute nodes.) A helper script for creating the input file TSV
lists can be found [here](scripts/make_filelists.py).
