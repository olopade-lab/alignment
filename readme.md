This is the WGS alignment workflow corresponding to the manuscript _Whole-genome analysis of
Nigerian patients with breast cancer reveals ethnic-driven somatic evolution and distinct genomic
subtypes_ (Ansari-Pour and Zheng, et al; under review). It is based on the GATK4 [format
conversion](https://github.com/gatk-workflows/seq-format-conversion) and [best-practices
workflows](https://github.com/gatk-workflows/gatk4-data-processing). 


# Installation
Docker and Python 3 are required. Installation via Conda is recommended. To install the library dependencies:
```
conda create --name alignment python=3.7
conda activate alignment
git clone https://github.com/olopade-lab/alignment.git
cd alignment
python setup.py install
```


# Configuration
The [Cromwell](https://cromwell.readthedocs.io/en/stable/Configuring/) and
[Parsl](https://parsl.readthedocs.io/en/stable/userguide/configuring.html) configurations can be
found [here](configs/docker_slurm.conf) and [here](configs/igsb.py), respectively. In particular,
you will need to modify `worker_init` in the Parsl config to activate your conda environment.
The full configuration can be found [here](configs/HumanG1Kv37.yaml). There is an example config with small
truncated input files for testing [here](configs/small_test.HumanG1Kv37.yaml). The configuration
should specify the location of the raw input data in CSV format. Example files are included. Both
fastq and bam inputs are supported.


# Execution
To run the small example (this should work out-of-the-box, without modifying the parsl, cromwell, or alignment configuration): 
```
align configs/small_test.HumanG1Kv37.yaml
```

To run the full workflow:
```
align configs/HumanG1Kv37.yaml
```
