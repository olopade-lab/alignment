import argparse
import glob
import os
import shutil

parser = argparse.ArgumentParser(
    description="Helper script to make input bam and fastq CSV files from a directory tree",
)
parser.add_argument(
    "--project_dir",
    default="/cephfs/PROJECTS/WABCS/Tumor-Normal",
    help="top-level directory to make lists from",
)
parser.add_argument("--sequencing_center", default="University of Chicago")
parser.add_argument("--platform_name", default="Illumina")
parser.add_argument("--run_date", default="''")
parser.add_argument("--fastq_1_wildcard", default="1_sequence.txt.gz")
parser.add_argument(
    "--fastq_2_wildcard",
    default="2_sequence.txt.gz",
)
parser.add_argument("--output", default=os.path.join(os.getcwd()))
args = parser.parse_args()

samples = [os.path.basename(p) for p in glob.glob(os.path.join(args.project_dir, "*"))]
bams = []
fastq_lists = []
fastq_tags = []
bam_tags = []
fastq_samples = []
bam_samples = []
for fn in ["fastqs.tsv", "bams.tsv", "fastq_lists"]:
    path = os.path.join(args.output, fn)
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)
    except FileNotFoundError:
        continue
os.makedirs(os.path.join(args.output, "fastq_lists"))
for sample in samples:
    if os.path.isdir(os.path.join(args.project_dir, sample, "FastQ")):
        for tag in ["normal", "tumor"]:
            path = os.path.join(args.project_dir, sample, "FastQ", tag.capitalize())
            fastq_tags += [tag]
            fastq_samples += [sample]
            fastq_1s = []
            fastq_2s = []
            fastq_1_wildcard_path = os.path.join(
                path, "*" + args.fastq_1_wildcard.strip() + "*"
            )
            for f in glob.glob(fastq_1_wildcard_path):
                if os.path.isfile(
                    f.replace(args.fastq_1_wildcard, args.fastq_2_wildcard)
                ):  # make sure we have paired files
                    fastq_1s += [f]
                    fastq_2s += [
                        f.replace(args.fastq_1_wildcard, args.fastq_2_wildcard)
                    ]
                else:
                    print("no paired fastq found for {}-- skipping it".format(f))
            if len(glob.glob(fastq_1_wildcard_path)) == 0:
                print("no files found matching {}".format(fastq_1_wildcard_path))
            elif len(fastq_1s) == 0:
                print("no paired fastqs found in {}".format(path))
            list_path = "{}/fastq_lists/{}_{}.tsv".format(args.output, sample, tag)
            with open(list_path, "w") as f:
                print("{}\t{}".format("fastq_1_path", "fastq_2_path"), file=f)
                for fastq_1, fastq_2 in zip(fastq_1s, fastq_2s):
                    print("{}\t{}".format(fastq_1, fastq_2), file=f)
            fastq_lists += [list_path]

    elif len(glob.glob(os.path.join(args.project_dir, sample, "Normal", "*"))) > 0:
        bams += [glob.glob(os.path.join(args.project_dir, sample, "Normal", "*bam"))[0]]
        bams += [glob.glob(os.path.join(args.project_dir, sample, "Tumor", "*bam"))[0]]
        bam_tags += ["normal", "tumor"]
        bam_samples += [sample, sample]

with open(os.path.join(args.output, "fastqs.tsv"), "a") as f:
    print(
        "\t".join(
            [
                "fastq_list",
                "sequencing_center",
                "platform_name",
                "run_date",
                "sample",
                "tag",
            ]
        ),
        file=f,
    )
    for l, sample, tag in zip(fastq_lists, fastq_samples, fastq_tags):
        print(
            "\t".join([l, args.sequencing_center, args.platform_name, args.run_date, sample, tag]), file=f
        )

with open(os.path.join(args.output, "bams.tsv"), "a") as f:
    print("{}\t{}\t{}".format("path", "sample", "tag"), file=f)
    for path, sample, tag in zip(bams, bam_samples, bam_tags):
        print("\t".join([path, sample, tag]), file=f)
