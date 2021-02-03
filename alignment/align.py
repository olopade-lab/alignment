import time
import argparse
import pandas as pd
import glob
import os
import importlib
import yaml

import parsl

from alignment import apps


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    config["source_dir"] = os.path.dirname(os.path.dirname(apps.__file__))

    spec = importlib.util.spec_from_file_location("", config["parsl_config"])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    parsl.load(module.config)

    bams = pd.read_csv(
        config["bam_inputs"], comment="#", sep="\s*,\s*", engine="python"
    )
    fastqs = pd.read_csv(
        config["fastq_inputs"], comment="#", sep="\s*,\s*", engine="python"
    )
    for df in [bams, fastqs]:
        for column in df.columns:
            df[column] = df[column].astype(str).str.strip()

    localized_config = apps.prepare_ref_auxiliary_files(
        apps.localize_library(config), config.get("library")
    )
    localized_config.result()

    aligned_bams = []
    for sample, tag, path in zip(bams["sample"], bams["tag"], bams["path"]):
        if len(aligned_bams) > config["max_concurrent_samples"]:
            [b.result() for b in aligned_bams]
            aligned_bams = []
        # while True:
        #     print(aligned_bams)
        #     concurrent_samples = len([b for b in aligned_bams if not b.done()])
        #     if concurrent_samples < config["max_concurrent_samples"]:
        #         break
        #     else:
        #         time.sleep(config["poll_interval"])
        print("starting bam to ubam for sample {}".format(sample))
        path = path.strip()
        if not os.path.isabs(path):
            path = os.path.join(config["project_dir"], path)
        aligned_bams.append(
            apps.align(
                apps.bam_to_ubam(path, sample, tag, config),
                config,
                localized_config,
                sample,
                tag,
                clean_inputs=True,
            )
        )

    for _, row in fastqs.iterrows():
        if len(aligned_bams) > config["max_concurrent_samples"]:
            [b.result() for b in aligned_bams]
            aligned_bams = []
        # while True:
        #     print(aligned_bams)
        #     concurrent_samples = len([b for b in aligned_bams if not b.done()])
        #     if concurrent_samples < config["max_concurrent_samples"]:
        #         break
        #     else:
        #         time.sleep(config["poll_interval"])
        path = row["path"].strip()
        if not os.path.isabs(path):
            path = os.path.join(config["project_dir"], path)
        left_fastqs = []
        right_fastqs = []
        left_wildcard_path = os.path.join(
            path, "*" + row["left_wildcard"].strip() + "*"
        )
        for f in glob.glob(left_wildcard_path):
            if os.path.isfile(
                f.replace(row["left_wildcard"], row["right_wildcard"])
            ):  # make sure we have paired files
                left_fastqs += [f]
                right_fastqs += [f.replace(row["left_wildcard"], row["right_wildcard"])]
            else:
                print("no paired fastq found for {}".format(f))
        if len(glob.glob(left_wildcard_path)) == 0:
            print("no files found matching {}".format(left_wildcard_path))
        elif len(left_fastqs) == 0:
            print("no paired fastqs found in {}".format(path))
        ubams = []
        for left_fastq, right_fastq in zip(left_fastqs, right_fastqs):
            print(
                "starting fastq to ubam for {} and {}".format(left_fastq, right_fastq)
            )
            ubams.append(
                apps.fastq_to_ubam(
                    left_fastq,
                    right_fastq,
                    row["sample"],
                    row["tag"],
                    row["path"],
                    row["sequencing_center"],
                    row["run_date"],
                    config,
                )
            )
        aligned_bams.append(
            apps.align(
                apps.make_ubam_list(config, row["sample"], row["tag"], *ubams),
                config,
                localized_config,
                row["sample"],
                row["tag"],
                clean_inputs=True,
            )
        )
    parsl.wait_for_current_tasks()
    print("workflow completed!")
