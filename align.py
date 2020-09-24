import argparse
import pandas as pd
import glob
import os
import subprocess
import json
import importlib

import parsl
from parsl import python_app, bash_app
from parsl.config import Config
from parsl.utils import get_all_checkpoints
from parsl.executors.threads import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("config", help="")
args = parser.parse_args()

from apps import align, bam_to_ubam, prepare_ref_auxiliary_files, localize_library

with open(args.config) as f:
    config = json.load(f)

spec = importlib.util.spec_from_file_location('', config['parsl_config'])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
parsl.load(module.config)
# parsl.load(
#     Config(
#         executors=[ThreadPoolExecutor(max_threads=10)],
#         checkpoint_mode='task_exit', checkpoint_files=get_all_checkpoints()
#     )
# )

# temporary hack
exec(open('apps.py').read())

files = pd.read_csv(config['samples'], names=['sample', 'path'], comment='#')
library = os.path.join(config['project_dir'], 'library')

localized_config = prepare_ref_auxiliary_files(localize_library(config), library)

bams = []
executable = glob.glob(os.path.join(config['source_dir'], 'cromwell*jar'))[-1]
for sample, path in zip(files['sample'], files['path']):
    sample = sample.replace('/', '#')
    print('starting sample {}; path is {}'.format(sample, path))
    bams.append(
        align(
            bam_to_ubam(path, sample, config),
            config,
            localized_config,
            sample,
            clean_inputs=True
        )
    )

parsl.wait_for_current_tasks()
print('workflow completed')
