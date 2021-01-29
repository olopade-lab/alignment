from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            worker_debug=True,
            max_workers=10,
            provider=SlurmProvider(
                'daenerys',
                init_blocks=1,
                max_blocks=1,
                worker_init='source ~/.profile; source ~/.bashrc; conda activate alignment',
                walltime='2400000:00:00'
            ),
        )
    ],
)
