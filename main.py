# main.py

import luigi
from luigi_pipeline.pipeline import RunETLPipeline
from luigi_pipeline.tasks.logging_config import initialize_logging  # Updated import

if __name__ == '__main__':
    initialize_logging()
    luigi.run(['RunETLPipeline', '--local-scheduler'])
