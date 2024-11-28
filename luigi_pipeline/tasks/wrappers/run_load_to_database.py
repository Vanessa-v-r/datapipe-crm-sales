# tasks/wrappers/run_load_to_database.py

import luigi
from luigi_pipeline.tasks.load_to_database import LoadToDatabase

class RunLoadToDatabase(luigi.WrapperTask):
    def requires(self):
        yield LoadToDatabase(force_once=True)
