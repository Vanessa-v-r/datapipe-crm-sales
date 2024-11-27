# tasks/wrappers/run_normalize_data.py

import luigi
from luigi_pipeline.tasks.normalize_data import NormalizeData

class RunNormalizeData(luigi.WrapperTask):
    def requires(self):
        yield NormalizeData(force_once=True)
