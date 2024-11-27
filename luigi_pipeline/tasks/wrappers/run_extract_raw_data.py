# tasks/wrappers/run_extract_raw_data.py

import luigi
from luigi_pipeline.tasks.extract_raw_data import ExtractRawData

class RunExtractRawData(luigi.WrapperTask):
    def requires(self):
        yield ExtractRawData(force_once=True)
