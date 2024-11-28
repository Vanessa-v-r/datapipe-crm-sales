# tasks/wrappers/run_extract_selected_properties.py

import luigi
from luigi_pipeline.tasks.extract_selected_properties import ExtractSelectedProperties

class RunExtractSelectedProperties(luigi.WrapperTask):
    def requires(self):
        yield ExtractSelectedProperties(force_once=True)
