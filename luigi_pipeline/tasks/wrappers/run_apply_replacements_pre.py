# tasks/wrappers/run_apply_replacements_pre.py

import luigi
from luigi_pipeline.tasks.apply_replacements_pre import ApplyReplacementsPreNormalization

class RunApplyReplacementsPreNormalization(luigi.WrapperTask):
    def requires(self):
        return ApplyReplacementsPreNormalization(force_once=False)
