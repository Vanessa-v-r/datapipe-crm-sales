# tasks/wrappers/run_apply_replacements_post.py

import luigi
from luigi_pipeline.tasks.apply_replacements_post import ApplyReplacementsPostNormalization

class RunApplyReplacementsPostNormalization(luigi.WrapperTask):
    def requires(self):
        return ApplyReplacementsPostNormalization(force_once=False)
