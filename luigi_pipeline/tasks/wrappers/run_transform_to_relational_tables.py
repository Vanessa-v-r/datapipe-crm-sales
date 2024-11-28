# tasks/wrappers/run_transform_to_relational_tables.py

import luigi
from luigi_pipeline.tasks.transform_to_relational_tables import TransformToRelationalTables

class RunTransformToRelationalTables(luigi.WrapperTask):
    def requires(self):
        yield TransformToRelationalTables(force_once=True)
