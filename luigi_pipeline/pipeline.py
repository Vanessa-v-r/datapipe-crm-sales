# pipeline.py

import luigi
from luigi_pipeline.tasks.logging_config import initialize_logging  # Ensure this is first
from luigi_pipeline.tasks.wrappers.run_extract_raw_data import RunExtractRawData
from luigi_pipeline.tasks.wrappers.run_extract_selected_properties import RunExtractSelectedProperties
from luigi_pipeline.tasks.wrappers.run_apply_replacements_pre import RunApplyReplacementsPreNormalization
from luigi_pipeline.tasks.wrappers.run_normalize_data import RunNormalizeData
from luigi_pipeline.tasks.wrappers.run_apply_replacements_post import RunApplyReplacementsPostNormalization
from luigi_pipeline.tasks.wrappers.run_transform_to_relational_tables import RunTransformToRelationalTables
from luigi_pipeline.tasks.wrappers.run_load_to_database import RunLoadToDatabase
from luigi_pipeline.tasks.wrappers.run_backup_database import RunBackupDatabase

# Initialize structured logging before executing any tasks
initialize_logging()

class RunETLPipeline(luigi.WrapperTask):
    def requires(self):
        # Sequentially yield each task to ensure they run in order
        yield RunExtractRawData()
        yield RunExtractSelectedProperties()
        yield RunApplyReplacementsPreNormalization()  # First application of replacements
        yield RunNormalizeData()
        yield RunApplyReplacementsPostNormalization()  # Second application of replacements
        yield RunTransformToRelationalTables()
        yield RunLoadToDatabase()
        yield RunBackupDatabase()

if __name__ == "__main__":
    luigi.run()
