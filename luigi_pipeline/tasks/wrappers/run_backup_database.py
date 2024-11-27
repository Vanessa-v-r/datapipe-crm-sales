# tasks/wrappers/run_backup_database.py

import luigi
from luigi_pipeline.tasks.backup_database import BackupDatabase

class RunBackupDatabase(luigi.WrapperTask):
    def requires(self):
        yield BackupDatabase(force_once=True)
