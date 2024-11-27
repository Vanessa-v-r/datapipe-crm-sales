# tasks/backup_database.py

import luigi
import os
import shutil
from datetime import datetime, timedelta
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.configs.settings import DATABASE_PATH, BACKUPS_DIR
from luigi_pipeline.tasks.load_to_database import LoadToDatabase
from luigi_pipeline.utils.logging_decorator import log_function

class BackupDatabase(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force BackupDatabase to run once"
    )

    def requires(self):
        return LoadToDatabase(force_once=False)  # Not forced dependency

    def output(self):
        date_str = datetime.now().strftime('%Y%m%d')
        backup_path = os.path.join(BACKUPS_DIR, f'crm_database_{date_str}.db')
        return luigi.LocalTarget(backup_path)

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting BackupDatabase task.")
        try:
            os.makedirs(BACKUPS_DIR, exist_ok=True)
            shutil.copy(DATABASE_PATH, self.output().path)
            self.logger.info(event="backup_created", message=f"Database backed up to {self.output().path}.")

            # Remove backups older than one month
            cutoff_date = datetime.now() - timedelta(days=30)
            for filename in os.listdir(BACKUPS_DIR):
                if filename.startswith('crm_database_') and filename.endswith('.db'):
                    date_part = filename.replace('crm_database_', '').replace('.db', '')
                    try:
                        file_date = datetime.strptime(date_part, '%Y%m%d')
                        if file_date < cutoff_date:
                            os.remove(os.path.join(BACKUPS_DIR, filename))
                            self.logger.info(event="backup_removed", message=f"Removed old backup: {filename}.")
                    except ValueError:
                        self.logger.warning(event="unexpected_filename_format", message=f"Unexpected backup filename format: {filename}.")
            self.logger.info(event="end_task", message="Completed BackupDatabase task.")
        except Exception as e:
            self.logger.error(event="task_exception", message=f"Error in BackupDatabase task: {str(e)}", exception=str(e))
            raise
