# tasks/load_to_database.py

import luigi
import os
import sqlite3
import pandas as pd
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.configs.settings import DATABASE_PATH, TRANSFORMED_DATA_DIR
from luigi_pipeline.tasks.transform_to_relational_tables import TransformToRelationalTables
from luigi_pipeline.utils.logging_decorator import log_function

class LoadToDatabase(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force LoadToDatabase to run once"
    )

    def requires(self):
        return TransformToRelationalTables(force_once=False)  # Not forced dependency

    def output(self):
        return luigi.LocalTarget(DATABASE_PATH)

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting LoadToDatabase task.")
        try:
            os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
            conn = sqlite3.connect(DATABASE_PATH)

            # Read CSV files
            self.logger.info(event="read_csv", message="Reading transformed CSV files.")
            company_df = pd.read_csv(self.input()[0].path)
            contact_df = pd.read_csv(self.input()[1].path)
            deal_df = pd.read_csv(self.input()[2].path)
            self.logger.debug(event="read_csv_complete", message="Completed reading CSV files.")

            # Load existing data for incremental updates
            existing_tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            existing_tables = [table[0] for table in existing_tables]

            if 'company_table' in existing_tables:
                existing_company_df = pd.read_sql('SELECT * FROM company_table', conn)
                company_df = pd.concat([existing_company_df, company_df]).drop_duplicates(subset=['company_name_new'], keep='last')
                self.logger.info(event="update_table", message="Updated company_table with new data.")
            if 'contact_table' in existing_tables:
                existing_contact_df = pd.read_sql('SELECT * FROM contact_table', conn)
                contact_df = pd.concat([existing_contact_df, contact_df]).drop_duplicates(subset=['contact_name_new', 'company_name_new'], keep='last')
                self.logger.info(event="update_table", message="Updated contact_table with new data.")
            if 'deals_table' in existing_tables:
                existing_deal_df = pd.read_sql('SELECT * FROM deals_table', conn)
                deal_df = pd.concat([existing_deal_df, deal_df]).drop_duplicates(subset=['page_id'], keep='last')
                self.logger.info(event="update_table", message="Updated deals_table with new data.")

            # Write to database
            self.logger.info(event="write_database", message="Writing data to SQLite database.")
            company_df.to_sql('company_table', conn, if_exists='replace', index=False)
            contact_df.to_sql('contact_table', conn, if_exists='replace', index=False)
            deal_df.to_sql('deals_table', conn, if_exists='replace', index=False)

            conn.close()
            self.logger.info(event="database_loaded", message=f'Data loaded into database at {DATABASE_PATH}.')
            self.logger.info(event="end_task", message="Completed LoadToDatabase task.")
        except Exception as e:
            self.logger.error(event="task_exception", message=f"Error in LoadToDatabase task: {str(e)}", exception=str(e))
            raise
