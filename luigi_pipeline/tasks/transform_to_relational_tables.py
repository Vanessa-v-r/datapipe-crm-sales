# tasks/transform_to_relational_tables.py

import luigi
import os
import json
import pandas as pd
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.configs.settings import REPLACED_DATA_DIR, TRANSFORMED_DATA_DIR
from luigi_pipeline.tasks.apply_replacements_post import ApplyReplacementsPostNormalization
from luigi_pipeline.utils.data_utils import transform_data_to_relational_tables
from luigi_pipeline.utils.logging_decorator import log_function

class TransformToRelationalTables(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force TransformToRelationalTables to run once"
    )

    def requires(self):
        # Now depends on the post-normalization replacements
        return ApplyReplacementsPostNormalization(force_once=False)

    def output(self):
        return [
            luigi.LocalTarget(os.path.join(TRANSFORMED_DATA_DIR, 'company_table.csv')),
            luigi.LocalTarget(os.path.join(TRANSFORMED_DATA_DIR, 'contact_table.csv')),
            luigi.LocalTarget(os.path.join(TRANSFORMED_DATA_DIR, 'deals_table.csv')),
        ]

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting TransformToRelationalTables task.")
        try:
            os.makedirs(TRANSFORMED_DATA_DIR, exist_ok=True)
            company_records = []
            contact_records = []
            deal_records = []

            for input_target in self.input():
                self.logger.info(event="process_file", message=f"Processing input file: {input_target.path}.")
                try:
                    with open(input_target.path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.logger.debug(event="load_data", message=f"Loaded data from {input_target.path}. Number of entries: {len(data)}.")
                except json.JSONDecodeError as e:
                    self.logger.error(event="json_error", message=f"JSON decode error for {input_target.path}: {str(e)}.")
                    continue

                comp_recs, cont_recs, deal_recs = transform_data_to_relational_tables(data)
                company_records.extend(comp_recs)
                contact_records.extend(cont_recs)
                deal_records.extend(deal_recs)

            # Convert to DataFrames and save as CSV
            self.logger.info(event="convert_to_dataframe", message="Converting records to DataFrames.")
            company_df = pd.DataFrame(company_records).drop_duplicates(subset=['company_name_new'])
            contact_df = pd.DataFrame(contact_records).drop_duplicates(subset=['contact_name_new', 'company_name_new'])
            deal_df = pd.DataFrame(deal_records).drop_duplicates(subset=['page_id'])

            self.logger.info(event="write_csv", message="Writing DataFrames to CSV files.")
            company_df.to_csv(self.output()[0].path, index=False)
            contact_df.to_csv(self.output()[1].path, index=False)
            deal_df.to_csv(self.output()[2].path, index=False)

            self.logger.info(event="save_relational_tables", message=f'Transformed data saved to relational tables in {TRANSFORMED_DATA_DIR}.')
            self.logger.info(event="end_task", message="Completed TransformToRelationalTables task.")
        except Exception as e:
            self.logger.error(event="task_exception", message=f"Error in TransformToRelationalTables task: {str(e)}", exception=str(e))
            raise
