# tasks/apply_replacements_post.py

import luigi
import os
import json
import pandas as pd
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.configs.settings import (
    NORMALIZED_DATA_DIR,
    REPLACED_DATA_DIR,
    REPLACEMENTS_DIR,
    REPLACEMENT_LISTS_EXCEL_PATH
)
from luigi_pipeline.tasks.normalize_data import NormalizeData
from luigi_pipeline.utils.normalization_utils import apply_replacements_to_entry
from luigi_pipeline.utils.logging_decorator import log_function

class ApplyReplacementsPostNormalization(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force ApplyReplacementsPostNormalization to run once"
    )

    def requires(self):
        # Depends on NormalizeData
        return NormalizeData(force_once=False)

    def output(self):
        outputs = []
        for input_target in self.input():
            # Output path for post-normalization replacements
            output_filename = os.path.basename(input_target.path).replace('normalized', 'replaced')
            output_path = os.path.join(REPLACED_DATA_DIR, output_filename)
            outputs.append(luigi.LocalTarget(output_path))
        return outputs

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting ApplyReplacementsPostNormalization task.")
        try:
            os.makedirs(REPLACED_DATA_DIR, exist_ok=True)
            os.makedirs(REPLACEMENTS_DIR, exist_ok=True)

            # Verify that the replacement lists file exists
            if not os.path.exists(REPLACEMENT_LISTS_EXCEL_PATH):
                error_msg = f"Replacement lists Excel file not found at {REPLACEMENT_LISTS_EXCEL_PATH}"
                self.logger.error(event="file_not_found", message=error_msg)
                raise FileNotFoundError(error_msg)

            # Load replacement lists from Excel
            company_replacements = pd.read_excel(
                REPLACEMENT_LISTS_EXCEL_PATH,
                sheet_name='company_replacement_list'
            )
            contact_replacements = pd.read_excel(
                REPLACEMENT_LISTS_EXCEL_PATH,
                sheet_name='contact_replacement_list'
            )
            region_replacements = pd.read_excel(
                REPLACEMENT_LISTS_EXCEL_PATH,
                sheet_name='region_replacement_list'
            )

            self.logger.debug(event="load_replacements", message="Loaded replacement lists from Excel.")

            # Save replacements as CSV for later use
            company_replacements.to_csv(
                os.path.join(REPLACEMENTS_DIR, 'company_replacement_list.csv'),
                index=False
            )
            contact_replacements.to_csv(
                os.path.join(REPLACEMENTS_DIR, 'contact_replacement_list.csv'),
                index=False
            )
            region_replacements.to_csv(
                os.path.join(REPLACEMENTS_DIR, 'region_replacement_list.csv'),
                index=False
            )

            self.logger.debug(event="save_replacements", message="Saved replacement lists as CSV.")

            # Convert replacements to dictionaries
            company_replacements_dict = dict(
                zip(
                    company_replacements['old_company_name'],
                    company_replacements['new_company_name']
                )
            )
            contact_replacements_dict = {}
            for _, row in contact_replacements.iterrows():
                key = (row['old_company_name'], row['old_contact_name'])
                contact_replacements_dict[key] = {
                    'new_company_name': row['new_company_name'],
                    'new_contact_name': row['new_contact_name'],
                    'contact_phone': row['contact_phone'],
                    'contact_email': row['contact_email']
                }
            region_replacements_dict = {}
            for _, row in region_replacements.iterrows():
                region_replacements_dict[row['region_value']] = {
                    'company_city': row['company_city'],
                    'company_state': row['company_state'],
                    'company_country': row['company_country'],
                    'company_region': row['company_region']
                }

            self.logger.debug(event="convert_replacements", message="Converted replacement lists to dictionaries.")

            # Apply replacements
            for input_target, output_target in zip(self.input(), self.output()):
                self.logger.info(event="process_file", message=f"Processing file {input_target.path}.")
                with open(input_target.path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                replaced_data = []
                for entry in data:
                    updated_entry = apply_replacements_to_entry(
                        entry,
                        company_replacements_dict,
                        contact_replacements_dict,
                        region_replacements_dict,
                        self.logger
                    )
                    replaced_data.append(updated_entry)
                with open(output_target.path, 'w', encoding='utf-8') as f_out:
                    json.dump(replaced_data, f_out, ensure_ascii=False, indent=4)
                self.logger.info(event="save_replaced_data", message=f"Applied replacements and saved to {output_target.path}.")
            self.logger.info(event="end_task", message="Completed ApplyReplacementsPostNormalization task.")
        except Exception as e:
            self.logger.error(
                event="task_exception",
                message=f"Error in ApplyReplacementsPostNormalization task: {str(e)}",
                exception=str(e)
            )
            raise
