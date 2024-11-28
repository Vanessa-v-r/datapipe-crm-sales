# tasks/extract_raw_data.py

import os
import json
import luigi
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.utils.notion_client import NotionClient
from luigi_pipeline.configs.settings import RAW_DATA_DIR, SELECTED_DATABASES
from luigi_pipeline.utils.logging_decorator import log_function

class ExtractRawData(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force ExtractRawData to run once"
    )

    def requires(self):
        return []  # No dependencies

    def output(self):
        outputs = []
        for db_id, db_name in SELECTED_DATABASES.items():
            output_path = os.path.join(RAW_DATA_DIR, f'db_{db_name}_data.json')
            schema_output_path = os.path.join(RAW_DATA_DIR, f'db_{db_name}_schema.json')
            outputs.extend([luigi.LocalTarget(output_path), luigi.LocalTarget(schema_output_path)])
        return outputs

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting ExtractRawData task.")
        try:
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            client = NotionClient(logger=self.logger)  # Pass the task's logger

            for db_id, db_name in SELECTED_DATABASES.items():
                # Fetch and save database data
                self.logger.info(event="fetch_database", message=f"Fetching data for database '{db_name}'.")
                data = client.fetch_database(db_id)
                data_output_path = os.path.join(RAW_DATA_DIR, f'db_{db_name}_data.json')
                with open(data_output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                self.logger.info(event="save_data", message=f"Fetched data for '{db_name}' and saved to {data_output_path}.")

                # Fetch and save schema information
                if data['results']:
                    schema_info = {name: prop['type'] for name, prop in data['results'][0]['properties'].items()}
                else:
                    schema_info = {}
                    self.logger.warning(event="no_results", message=f"No results found for database '{db_name}' to extract schema.")
                schema_output_path = os.path.join(RAW_DATA_DIR, f'db_{db_name}_schema.json')
                with open(schema_output_path, 'w', encoding='utf-8') as schema_file:
                    json.dump(schema_info, schema_file, ensure_ascii=False, indent=4)
                self.logger.info(event="save_schema", message=f"Saved schema for '{db_name}' to {schema_output_path}.")
            self.logger.info(event="end_task", message="Completed ExtractRawData task.")
        except Exception as e:
            self.logger.error(event="task_exception", message=f"Error in ExtractRawData task: {str(e)}", exception=str(e))
            raise
