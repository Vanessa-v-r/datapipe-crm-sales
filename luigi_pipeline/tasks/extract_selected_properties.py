# tasks/extract_selected_properties.py

import luigi
import os
import json
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.configs.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, SELECTED_DATABASES
from luigi_pipeline.configs.column_mappings import COLUMN_MAPPINGS
from luigi_pipeline.tasks.extract_raw_data import ExtractRawData
from luigi_pipeline.utils.logging_decorator import log_function

class ExtractSelectedProperties(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force ExtractSelectedProperties to run once"
    )

    def requires(self):
        return ExtractRawData(force_once=False)  # Not forced dependency

    def output(self):
        outputs = []
        for db_name in COLUMN_MAPPINGS.keys():
            output_path = os.path.join(PROCESSED_DATA_DIR, f'processed_{db_name}_data.json')
            outputs.append(luigi.LocalTarget(output_path))
        return outputs

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting ExtractSelectedProperties task.")
        try:
            os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

            for db_id, db_name in SELECTED_DATABASES.items():
                if db_name not in COLUMN_MAPPINGS:
                    self.logger.warning(event="missing_mapping", message=f"No column mappings found for database '{db_name}'. Skipping.")
                    continue

                # Define data and schema paths
                data_path = os.path.join(RAW_DATA_DIR, f'db_{db_name}_data.json')
                schema_path = os.path.join(RAW_DATA_DIR, f'db_{db_name}_schema.json')
                output_path = os.path.join(PROCESSED_DATA_DIR, f'processed_{db_name}_data.json')

                # Check file existence
                data_target = next((x for x in self.input() if x.path == data_path), None)
                schema_target = next((x for x in self.input() if x.path == schema_path), None)

                if not data_target:
                    self.logger.error(event="missing_data_file", message=f"Data file for '{db_name}' not found at '{data_path}'. Skipping.")
                    continue
                if not schema_target:
                    self.logger.error(event="missing_schema_file", message=f"Schema file for '{db_name}' not found at '{schema_path}'. Skipping.")
                    continue

                self.logger.info(event="processing_database", message=f"Processing database '{db_name}'.")

                # Read data
                try:
                    with open(data_target.path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.logger.debug(event="load_data", message=f"Loaded data for '{db_name}'. Number of entries: {len(data.get('results', []))}.")
                except json.JSONDecodeError as e:
                    self.logger.error(event="json_error", message=f"JSON decode error for '{db_name}': {str(e)}.")
                    continue

                # Process data
                processed_data = []
                mappings = COLUMN_MAPPINGS[db_name]
                self.logger.debug(event="process_mappings", message=f"Processing entries with mappings: {mappings}.")

                for entry in data.get('results', []):
                    processed_entry = self.process_entry(entry, mappings)
                    processed_data.append(processed_entry)

                # Write processed data
                try:
                    with open(output_path, 'w', encoding='utf-8') as f_out:
                        json.dump(processed_data, f_out, ensure_ascii=False, indent=4)
                    self.logger.info(event="save_output", message=f"Extracted selected properties for '{db_name}' and saved to {output_path}.")
                except Exception as e:
                    self.logger.error(event="write_error", message=f"Error writing processed data for '{db_name}': {str(e)}.")
            self.logger.info(event="end_task", message="Completed ExtractSelectedProperties task.")
        except Exception as e:
            self.logger.error(event="task_exception", message=f"Error in ExtractSelectedProperties task: {str(e)}", exception=str(e))
            raise

    def process_entry(self, entry, mappings):
        """
        Processes a single entry by extracting and mapping selected properties.
        """
        entry_id = entry.get('id')
        self.logger.debug(event="process_entry", message=f"Processing entry ID: {entry_id}.")

        processed_entry = {
            'page_id': entry.get('id'),
            'last_edit_time': entry.get('last_edited_time'),
            'source_db': entry.get('source_db')
        }
        properties = entry.get('properties', {})

        for key, value in mappings.items():
            if key in properties:
                property_data = properties.get(key)

                if property_data is None:
                    self.logger.warning(event="missing_property", message=f"Property '{key}' is None for entry ID: {entry_id}. Assigning default value.")
                    processed_entry[value] = ''  # Assign default value
                    continue

                property_type = property_data.get('type')
                processed_entry[value] = self.extract_property_value(property_data, property_type, key, entry_id)
            else:
                self.logger.warning(event="missing_property_key", message=f"Property '{key}' not found in entry ID: {entry_id}. Assigning default value.")
                processed_entry[value] = ''
        return processed_entry

    def extract_property_value(self, property_data, property_type, key, entry_id):
        """
        Extracts the value of a property based on its type.
        """
        try:
            if property_type == 'title':
                text = ' '.join([t.get('plain_text', '') for t in property_data.get('title', [])])
                return text.strip()
            elif property_type == 'rich_text':
                text = ' '.join([t.get('plain_text', '') for t in property_data.get('rich_text', [])])
                return text.strip()
            elif property_type == 'select':
                select_info = property_data.get('select')
                return select_info.get('name', '').strip() if select_info else ''
            elif property_type == 'multi_select':
                multi_select_info = property_data.get('multi_select')
                return [ms.get('name', '').strip() for ms in multi_select_info if isinstance(ms, dict)] if multi_select_info else []
            elif property_type == 'number':
                return property_data.get('number', '')
            elif property_type == 'email':
                return property_data.get('email', '')
            elif property_type == 'phone_number':
                return property_data.get('phone_number', '')
            elif property_type == 'date':
                date_info = property_data.get('date')
                return date_info.get('start', '').strip() if date_info else ''
            elif property_type == 'url':
                return property_data.get('url', '').strip() if property_data.get('url') else ''
            elif property_type == 'formula':
                formula = property_data.get('formula', {})
                formula_type = formula.get('type')
                if formula_type == 'number':
                    return formula.get('number', '')
                elif formula_type == 'string':
                    return formula.get('string', '')
                else:
                    self.logger.warning(event="unhandled_formula_type", message=f"Unhandled formula type '{formula_type}' for key '{key}' in entry ID: {entry_id}.")
                    return ''
            elif property_type == 'status':
                status_info = property_data.get('status')
                return status_info.get('name', '').strip() if status_info else ''
            else:
                self.logger.warning(event="unhandled_property_type", message=f"Unhandled property type '{property_type}' for key '{key}' in entry ID: {entry_id}.")
                return ''
        except Exception as e:
            self.logger.error(event="property_extraction_error", message=f"Error extracting property '{key}' for entry ID: {entry_id}: {str(e)}.", exception=str(e))
            return ''
