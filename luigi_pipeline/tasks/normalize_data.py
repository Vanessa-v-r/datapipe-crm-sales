# tasks/normalize_data.py

import luigi
import os
import json
from luigi_pipeline.tasks.base import BaseTask
from luigi_pipeline.configs.settings import REPLACED_DATA_DIR, NORMALIZED_DATA_DIR
from luigi_pipeline.tasks.apply_replacements_pre import ApplyReplacementsPreNormalization
from luigi_pipeline.utils.normalization_utils import normalize_entry
from luigi_pipeline.utils.logging_decorator import log_function

class NormalizeData(BaseTask):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force NormalizeData to run once"
    )

    def requires(self):
        # Now depends on the pre-normalization replacements
        return ApplyReplacementsPreNormalization(force_once=False)

    def output(self):
        outputs = []
        for input_target in self.input():
            # Adjust the output path to reflect the new input
            output_path = os.path.join(NORMALIZED_DATA_DIR, os.path.basename(input_target.path).replace('replaced_pre', 'normalized'))
            outputs.append(luigi.LocalTarget(output_path))
        return outputs

    @log_function
    def run_task(self):
        self.logger.info(event="start_task", message="Starting NormalizeData task.")
        try:
            os.makedirs(NORMALIZED_DATA_DIR, exist_ok=True)
            for input_target, output_target in zip(self.input(), self.output()):
                self.logger.info(event="process_file", message=f"Processing input file: {input_target.path}.")
                try:
                    with open(input_target.path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.logger.debug(event="load_data", message=f"Loaded data from {input_target.path}. Number of entries: {len(data)}.")
                except json.JSONDecodeError as e:
                    self.logger.error(event="json_error", message=f"JSON decode error for {input_target.path}: {str(e)}.")
                    continue

                normalized_data = []
                for entry in data:
                    normalized_entry = normalize_entry(entry, self.logger)
                    normalized_data.append(normalized_entry)

                try:
                    with open(output_target.path, 'w', encoding='utf-8') as f_out:
                        json.dump(normalized_data, f_out, ensure_ascii=False, indent=4)
                    self.logger.info(event="save_normalized_data", message=f'Normalized data saved to {output_target.path}.')
                except Exception as e:
                    self.logger.error(event="write_error", message=f"Error writing normalized data to {output_target.path}: {str(e)}.")
            self.logger.info(event="end_task", message="Completed NormalizeData task.")
        except Exception as e:
            self.logger.error(event="task_exception", message=f"Error in NormalizeData task: {str(e)}", exception=str(e))
            raise
