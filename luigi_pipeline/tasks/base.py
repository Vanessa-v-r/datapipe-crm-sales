# tasks/base.py

import luigi
import os
import logging
from luigi_pipeline.configs.settings import LOGS_DIR
import structlog

class BaseTask(luigi.Task):
    force_once = luigi.BoolParameter(
        default=False,
        description="Force task to run once if not already executed"
    )

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)
        self.setup_task_logging()

    def setup_task_logging(self):
        """
        Configures a structured logger specific to the task.
        """
        task_name = self.__class__.__name__
        self.logger = structlog.get_logger(task_name)
        
        # Create task-specific log file if not already created
        task_log_path = os.path.join(LOGS_DIR, f'{task_name}.log')
        if not hasattr(self, '_task_log_handler_added'):
            if not os.path.exists(task_log_path):
                open(task_log_path, 'a').close()  # Create the file if it doesn't exist
            
            file_handler = logging.FileHandler(task_log_path)
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(message)s')
            file_handler.setFormatter(formatter)
            # Prevent adding multiple handlers
            if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(task_log_path) for h in logging.getLogger().handlers):
                logging.getLogger().addHandler(file_handler)
            self.logger = self.logger.bind(task=task_name)
            self._task_log_handler_added = True  # Mark as handler added

    def complete(self):
        marker_path = self.get_marker_path()
        if self.force_once:
            if os.path.exists(marker_path):
                self.logger.info(event="skip_execution", message=f"Task {self.__class__.__name__} already completed once. Skipping execution.")
                return super().complete()
            else:
                return False  # Needs to run with force_once=True
        else:
            return super().complete()

    def run(self):
        if self.force_once:
            self.logger.info(event="start_task", message="Executing with force_once=True")
            try:
                self.run_task()
                self.create_marker()
                self.logger.info(event="end_task", message="Task completed successfully.")
            except Exception as e:
                self.logger.error(event="error", message=f"Error during task execution: {str(e)}", exception=str(e))
                raise
        else:
            self.logger.info(event="start_task", message="Executing with force_once=False")
            try:
                self.run_task()
                self.logger.info(event="end_task", message="Task completed successfully.")
            except Exception as e:
                self.logger.error(event="error", message=f"Error during task execution: {str(e)}", exception=str(e))
                raise

    def run_task(self):
        """Method to be implemented by each specific task."""
        raise NotImplementedError

    def get_marker_path(self):
        """Defines the path for the marker file."""
        marker_dir = os.path.join(os.path.dirname(__file__), '..', 'markers')
        os.makedirs(marker_dir, exist_ok=True)
        return os.path.join(marker_dir, f"{self.__class__.__name__}.marker")

    def create_marker(self):
        """Creates a marker file indicating task completion."""
        marker_path = self.get_marker_path()
        with open(marker_path, 'w') as marker_file:
            marker_file.write('completed')
        self.logger.info(event="create_marker", message="Marker created.")
