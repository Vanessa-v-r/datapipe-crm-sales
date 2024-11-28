# tasks/logging_config.py

import logging
import os
import sys
import structlog
from luigi_pipeline.configs.settings import LOGS_DIR

def initialize_logging():
    """
    Configures structured logging using structlog.
    Logs are written in JSON format for easy parsing and analysis.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)

    # Configure standard logging to capture warnings and above from structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    # Configure structlog for structured logging
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Add file handler for structured logs
    structured_log_path = os.path.join(LOGS_DIR, 'structured_pipeline.log')
    file_handler = logging.FileHandler(structured_log_path)
    file_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(file_handler)

    # Add console handler for real-time monitoring
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)
