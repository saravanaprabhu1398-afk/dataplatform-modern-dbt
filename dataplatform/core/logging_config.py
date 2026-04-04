import logging
import logging.config
import json
import os
from datetime import datetime
from typing import Dict, Any


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields if present
        if hasattr(record, 'extra_data'):
            log_entry.update(record.extra_data)

        return json.dumps(log_entry)


def setup_logging(log_level: str = "INFO", json_format: bool = False, log_file: str = "logs/pipeline.log"):
    """Set up enhanced logging configuration."""

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Convert log level string to logging level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Base configuration
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': numeric_level,
                'formatter': 'standard' if not json_format else 'json',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.FileHandler',
                'level': numeric_level,
                'formatter': 'detailed' if not json_format else 'json',
                'filename': log_file,
                'encoding': 'utf-8'
            }
        },
        'loggers': {
            'dataplatform': {
                'level': numeric_level,
                'handlers': ['console', 'file'],
                'propagate': False
            },
            'dataplatform.core': {
                'level': numeric_level,
                'handlers': ['console', 'file'],
                'propagate': False
            },
            'dataplatform.plugins': {
                'level': numeric_level,
                'handlers': ['console', 'file'],
                'propagate': False
            }
        },
        'root': {
            'level': 'WARNING',
            'handlers': ['console']
        }
    }

    # Add JSON formatter if requested
    if json_format:
        config['formatters']['json'] = {
            '()': StructuredFormatter
        }

    logging.config.dictConfig(config)

    # Get logger
    logger = logging.getLogger('dataplatform')
    logger.info("Logging initialized", extra={'extra_data': {'log_level': log_level, 'json_format': json_format}})

    return logger


def log_task_start(task_name: str, attempt: int = 1) -> None:
    """Log task execution start."""
    logger = logging.getLogger('dataplatform.core.executor')
    logger.info(f"Starting task execution: {task_name}",
                extra={'extra_data': {'task': task_name, 'attempt': attempt, 'event': 'task_start'}})


def log_task_success(task_name: str, duration: float = None) -> None:
    """Log task execution success."""
    logger = logging.getLogger('dataplatform.core.executor')
    extra_data = {'task': task_name, 'event': 'task_success'}
    if duration:
        extra_data['duration_seconds'] = round(duration, 2)
    logger.info(f"Task completed successfully: {task_name}",
                extra={'extra_data': extra_data})


def log_task_failure(task_name: str, error: str, attempt: int, max_retries: int) -> None:
    """Log task execution failure."""
    logger = logging.getLogger('dataplatform.core.executor')
    logger.warning(f"Task failed: {task_name} - {error}",
                   extra={'extra_data': {'task': task_name, 'error': error, 'attempt': attempt,
                                       'max_retries': max_retries, 'event': 'task_failure'}})


def log_pipeline_start(pipeline_name: str, task_count: int) -> None:
    """Log pipeline execution start."""
    logger = logging.getLogger('dataplatform')
    logger.info(f"Starting pipeline execution: {pipeline_name}",
                extra={'extra_data': {'pipeline': pipeline_name, 'task_count': task_count, 'event': 'pipeline_start'}})


def log_pipeline_success(pipeline_name: str, duration: float) -> None:
    """Log pipeline execution success."""
    logger = logging.getLogger('dataplatform')
    logger.info(f"Pipeline completed successfully: {pipeline_name}",
                extra={'extra_data': {'pipeline': pipeline_name, 'duration_seconds': round(duration, 2),
                                    'event': 'pipeline_success'}})


def log_pipeline_failure(pipeline_name: str, failed_task: str, duration: float) -> None:
    """Log pipeline execution failure."""
    logger = logging.getLogger('dataplatform')
    logger.error(f"Pipeline failed: {pipeline_name} at task {failed_task}",
                 extra={'extra_data': {'pipeline': pipeline_name, 'failed_task': failed_task,
                                     'duration_seconds': round(duration, 2), 'event': 'pipeline_failure'}})