import logging
import datetime
from pathlib import Path

from src.workers.worker_tools import create_folder


def setup_logger(log_filepath):
    path = Path(log_filepath).parent
    create_folder(path)

    logger = logging.getLogger('AppLogger')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(log_filepath)
    logger.addHandler(handler)
    return logger

def write_log_message(message, calling_file, logger):
    now = datetime.datetime.now().strftime('%m/%d/%Y %I:%M %p')
    file_name = Path(calling_file).name
    logger.info(f'[{now}]: {file_name}: {message}')

