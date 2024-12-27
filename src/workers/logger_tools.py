import logging
import datetime
from pathlib import Path

from src.workers.worker_tools import create_folder, compute_time_diff_mins


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

def log_method_start(method, task_index, step, source_base_path, logger):
    if step is None:
        logger.info(f"task:{task_index}\tStarting '{method}'\tconfig:'{source_base_path}')")
    else:
        logger.info(f"task:{task_index}\tStarting '{method}' for met_series:{step}\tconfig:'{source_base_path}'")


def log_method_completion(start_time, method, task_index, step, source_base_path, logger):
    runtime = compute_time_diff_mins(start_time)
    if step is None:
        logger.info(f"task:{task_index}\tFinished '{method}', runtime:{runtime} mins\tconfig:'{source_base_path}'")
    else:
        logger.info(f"task:{task_index}\tFinished '{method}' for met_series:{step}, runtime:{runtime} mins\tconfig:'{source_base_path}'")


def log_method_failure(start_time, feature, task_index, step, source_base_path, e_msg, logger):
    print('Method failure. See log file.')
    runtime = compute_time_diff_mins(start_time)
    if step is None:
        logger.error(f"task:{task_index}\t**** FAILED execution of '{feature}' after runtime:{runtime} mins\tconfig:'{source_base_path}'({e_msg})")
    else:
        logger.error(f"task:{task_index}\t**** FAILED execution of '{feature}' fpr met_series:{step} after runtime:{runtime} mins\tconfig:'{source_base_path}'({e_msg})")

