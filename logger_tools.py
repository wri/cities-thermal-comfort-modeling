import logging
from logging_config import logger_name
logger = logging.getLogger(logger_name())
from datetime import datetime

def simple_info_message(message):
    logger.info(message)

def log_method_start(method, task_id, step, source_base_path):
    if step is None:
        logger.info("task_id:%s\tStarting '%s'\tconfig:'%s')" % (task_id, method, source_base_path))
    else:
        logger.info("task_id:%s\tStarting '%s' for met_series:%s\tconfig:'%s'" % (task_id, method, step, source_base_path))

def log_method_completion(start_time, method, task_id, step, source_base_path):
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.info("task_id:%s\tFinished '%s', runtime:%s mins\tconfig:'%s'" % (task_id, method, runtime, source_base_path))
    else:
        logging.info("task_id:%s\tFinished '%s' for met_series:%s, runtime:%s mins\tconfig:'%s'" % (task_id, method, step, runtime, source_base_path))

def log_method_failure(start_time, feature, task_id, step, source_base_path, e_msg):
    print('Method ailure. See log file.')
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.error("task_id:%s\t**** FAILED execution of '%s' after runtime:%s mins\tconfig:'%s'(%s)" % (task_id, feature, runtime, source_base_path, e_msg))
    else:
        logging.error("task_id:%s\t**** FAILED execution of '%s' fpr met_series:%s after runtime:%s mins\tconfig:'%s'(%s)" % (task_id, feature, step, runtime, source_base_path, e_msg))

def log_other_failure(message, e_msg):
    print('Failure. See log file.')
    logging.critical("**** FAILED execution with '%s' (%s)" % (message, e_msg))