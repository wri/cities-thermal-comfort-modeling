import logging
from logging_config import logger_name
logger = logging.getLogger(logger_name())
from datetime import datetime

def simple_info_message(message):
    logger.info(message)

def log_method_start(method, runid, step, source_base_path):
    if step is None:
        logger.info("RunId:%s\tStarting '%s'\tconfig:'%s')" % (runid, method, source_base_path))
    else:
        logger.info("RunId:%s\tStarting '%s' for met_series:%s\tconfig:'%s'" % (runid, method, step, source_base_path))

def log_method_completion(start_time, method, runId, step, source_base_path):
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.info("RunId:%s\tFinished '%s', runtime:%s mins\tconfig:'%s'" % (runId, method, runtime, source_base_path))
    else:
        logging.info("RunId:%s\tFinished '%s' for met_series:%s, runtime:%s mins\tconfig:'%s'" % (runId, method, step, runtime, source_base_path))

def log_method_failure(start_time, feature, runId, step, source_base_path, e_msg):
    print('Failure. See log file.')
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.error("RunId:%s\t**** FAILED execution of '%s' after runtime:%s mins\tconfig:'%s'(%s)" % (runId, feature, runtime, source_base_path, e_msg))
    else:
        logging.error("RunId:%s\t**** FAILED execution of '%s' fpr met_series:%s after runtime:%s mins\tconfig:'%s'(%s)" % (runId, feature, step, runtime, source_base_path, e_msg))

def log_other_failure(message, e_msg):
    print('Failure. See log file.')
    logging.critical("**** FAILED execution with '%s' (%s)" % (message, e_msg))