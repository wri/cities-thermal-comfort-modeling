# https://betterstack.com/community/guides/logging/python/python-logging-best-practices/#
import logging.handlers
import os
from src.tools import create_folder, get_application_path

log_folder = os.path.join(get_application_path(), 'logs')
create_folder(log_folder)
log_file = os.path.join(log_folder, 'execution.log')

def logger_name():
    return 'tmc_logger'

# Create a rotating file handler
handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=1000000, backupCount=5)

logger = logging.getLogger(logger_name())
logger.setLevel(logging.DEBUG)

# Set the formatter for the handler
formatter = logging.Formatter(fmt='%(asctime)s\t%(levelname)s\t%(message)s', datefmt='%a_%Y_%b_%d_%H:%M:%S', style="%")
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

