import os
from src.process_runner import run_plugins
import logging
from logging_config import logger_name
logger = logging.getLogger(logger_name())

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
"""
def main(data_source_folder, results_target_folder):
    run_plugins(data_source_folder, results_target_folder)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--data_source_folder', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--results_target_folder', metavar='path', required=True,
                        help='path to export results')
    args = parser.parse_args()

    main(data_source_folder=args.data_source_folder, results_target_folder=args.results_target_folder)