import os
import math
import datetime

from src.worker_manager.config_validator import validate_basic_inputs, validate_config_inputs, _verify_processing_config
from src.worker_manager.job_manager import start_jobs

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""


def start_processing(source_base_path, target_base_path, city_folder_name, processing_option):
    abs_source_base_path = os.path.abspath(source_base_path)
    abs_target_base_path = os.path.abspath(target_base_path)

    return_code_basic = validate_basic_inputs(abs_source_base_path, abs_target_base_path, city_folder_name)

    if processing_option == 'run_pipeline':
        precheck_option = 'pre_check'
    else:
        precheck_option = processing_option

    solweig_full_cell_count, return_code_configs  = validate_config_inputs(abs_source_base_path, abs_target_base_path,
                                                                           city_folder_name, precheck_option)

    # Print runtime estimate
    if solweig_full_cell_count is not None:
        if solweig_full_cell_count < 2E3:
            print('\nEstimated runtime is a few minutes or less.\n')
        else:
            if solweig_full_cell_count < 1E6:
                x = solweig_full_cell_count
                est_runtime_mins = math.ceil((7E-5 * x) + 6.9158)
            else:
                x = pow(solweig_full_cell_count, 9)
                est_runtime_mins = math.ceil((4E-58 * x) + 77.95)
            _print_runtime_estimate(est_runtime_mins)

    if processing_option == 'run_pipeline':
        return_code, return_str = start_jobs(abs_source_base_path, abs_target_base_path, city_folder_name)

        if return_code == 0:
            print(return_str)
        else:
            _highlighted_yellow_print(return_str)
        return return_code


def _print_runtime_estimate(est_runtime_mins):
    est_runtime_hours = round(est_runtime_mins / 60, 1)
    now = datetime.datetime.now()
    time_change = datetime.timedelta(minutes=est_runtime_mins)
    est_end_time = now + time_change
    print(f'\nEstimated runtime: {est_runtime_hours} hours.')
    print(f'Estimated completion time for processing of tile_001: {est_end_time.strftime('%m/%d/%Y %I:%M %p')}\n')


def _highlighted_yellow_print(msg):
    print('\n\x1b[6;30;43m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--source_base_path', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--target_base_path', metavar='str', required=True,
                        help='the path to city-based target data')
    parser.add_argument('--city_folder_name', metavar='str', required=True,
                        help='name of source city_folder')
    valid_methods = ['run_pipeline', 'pre_check_all', 'pre_check']
    parser.add_argument('--processing_option', metavar='str', choices=valid_methods, required=True,
                        help=f'specifies type of configuration pre-check. Options are: {valid_methods}')
    args = parser.parse_args()

    start_processing(args.source_base_path, args.target_base_path, args.city_folder_name, args.processing_option)
