import os
import math
import datetime

from worker_manager.config_validator import validate_basic_inputs, validate_config_inputs
from worker_manager.graph_builder import _build_source_dataframes
from worker_manager.job_manager import start_jobs

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""

def start_processing(base_path, city_folder_name, processing_option):
    # Unify the data into a single umbrella folder
    # TODO Remove the separation of source and target base path
    abs_source_base_path = os.path.abspath(base_path)
    abs_target_base_path = os.path.abspath(base_path)

    return_code_basic = validate_basic_inputs(abs_source_base_path, abs_target_base_path, city_folder_name)

    processing_config_df = _build_source_dataframes(abs_source_base_path, city_folder_name)

    if processing_option == 'run_pipeline':
        precheck_option = 'pre_check'
    else:
        precheck_option = processing_option

    solweig_full_cell_count, return_code_configs = validate_config_inputs(processing_config_df, abs_source_base_path,
                                                                          abs_target_base_path, city_folder_name, precheck_option)

    # Print runtime estimate
    if solweig_full_cell_count is not None:
        est_runtime_mins = math.ceil((6E-38 * pow(solweig_full_cell_count,6)) - 2.8947)
        if est_runtime_mins < 5:
            print('\nEstimated runtime of a few minutes or less for tile_001.\n')
        else:
            now = datetime.datetime.now()
            time_change = datetime.timedelta(minutes=est_runtime_mins)
            est_end_time = now + time_change
            print(f'\nEstimated completion time for processing of tile_001: {est_end_time.strftime('%m/%d/%Y %I:%M %p')}\n')

    if processing_option == 'run_pipeline':
        return_code, return_str = start_jobs(abs_source_base_path, abs_target_base_path, city_folder_name, processing_config_df)

        if return_code == 0:
            print(return_str)
        else:
            _highlighted_print(return_str)
        return return_code
    else:
        if return_code_basic == 0 and return_code_configs == 0:
            print("\nPassed all validation checks")
            return 0
        else:
            _highlighted_print('Pre-check encountered errors.')
            return -99

def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--base_path', metavar='path', required=True,
                        help='the path to city-based source and results data')
    parser.add_argument('--city_folder_name', metavar='path', required=True,
                        help='name of source city_folder')

    valid_methods = ['run_pipeline', 'pre_check_all', 'pre_check']
    parser.add_argument('--processing_option', metavar='str', choices=valid_methods, required=True,
                        help=f'specifies type of configuration pre-check. Options are: {valid_methods}')
    args = parser.parse_args()

    return_code = start_processing(args.base_path, args.city_folder_name, args.processing_option)

    print(f'return code: {return_code}')
