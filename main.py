import os
import time
import json
import pandas as pd
import dask
import multiprocessing as mp
import subprocess
import warnings
warnings.filterwarnings('ignore')

from datetime import datetime
from collections.abc import Iterable
from src.source_quality_verifier import verify_fundamental_paths, verify_processing_config
from workers.city_data import CityData
from src.tools import get_application_path, create_folder

import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

dask.config.set({'logging.distributed': 'warning'})

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""

SCRIPT_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'umep_plugin_processor.py'))
PRE_SOLWEIG_FULL_PAUSE_TIME_SEC = 30


def main(source_base_path, target_base_path, city_folder_name, pre_check_option):
    abs_source_base_path = os.path.abspath(source_base_path)
    abs_target_base_path = os.path.abspath(target_base_path)
    return_code_basic = _validate_basic_inputs(abs_source_base_path, abs_target_base_path, city_folder_name)

    processing_config_df = _build_source_dataframes(abs_source_base_path, city_folder_name)
    return_code_configs = _validate_config_inputs(processing_config_df, abs_source_base_path, abs_target_base_path, city_folder_name, pre_check_option)

    # TODO - Add checks for prerequite met data, such as consistent CRS

    if pre_check_option == 'no_pre_check':
        _start_logging(abs_target_base_path, city_folder_name)

        enabled_processing_tasks_df = processing_config_df[(processing_config_df['enabled'])]
        delayed_results, solweig_delayed_results = _build_processing_graphs(enabled_processing_tasks_df, abs_source_base_path, abs_target_base_path, city_folder_name)

        # Task processing
        delays_all_passed, results_df = process_rows(delayed_results)
        # wait for prior processing to complete
        time.sleep(PRE_SOLWEIG_FULL_PAUSE_TIME_SEC)
        solweig_delays_all_passed, solweig_results_df = process_rows(solweig_delayed_results)

        # Write run_report
        report_file_path = _report_results(enabled_processing_tasks_df, results_df, solweig_results_df, abs_target_base_path, city_folder_name)
        print(f'\nRun report written to {report_file_path}')

        return_code = 0 if (delays_all_passed and solweig_delays_all_passed) else 1

        if return_code == 0:
            print("\nProcessing encountered no errors.")
        else:
            _highlighted_print('Processing encountered no errors. See log file.')

        return return_code
    else:
        if return_code_basic == 0 and return_code_configs == 0:
            print("\nPassed all validation checks")
            return 0
        else:
            _highlighted_print('Pre-check encountered errors.')
            return -99

def _start_logging(target_base_path, city_folder_name):
    results_subfolder = CityData.folder_name_results
    log_folder_path = str(os.path.join(target_base_path, city_folder_name, results_subfolder, '.logs'))
    create_folder(log_folder_path)
    log_file_path = os.path.join(log_folder_path, 'execution.log')
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s\t%(levelname)s\t%(message)s',
                        datefmt='%a_%Y_%b_%d_%H:%M:%S',
                        filename=log_file_path
                        )

def _report_results(enabled_processing_tasks_df, results_df, solweig_results_df, target_base_path, city_folder_name):
    combined_results = results_df if solweig_results_df.empty else\
        pd.concat([results_df, solweig_results_df], ignore_index=True)

    combined_results.sort_values(['task_index', 'step_index', 'met_filename'], inplace=True)

    merged = pd.merge(enabled_processing_tasks_df, combined_results, left_index=True, right_on='task_index',
                      how='outer')
    merged['run_status'] = merged['return_code'].apply(_evaluate_return_code)
    merged['city_folder_name'] = city_folder_name

    reporting_df = merged.loc[:,
                   ['run_status', 'task_index', 'city_folder_name', 'tile_folder_name', 'method', 'step_index',
                    'step_method', 'met_filename',
                    'return_code', 'start_time', 'run_duration_min']]

    results_subfolder = CityData.folder_name_results
    report_folder = str(os.path.join(target_base_path, city_folder_name, results_subfolder, '.run_reports'))
    create_folder(report_folder)

    report_date_str =  datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    report_name = f'run_report_{report_date_str}.html'
    report_file_path = os.path.join(report_folder, report_name)

    reporting_df.to_html(report_file_path)

    return report_file_path

def _evaluate_return_code(return_code):
    return 'success' if return_code == 0 else 'FAILURE'


def _validate_basic_inputs(source_base_path, target_path, city_folder_name):
    invalids = verify_fundamental_paths(source_base_path, target_path, city_folder_name)
    if invalids:
        for invalid in invalids:
            _highlighted_print(invalid)
        raise Exception("Stopping due to invalid source/target folders.")
    else:
        return 0

def _validate_config_inputs(processing_config_df, source_base_path, target_path, city_folder_name, pre_check_option):
    detailed_invalids = verify_processing_config(processing_config_df, source_base_path, target_path, city_folder_name, pre_check_option)
    if detailed_invalids:
        for invalid in detailed_invalids:
            _highlighted_print(invalid)
        raise Exception("Stopping due to invalid configurations.")
    else:
        return 0

def _build_source_dataframes(source_base_path, city_folder_name):
    config_processing_file_path = str(os.path.join(source_base_path, city_folder_name, CityData.filename_umep_city_processing_config))
    processing_config_df = pd.read_csv(config_processing_file_path)

    return processing_config_df

def process_rows(futures):
    # chunk size??
    from dask.distributed import Client
    with Client(n_workers=int(0.9 * mp.cpu_count()),
                threads_per_worker=1,
                processes=False,
                memory_limit='2GB',
                asynchronous=False
                ) as client:

        msg = f'*************Monitor processing at {client.dashboard_link}'
        _log_info_msg(msg)
        dc = dask.compute(*futures)

    all_passed, results_df, failed_task_ids, failed_task_details =_parse_and_report_row_results(dc)

    if not all_passed:
        task_str = ','.join(map(str,failed_task_ids))
        count = len(failed_task_ids)
        msg =  f'FAILURE: There were {count} processing failure for tasks indices: ({task_str})'
        _highlighted_print(msg)

        for failed_run in failed_task_details:
            _log_failure(failed_run, '')

    return all_passed, results_df

def _parse_and_report_row_results(dc):
    results = []
    # serialize the return information - one way or another
    for obj in dc:
        if isinstance(obj, Iterable):
            for row in obj:
                results.append(row)
        else:
            results.append(obj)

    # extract content from the return package and determine if there was a failure
    results_df = pd.DataFrame(columns=['task_index', 'step_index', 'step_method', 'met_filename', 'return_code', 'start_time', 'run_duration_min'])
    all_passed = True
    failed_task_ids = []
    failed_task_details = []
    for row in results:
        if hasattr(row, 'stdout'):
            return_info = _get_inclusive_between_string(row.stdout, '{"Return_package":', "}}")
            if return_info:
                return_package = json.loads(return_info)['Return_package']
                task_index = return_package['task_index']
                step_index = return_package['step_index']
                step_method = return_package['step_method']
                met_filename = return_package['met_filename']
                return_code = return_package['return_code']
                start_time = return_package['start_time']
                run_duration_min = return_package['run_duration_min']

                new_row = [task_index, step_index, step_method, met_filename, return_code, start_time, run_duration_min]
                results_df.loc[len(results_df.index)] = new_row

                if return_code != 0:
                    failed_task_ids.append(task_index)
                    failed_task_details.append(row)
                    all_passed = False

    return all_passed, results_df, failed_task_ids, failed_task_details

def _get_inclusive_between_string(text, start_substring, end_substring):
    try:
        start_index = text.index(start_substring)
        end_index = text.index(end_substring, start_index) + len(end_substring)
        return text[start_index:end_index]
    except ValueError:
        return None

def _build_processing_graphs(enabled_processing_task_df, source_base_path, target_base_path, city_folder_name):
    delayed_results = []
    solweig_delayed_results = []
    for index, config_row in enabled_processing_task_df.iterrows():
        task_index = index
        tile_folder_name = config_row.tile_folder_name
        task_method = config_row.method
        if task_method == 'solweig_full':
            dep_delayed_results, solweig_task_delayed_results = \
                _build_solweig_full_dependency(task_index, city_folder_name, tile_folder_name, source_base_path, target_base_path)
            delayed_results.extend(dep_delayed_results)
            solweig_delayed_results.extend(solweig_task_delayed_results)
        elif task_method == 'solweig_only':
            delayed_result = _build_solweig_only_steps(task_index, 0, city_folder_name,
                                                       tile_folder_name, source_base_path, target_base_path)
            delayed_results.append(delayed_result)
        else:
            proc_array = _construct_proc_array(task_index, 0, task_method, city_folder_name, tile_folder_name,
                                  source_base_path, target_base_path)
            delayed_result = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            delayed_results.append(delayed_result)

    return delayed_results, solweig_delayed_results


def _build_solweig_full_dependency(task_index, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
    dep_delayed_results = []
    proc_array = _construct_proc_array(task_index, 0, 'wall_height_aspect', folder_name_city_data, folder_name_tile_data,
                                       source_base_path, target_base_path, None, None)
    walls = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(walls)

    proc_array = _construct_proc_array(task_index, 1, 'skyview_factor', folder_name_city_data, folder_name_tile_data,
                                       source_base_path, target_base_path, None, None)
    skyview = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(skyview)

    solweig_delayed_results = _build_solweig_only_steps(task_index, 2, folder_name_city_data,
                                                        folder_name_tile_data, source_base_path, target_base_path)

    return dep_delayed_results, solweig_delayed_results


def _build_solweig_only_steps(task_index, step_index, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
    city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)

    delayed_result = []
    for met_file in city_data.met_files:
        met_filename = met_file.get('filename')
        utc_offset = met_file.get('utc_offset')

        proc_array = _construct_proc_array(task_index, step_index, 'solweig_only', folder_name_city_data, folder_name_tile_data,
                                           source_base_path, target_base_path, met_filename, utc_offset)
        solweig = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
        delayed_result.append(solweig)
    return delayed_result


def _construct_proc_array(task_index, step_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path,
                          met_filename=None, utc_offset=None):
    proc_array = ['python', SCRIPT_PATH, f'--task_index={task_index}', f'--step_index={step_index}', f'--step_method={step_method}',
                  f'--folder_name_city_data={folder_name_city_data}',
                  f'--folder_name_tile_data={folder_name_tile_data}',
                  f'--source_data_path={source_base_path}', f'--target_path={target_base_path}',
                  f'--met_filename={met_filename}', f'--utc_offset={utc_offset}']
    return proc_array


toBool = {'true': True, 'false': False}


def _log_info_msg(message):
    logging.info(message)

def _log_failure(message, e_msg):
    _highlighted_print('Failure. See log file.')
    logging.critical(f"**** FAILED execution with '{message}' ({e_msg})")

def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--source_base_path', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--target_base_path', metavar='path', required=True,
                        help='path to export results')
    parser.add_argument('--city_folder_name', metavar='path', required=True,
                        help='name of source city_folder')

    valid_methods = ['no_pre_check', 'check_all', 'check_enabled_only']
    parser.add_argument('--pre_check_option', metavar='str', choices=valid_methods, required=True,
                        help=f'specifies type of configuration pre-check. Options are: {valid_methods}')
    args = parser.parse_args()

    return_code = main(source_base_path=args.source_base_path, target_base_path=args.target_base_path,
                       city_folder_name=args.city_folder_name, pre_check_option=args.pre_check_option)

    print(return_code)
