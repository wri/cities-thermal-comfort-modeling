import os
import time
import json
import pandas as pd
import dask
import multiprocessing as mp
import subprocess
import logging

from collections.abc import Iterable
from workers.city_data import CityData
from workers.tools import get_application_path, create_folder

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
create_folder(os.path.abspath(os.path.join(get_application_path(), 'logs')))
log_file_path = os.path.abspath(os.path.join(get_application_path(), 'logs', 'execution.log'))
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s\t%(levelname)s\t%(message)s',
                    datefmt='%a_%Y_%b_%d_%H:%M:%S',
                    filename=log_file_path
                    )
dask.config.set({'logging.distributed': 'warning'})

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""

SCRIPT_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'umep_plugin_processor.py'))
PRE_SOLWEIG_FULL_PAUSE_TIME_SEC = 30

def main(source_base_path, target_base_path):
    abs_source_base_path = os.path.abspath(source_base_path)
    abs_target_path = os.path.abspath(target_base_path)
    delayed_results, solweig_delayed_results = build_processing_graphs(abs_source_base_path, abs_target_path)
    delays_all_passed = start_processor(delayed_results)

    # wait for prior processing to complete
    time.sleep(PRE_SOLWEIG_FULL_PAUSE_TIME_SEC)
    # check that solweig completed

    solweig_delays_all_passed = start_processor(solweig_delayed_results)

    return_code = 0 if (delays_all_passed and solweig_delays_all_passed) else 1
    return return_code


def start_processor(futures):
    # chunk size??
    from dask.distributed import Client
    with Client(n_workers=int(0.9 * mp.cpu_count()),
                threads_per_worker=1,
                processes=False,
                memory_limit='2GB',
                asynchronous=False
                ) as client:
        dc = dask.compute(*futures)
        # Client(threads_per_worker=2, n_workers=int(0.5 * mp.cpu_count()), processes=False, asynchronous=False)

    all_passed, failed_tasks, failed_task_details =_parse_and_log_return_package(dc)

    if not all_passed:
        task_str = ','.join(map(str,failed_tasks))
        count = len(failed_tasks)
        msg =  f'FAILURE: There were {count} processing failure for tasks indices: ({task_str})'
        _highlighted_print(msg)

        for failed_run in failed_task_details:
            _log_failure(failed_run, '')

    return all_passed

def _parse_and_log_return_package(dc):

    results = []
    # serialize the return information - one way or another
    for obj in dc:
        if isinstance(obj, Iterable):
            for row in obj:
                results.append(row)
        else:
            results.append(obj)

    # extract content from the return package and determine if there was a failure
    all_passed = True
    failed_task_ids = []
    failed_task_details = []
    for row in results:
        if hasattr(row, 'stdout'):
            return_info = get_substring_sandwich(row.stdout, '{"Return_package":', "}}")
            if return_info is not None:
                return_package = json.loads(return_info)['Return_package']
                if return_info is None or return_package['return_code'] != 0:
                    task_index = return_package['task_index']
                    subtask_index = return_package['subtask_index']
                    runtime = return_package['runtime']
                    failed_task_ids.append(task_index)
                    failed_task_details.append(row)
                    all_passed = False

    return all_passed, failed_task_ids, failed_task_details

def get_substring_sandwich(text, start_substring, end_substring):
    try:
        start_index = text.index(start_substring) # + len(start_substring)
        end_index = text.index(end_substring, start_index) + len(end_substring)
        return text[start_index:end_index]
    except ValueError:
        return None

def build_processing_graphs(source_base_path, target_base_path):
    config_processing_file_path = str(os.path.join(source_base_path, CityData.file_name_umep_city_processing_config))
    _verify_fundamental_paths(source_base_path, target_base_path, config_processing_file_path)

    processing_config_df = pd.read_csv(config_processing_file_path)
    _verify_processing_conf(processing_config_df, source_base_path, target_base_path)

    delayed_results = []
    solweig_delayed_results = []
    for index, config_row in processing_config_df.iterrows():
        enabled = bool(config_row.enabled)
        if enabled is True:
            task_index = index
            city_folder_name = config_row.city_folder_name
            tile_folder_name = config_row.tile_folder_name
            method = config_row.method
            if method == 'solweig_full':
                dep_delayed_results, solweig_task_delayed_results = \
                    _build_solweig_dependency(task_index, method, city_folder_name, tile_folder_name, source_base_path, target_base_path)
                delayed_results.extend(dep_delayed_results)
                solweig_delayed_results.extend(solweig_task_delayed_results)
            elif method == 'solweig_only':
                delayed_result = _build_solweig_steps(task_index, 0, method, city_folder_name,
                                                      tile_folder_name, source_base_path, target_base_path)
                delayed_results.append(delayed_result)
            else:
                proc_array = _construct_proc_array(task_index, 0, method, city_folder_name, tile_folder_name,
                                      source_base_path, target_base_path)
                delayed_result = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
                delayed_results.append(delayed_result)

    return delayed_results, solweig_delayed_results


def _build_solweig_dependency(task_index, method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
    dep_delayed_results = []
    proc_array = _construct_proc_array(task_index, 0, 'wall_height_aspect', folder_name_city_data, folder_name_tile_data,
                                       source_base_path, target_base_path, None, None)
    walls = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(walls)

    proc_array = _construct_proc_array(task_index, 1, 'skyview_factor', folder_name_city_data, folder_name_tile_data,
                                       source_base_path, target_base_path, None, None)
    skyview = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(skyview)

    solweig_delayed_results = _build_solweig_steps(task_index, 2, method, folder_name_city_data,
                                                   folder_name_tile_data, source_base_path, target_base_path)

    return dep_delayed_results, solweig_delayed_results


def _build_solweig_steps(task_index, subtask_index, method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
    city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)

    delayed_result = []
    config_met_time_series_path = str(os.path.join(city_data.source_city_data_path, city_data.file_name_met_time_series_config))
    met_time_series_config_df = pd.read_csv(config_met_time_series_path)

    return_code = 0
    for index, config_row in met_time_series_config_df.iterrows():
        enabled = bool(config_row.enabled)
        if enabled:
            met_file_name = config_row.met_file_name
            utc_offset = config_row.utc_offset

            proc_array = _construct_proc_array(task_index, subtask_index, method, folder_name_city_data, folder_name_tile_data,
                                               source_base_path, target_base_path, met_file_name, utc_offset)
            solweig = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            delayed_result.append(solweig)

        if return_code != 0:
            break
    return delayed_result


def _construct_proc_array(task_index, subtask_index, method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path,
                          met_file_name=None, utc_offset=None):
    proc_array = ['python', SCRIPT_PATH, f'--task_index={task_index}', f'--subtask_index={subtask_index}', f'--method={method}',
                  f'--folder_name_city_data={folder_name_city_data}',
                  f'--folder_name_tile_data={folder_name_tile_data}',
                  f'--source_data_path={source_base_path}', f'--target_path={target_base_path}',
                  f'--met_file_name={met_file_name}', f'--utc_offset={utc_offset}']
    return proc_array


toBool = {'true': True, 'false': False}


def _verify_fundamental_paths(source_data_path, target_path, config_processing_file_path):
    if _verify_path(source_data_path) is False:
        msg = f'Invalid path: {source_data_path}'
        raise Exception(msg)
    elif _verify_path(target_path) is False:
        msg = f'Invalid path: {target_path}'
        raise Exception(msg)
    elif _verify_path(config_processing_file_path) is False:
        msg = f'Processing Registry file does not exist: {config_processing_file_path}'
        raise Exception(msg)

def _verify_path(path):
    is_valid = os.path.exists(path)
    return is_valid

def _verify_processing_conf(processing_config_df, source_base_path, target_base_path):
    for index, config_row in processing_config_df.iterrows():
        enabled = str(config_row.enabled)
        valid_enabled = ['true', 'false']
        if enabled.lower() not in valid_enabled:
            raise Exception(f"Invalid 'enable' value ({str(enabled)}) on row {index} and possibly other rows. Valid values: {valid_enabled}")

        if bool(enabled):
            folder_name_city_data = config_row.city_folder_name
            city_path = os.path.join(source_base_path, folder_name_city_data)
            if not os.path.isdir(city_path):
                raise Exception(
                    f"Invalid 'city_folder_name' value ({str(folder_name_city_data)}) on row {index} and possibly wrong on other rows using source path '{source_base_path}'.")

            folder_name_tile_data = config_row.tile_folder_name
            city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
            source_tile_path = city_data.source_tile_data_path
            if not os.path.isdir(source_tile_path):
                raise Exception(
                    f"Tile path ({str(source_tile_path)}) on row {index} does not exist and possibly wrong on other rows.")

            method = config_row.method
            valid_methods = CityData.plugin_methods
            if method not in valid_methods:
                raise Exception(f"Invalid 'method' ({method}) on row {index} and possibly other rows. Valid values: {valid_methods}")


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
    args = parser.parse_args()

    return_code = main(source_base_path=args.source_base_path, target_base_path=args.target_base_path)

    print(return_code)
