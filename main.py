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

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
log_file_path = os.path.join(os.path.dirname(os.getcwd()), 'logs', 'execution.log')
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

PRE_SOLWEIG_FULL_PAUSE_TIME_SEC = 15

def main(source_base_path, target_base_path):
    abs_source_base_path = os.path.abspath(source_base_path)
    abs_target_path = os.path.abspath(target_base_path)
    delayed_results, solweig_delayed_results = build_processing_graphs(abs_source_base_path, abs_target_path)
    all_passed = start_processor(delayed_results)

    # wait for prior processing to complete
    time.sleep(PRE_SOLWEIG_FULL_PAUSE_TIME_SEC)
    # check that solweig completed

    all_passed = start_processor(solweig_delayed_results)


def start_processor(futures):
    # chunk size??
    from dask.distributed import Client
    # time 4 > 1 > 3 > 2
    Client(threads_per_worker=2, n_workers=int(0.5 * mp.cpu_count()), processes=False, asynchronous=False)
    dc = dask.compute(*futures)
    # dask.compute(*delayed_results, pure=False )


    results = []
    for obj in dc:
        if isinstance(obj, Iterable):
            for row in obj:
                results.append(row)
        else:
            results.append(obj)

    all_passed = True
    failed_tasks = []
    for row in results:
        if hasattr(row, 'stdout'):
            ss = get_substring_sandwich(row.stdout, '{"Return_package":', "}}")
            if ss is not None:
                return_package = json.loads(ss)['Return_package']
                if ss is None or return_package['return_code'] != 0:
                    task_index = json.loads(ss)['Task_index']
                    failed_tasks.append(task_index)
                    all_passed = False


    if not all_passed:
        task_str = ','.join(failed_tasks)
        count = len(failed_tasks)
        msg =  f'FAILURE: There were {count} processing failure for tasks indices: ({task_str})'
        _highlighted_print(msg)

    return all_passed

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

    script_path = os.path.abspath(os.path.join('..', 'workers', 'umep_plugin_processor.py'))
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
                delayed_result = _build_solweig_steps(script_path, task_index, method, city_folder_name,
                                                               tile_folder_name, source_base_path, target_base_path)
                delayed_results.append(delayed_result)
            else:
                proc_array = ['python', script_path, f'--task_index={task_index}', f'--method={method}',
                             f'--folder_name_city_data={city_folder_name}', f'--folder_name_tile_data={tile_folder_name}',
                             f'--source_data_path={source_base_path}', f'--target_path={target_base_path}']
                delayed_result = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
                delayed_results.append(delayed_result)

    return delayed_results, solweig_delayed_results


def _build_solweig_dependency(task_index, method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
    dep_delayed_results = []

    script_path = os.path.join('..', 'workers', 'umep_plugin_processor.py')
    proc_array = ['python', script_path, f'--task_index={task_index}', '--method=wall_height_aspect',
                  f'--folder_name_city_data={folder_name_city_data}', f'--folder_name_tile_data={folder_name_tile_data}',
                  f'--source_data_path={source_base_path}', f'--target_path={target_base_path}']
    walls = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(walls)

    proc_array = ['python', script_path, f'--task_index={task_index}', '--method=skyview_factor',
                  f'--folder_name_city_data={folder_name_city_data}', f'--folder_name_tile_data={folder_name_tile_data}',
                  f'--source_data_path={source_base_path}', f'--target_path={target_base_path}']
    skyview = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(skyview)

    solweig_delayed_results = _build_solweig_steps(script_path, task_index, method, folder_name_city_data,
                                                   folder_name_tile_data, source_base_path, target_base_path)

    return dep_delayed_results, solweig_delayed_results


def _build_solweig_steps(script_path, task_index, method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
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

            proc_array = ['python', script_path, f'--task_index={task_index}', f'--method={method}',
                          f'--folder_name_city_data={folder_name_city_data}', f'--folder_name_tile_data={folder_name_tile_data}',
                          f'--source_data_path={source_base_path}', f'--target_path={target_base_path}',
                          f'--met_file_name={met_file_name}', f'--utc_offset={utc_offset}']
            solweig = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            delayed_result.append(solweig)

        if return_code != 0:
            break
    return delayed_result


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


def _log_other_failure(message, e_msg):
    _highlighted_print('Failure. See log file.')
    logging.critical(f"**** FAILED execution with '{message}' ({e_msg})")


def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--data_source_folder', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--results_target_folder', metavar='path', required=True,
                        help='path to export results')
    args = parser.parse_args()

    return_code = main(data_source_folder=args.data_source_folder, results_target_folder=args.results_target_folder)
    print(return_code)
