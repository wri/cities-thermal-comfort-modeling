import os
import csv
import dask
import multiprocessing as mp
import subprocess

import logging

from workers.city_data import instantiate_city_data

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
log_file_path = os.path.join(os.path.dirname(os.getcwd()), 'logs', 'execution.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s\t%(levelname)s\t%(message)s',
                    datefmt='%a_%Y_%b_%d_%H:%M:%S',
                    filename=log_file_path
                    )
"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""
UMEP_CITY_PROCESSING_REGISTRY_FILE = 'umep_city_processing_registry.csv'
SOLWEIG_TIME_SERIES_CONFIG_FILE = 'time_series_config.csv'
METHODS = ['all', 'wall_height_aspect', 'skyview_factor', 'solweig']


def main(data_source_folder, results_target_folder):
    delayed_results, solweig_delayed_results = build_processing_graphs(data_source_folder, results_target_folder)
    start_processor(delayed_results)
    # check that solweigs completed
    start_processor(solweig_delayed_results)
    b = 2

def start_processor(futures):
    # chunk size??
    from dask.distributed import Client
    # time 4 > 1 > 3 > 2
    Client(threads_per_worker=2, n_workers=int(0.9 * mp.cpu_count()), processes=False, asynchronous=False)
    dc = dask.compute(*futures)
    # dask.compute(*delayed_results, pure=False )

    all_passed = True
    for procrun in dc:
        stdout_list = procrun.stdout.split('\n')
        run_return_code = stdout_list[len(stdout_list)-2]
        if run_return_code != '0':
            all_passed = False
            msg =  'FAILURE: There was at least one processing failure.'
            _highlighted_print(msg)
            break

    return all_passed

def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')

def build_processing_graphs(data_source_folder, results_target_folder):
    source_data_path = os.path.abspath(data_source_folder)
    target_path = os.path.abspath(results_target_folder)
    config_processing_file_path = os.path.join(source_data_path, UMEP_CITY_PROCESSING_REGISTRY_FILE)
    return_code = _verify_io_paths(source_data_path, target_path, config_processing_file_path)
    if return_code != 0:
        raise Exception("Invalid specifications.")

    script_path = os.path.join('..', 'workers', 'umep_plugin_processor.py')
    with open(config_processing_file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader, None)  # skip the headers
        # Iterate over each row in the CSV file
        delayed_results = []
        solweig_delayed_results = []
        for row in csv_reader:
            if row:
                enabled = toBool[row[1].lower()]
                if enabled is True:
                    task_id = row[0]
                    city_folder_name = row[2]
                    tile_name = row[3]
                    method = row[4].lower()
                    is_valid_method = _verify_method(task_id, method)
                    if is_valid_method is not True:
                        continue
                    else:
                        if method != 'solweig':
                            proc_array = ['python', script_path, '--task_id=%s' % task_id, '--method=%s' % method,
                                         '--city_folder_name=%s' % city_folder_name, '--tile_folder_name=%s' % tile_name,
                                         '--source_data_path=%s' % source_data_path, '--target_path=%s' % results_target_folder]
                            delayed_result = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
                            delayed_results.append(delayed_result)
                        else:
                            dep_delayed_results, solweig_task_delayed_results = \
                                _build_solweig_dependency(task_id, city_folder_name, tile_name, source_data_path, results_target_folder)
                            delayed_results.extend(dep_delayed_results)
                            solweig_delayed_results.extend(solweig_task_delayed_results)

    return delayed_results, solweig_delayed_results

def _build_solweig_dependency(task_id, city_folder_name, tile_folder_name, source_data_path, target_path):
    dep_delayed_results = []

    script_path = os.path.join('..', 'workers', 'umep_plugin_processor.py')
    proc_array = ['python', script_path, '--task_id=%s' % task_id, '--method=%s' % 'wall_height_aspect',
                  '--city_folder_name=%s' % city_folder_name, '--tile_folder_name=%s' % tile_folder_name,
                  '--source_data_path=%s' % source_data_path, '--target_path=%s' % target_path]
    walls = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(walls)

    proc_array = ['python', script_path, '--task_id=%s' % task_id, '--method=%s' % 'skyview_factor',
                  '--city_folder_name=%s' % city_folder_name, '--tile_folder_name=%s' % tile_folder_name,
                  '--source_data_path=%s' % source_data_path, '--target_path=%s' % target_path]
    skyview = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    dep_delayed_results.append(skyview)

    solweig_delayed_results = _build_solweig_steps(script_path, task_id, city_folder_name, tile_folder_name, source_data_path, target_path)

    return dep_delayed_results, solweig_delayed_results


def _build_solweig_steps(script_path, task_id, city_folder_name, tile_folder_name, source_data_path, target_path):
    city_data = instantiate_city_data(city_folder_name, tile_folder_name, source_data_path, target_path)

    solweig_task_delayed_results = []
    config_tcm_time_series_path = os.path.join(city_data.city_source_path, SOLWEIG_TIME_SERIES_CONFIG_FILE)
    return_code = 0
    with open(config_tcm_time_series_path, mode='r') as solweig_config_file:
        csv_reader = csv.reader(solweig_config_file)
        next(csv_reader, None)  # skip the headers

        for row in csv_reader:
            step = row[0]
            enabled = toBool[row[1].lower()]
            if enabled:
                met_file_name = row[2]
                utc_offset = row[3]

                proc_array = ['python', script_path, '--task_id=%s' % task_id, '--method=%s' % 'solweig',
                              '--city_folder_name=%s' % city_folder_name, '--tile_folder_name=%s' % tile_folder_name,
                              '--source_data_path=%s' % source_data_path, '--target_path=%s' % target_path,
                              '--met_file_name=%s' % met_file_name, '--utc_offset=%s' % utc_offset]
                solweig = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
                solweig_task_delayed_results.append(solweig)

            if return_code != 0:
                break
    return solweig_task_delayed_results


toBool = {'true': True, 'false': False}


def _verify_io_paths(source_data_path, target_path, config_processing_file_path):
    if _verify_path(source_data_path) is False:
        msg = 'Invalid path: %s' % source_data_path
        _highlighted_print(msg)
        log_other_failure(msg, '')
        return 97
    elif _verify_path(target_path) is False:
        msg = 'Invalid path: %s' % target_path
        _highlighted_print(msg)
        log_other_failure(msg, '')
        return 98
    elif _verify_path(config_processing_file_path) is False:
        msg = 'Processing Registry file does not exist: %s' % config_processing_file_path
        _highlighted_print(msg)
        log_other_failure(msg, '')
        return 99
    return 0


def _verify_path(path):
    is_valid = os.path.exists(path)
    return is_valid


def _verify_method(task_id, method):
    if method not in METHODS:
        log_other_failure(('Skipping task_id:%s due to invalid method name: %s' % (task_id,method)), '')
        return False
    else:
        return True


def log_other_failure(message, e_msg):
    print('Failure. See log file.')
    logging.critical("**** FAILED execution with '%s' (%s)" % (message, e_msg))


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
