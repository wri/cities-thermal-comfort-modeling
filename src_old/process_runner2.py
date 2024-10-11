import csv
import os

import dask
# from dask.bag.text import delayed
# from dask.distributed import Client
# client = Client(threads_per_worker=2, n_workers=2, processes=False)
# client = Client(n_workers=2, processes=True)

# from dask.distributed import LocalCluster
# cluster = LocalCluster()

#  https://computing.stat.berkeley.edu/tutorial-dask-future/python-dask.html#:~:text=To%20run%20within%20a%20SLURM%20job%20we%20can%20use%20dask-ssh
# import dask.multiprocessing
# dask.config.set(scheduler='processes', num_workers = 4)
# from dask.distributed import Client, LocalCluster
# cluster = LocalCluster(n_workers = 4)
# c = Client(cluster)

# import multiprocessing as mp
# cluster = LocalCluster(
#     n_workers=int(0.9 * mp.cpu_count()),
#     # n_workers = 5,
#     processes=True,
#     threads_per_worker=2
# )
from dask.distributed import Client
client = Client(threads_per_worker=1, n_workers=1, processes=True)

# from dask.distributed import Client #, LocalCluster
# c = Client(threads_per_worker=4, n_workers=1)
# t = c.cluster
# b = 2


from datetime import datetime
from pathlib import Path
from src_old import tools as src_tools
from src_old.CityData import instantiate_city_data
from src_old.tools import verify_path


from src_old.umep_for_processing_plugins import log_method_failure, log_other_failure, \
    generate_wall_height_aspect, generate_skyview_factor_files, generate_solweig

UMEP_CITY_PROCESSING_REGISTRY_FILE = 'umep_city_processing_registry.csv'
SOLWEIG_TIME_SERIES_CONFIG_FILE = 'time_series_config.csv'
METHODS = ['all', 'wall_height_aspect', 'skyview_factor', 'solweig']



def run_plugins(data_source_folder, results_target_folder):
    source_data_path = os.path.abspath(data_source_folder)
    target_path = os.path.abspath(results_target_folder)
    config_processing_file_path = os.path.join(source_data_path, UMEP_CITY_PROCESSING_REGISTRY_FILE)
    _verify_source_paths(source_data_path, target_path, config_processing_file_path)
    _process_task_list(source_data_path, target_path, config_processing_file_path)

# https://stackoverflow.com/questions/53394935/what-is-the-right-way-to-close-a-dask-localcluster#:~:text=I%20am%20trying%20to%20use%20dask-distributed%20on%20my%20laptop%20using
def _process_task_list(source_data_path, target_path, config_processing_file_path):
    with (open(config_processing_file_path, mode='r') as file):
        csv_reader = csv.reader(file)
        next(csv_reader, None)  # skip the headers
        # Iterate over each row in the CSV file
        try:
            futures = []
            for row in csv_reader:
                if row:
                    enabled = src_tools.toBool[row[1].lower()]
                    if enabled is True:
                        task_id = row[0]
                        city_folder_name = row[2]
                        tile_folder_name = row[3]

                        method = row[4].lower()
                        is_valid_method = _verify_method(task_id, method)
                        if is_valid_method is not True:
                            return False

                        # _process_task(task_id, method, city_folder_name, tile_folder_name, source_data_path, target_path)
                        # futures.append(dask.delayed(_process_task(row, source_data_path, target_path)))
                        # _process_task(row, source_data_path, target_path)

                        futures.append(print_caller(row[2]))

                        # if is_success is not True:
                        #     continue
            dask.compute(futures)
        except Exception as e_msg:
            # client.close()
            b=2


@dask.delayed
def print_caller(city):
    printer(city)

def printer(city):
    for x in range(20):
        print('\nhi %s for %s' % (city, x))

# @dask.delayed
def _process_task(task_id, method, city_folder_name, tile_folder_name, source_data_path, target_path):
    job_success = True
    start_time = datetime.now()
    city_data = instantiate_city_data(city_folder_name, tile_folder_name, source_data_path, target_path)
    all_valid = _verify_all_primary_paths(task_id, city_data)
    if all_valid is not True:
        return False

    # TODO - Add checks for prerequite data!!
    e_msg = ''
    return_code = _run_wall_height_aspect(task_id, city_data, method)
    if return_code == 0:
        return_code = _run_skyview_factor(task_id, city_data, method)
        if return_code == 0:
            return_code = _run_solweig(task_id, city_data, method)
            if return_code != 0:
                job_success = False
        else:
            job_success = False
    else:
        job_success = False

    if job_success is not True:
        log_method_failure(start_time, city_data.city_folder_name, task_id, None, city_data.source_base_path, e_msg)

    return job_success

# @dask.delayed
# def _process_task(row, source_data_path, target_path):
#     task_id = row[0]
#     job_success = True
#     start_time = datetime.now()
#     city_folder_name = row[2]
#     tile_folder_name = row[3]
#     city_data = instantiate_city_data(city_folder_name, tile_folder_name, source_data_path, target_path)
#     all_valid = _verify_all_primary_paths(task_id, city_data)
#     if all_valid is not True:
#         return False
#     method = row[4].lower()
#     is_valid_method = _verify_method(task_id, method)
#     if is_valid_method is not True:
#         return False
#
#     # TODO - Add checks for prerequite data!!
#     e_msg = ''
#     return_code = _run_wall_height_aspect(task_id, city_data, method)
#     if return_code == 0:
#         return_code = _run_skyview_factor(task_id, city_data, method)
#         if return_code == 0:
#             return_code = _run_solweig(task_id, city_data, method)
#             if return_code != 0:
#                 job_success = False
#         else:
#             job_success = False
#     else:
#         job_success = False
#
#     if job_success is not True:
#         log_method_failure(start_time, city_data.city_folder_name, task_id, None, city_data.source_base_path, e_msg)
#
#     return job_success

# @dask.delayed
def _run_wall_height_aspect(task_id, city_data, method):
    return_code = 0
    if method in ['all', 'wall_height_aspect']:
        return_code = generate_wall_height_aspect(task_id, city_data)
    return return_code

def _run_skyview_factor(task_id, city_data, method):
    return_code = 0
    if method in ['all', 'skyview_factor']:
        return_code = generate_skyview_factor_files(task_id, city_data)
    return return_code

def _run_solweig(task_id, city_data, method):
    return_code = 0
    if method in ['all', 'solweig']:
        config_tcm_time_series_path = os.path.join(city_data.city_source_path, SOLWEIG_TIME_SERIES_CONFIG_FILE)
        return_code = 0
        with open(config_tcm_time_series_path, mode='r') as solweig_config_file:
            csv_reader = csv.reader(solweig_config_file)
            next(csv_reader, None)  # skip the headers

            for row in csv_reader:
                step = row[0]
                enabled = src_tools.toBool[row[1].lower()]
                if enabled:
                    met_file_name = row[2]
                    utc_offset = row[3]

                    return_code = generate_solweig(task_id, step, city_data, met_file_name, utc_offset)
                if return_code != 0:
                    break
    return return_code

def _construct_result_path(result_folder, met_file_path):
    result_path = os.path.join(os.path.abspath(result_folder), Path(met_file_path).stem)
    return result_path

def _verify_source_paths(source_data_path, target_path, config_processing_file_path):
    if verify_path(source_data_path) is False:
        log_other_failure(('Invalid path: %s' % source_data_path), '')
        raise Exception('Invalid path: %s' % source_data_path)
    if verify_path(target_path) is False:
        log_other_failure(('Invalid path: %s' % target_path), '')
        raise Exception('Invalid path: %s' % target_path)
    if verify_path(config_processing_file_path) is False:
        log_other_failure(('File does not exist: %s' % config_processing_file_path), '')
        raise Exception('Processing Registry file does not exist: %s' % config_processing_file_path)

def _verify_all_primary_paths(task_id, city_data):
    if verify_path(city_data.source_data_path) is False:
        log_other_failure(('Skipping task_id:%s due to invalid path: %s' % (task_id,city_data.source_data_path)), '')
        return False
    elif verify_path(city_data.source_met_files_path) is False:
        log_other_failure(('Skipping task_id:%s due to invalid path: %s' % (task_id,city_data.source_met_files_path)), '')
        return False
    elif verify_path(city_data.dem_path) is False:
        log_other_failure(('Skipping task_id:%s due to invalid path: %s' % (task_id,city_data.dem_path)), '')
        return False
    elif verify_path(city_data.dsm_path) is False:
        log_other_failure(('Skipping task_id:%s due to invalid path: %s' % (task_id,city_data.dsm_path)), '')
        return False
    elif verify_path(city_data.vegcanopy_path) is False:
        log_other_failure(('Skipping task_id:%s due to invalid path: %s' % (task_id,city_data.vegcanopy_path)), '')
        return False
    elif verify_path(city_data.landcover_path) is False:
        log_other_failure(('Skipping task_id:%s due to invalid path: %s' % (task_id,city_data.landcover_path)), '')
        return False
    else:
        return True

def _verify_method(task_id, method):
    if method not in METHODS:
        log_other_failure(('Skipping task_id:%s due to invalid method: %s' % (task_id,method)), '')
        return False
    else:
        return True