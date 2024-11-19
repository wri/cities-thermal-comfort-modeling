import os
import subprocess
import multiprocessing as mp
import warnings
import pandas as pd
import shapely
import dask

from worker_manager.ancillary_files import write_config_files, write_tile_grid, write_vrt_files
from worker_manager.graph_builder import get_aoi_fishnet, get_cif_features, get_aoi
from worker_manager.reporter import parse_row_results, report_results
from src.src_tools import create_folder, get_existing_tiles
from workers.worker_tools import get_application_path
from workers.city_data import CityData

warnings.filterwarnings('ignore')
import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

dask.config.set({'logging.distributed': 'warning'})

MET_PROCESSING_MODULE_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'meteorological_processor.py'))
TILE_PROCESSING_MODULE_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'tile_processor.py'))

DEBUG=False

def start_jobs(source_base_path, target_base_path, city_folder_name, processing_config_df):
    _start_logging(target_base_path, city_folder_name)

    aoi_boundary, tile_side_meters, tile_buffer_meters, utc_offset, crs_str = get_aoi(source_base_path, city_folder_name)

    out_list = []

    combined_results_df = pd.DataFrame(
        columns=['status', 'task_index', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code',
                 'start_time', 'run_duration_min'])
    combined_delays_passed = []

    city_data = CityData(city_folder_name, None, source_base_path, target_base_path)

    write_config_files(source_base_path, target_base_path, city_folder_name)

    has_era_met_download = any_value_matches_in_dict_list(city_data.met_files, CityData.method_name_era5_download)
    # meteorological data
    if has_era_met_download:
        sampling_local_hours = city_data.sampling_local_hours
        proc_array = _construct_met_proc_array(-1, source_base_path, city_folder_name, aoi_boundary, utc_offset, sampling_local_hours)
        delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
        met_futures = []
        met_futures.append(delay_tile_array)
        met_delays_all_passed, met_results_df = _process_rows(met_futures)
        out_list.extend(met_results_df)

        combined_results_df = pd.concat([combined_results_df, met_results_df])
        combined_delays_passed.append(met_delays_all_passed)

    # build plugin graph
    enabled_processing_tasks_df = processing_config_df[(processing_config_df['enabled'])]
    
    futures = []
    for index, config_row in enabled_processing_tasks_df.iterrows():
        task_index = index
        task_method = config_row.method

        source_city_path = str(os.path.join(source_base_path, city_folder_name))
        custom_file_names, has_custom_features, cif_features = get_cif_features(source_city_path)
    
        if has_custom_features:
            start_tile_id = config_row.start_tile_id
            end_tile_id = config_row.end_tile_id
            existing_tiles = get_existing_tiles(source_city_path, custom_file_names, start_tile_id, end_tile_id)

            write_tile_grid(existing_tiles, target_base_path, city_folder_name)

            print(f'\nProcessing over {len(existing_tiles)} existing tiles..')
            for tile_folder_name, tile_dimensions in existing_tiles.items():
                tile_boundary = tile_dimensions[0]
                tile_resolution = tile_dimensions[1]

                proc_array = _construct_tile_proc_array(task_index, task_method, source_base_path, target_base_path,
                                                        city_folder_name, tile_folder_name, cif_features,
                                                        tile_boundary, tile_resolution, utc_offset)
                delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
                futures.append(delay_tile_array)
        else:
            fishnet = get_aoi_fishnet(aoi_boundary, tile_side_meters, tile_buffer_meters)
            write_tile_grid(fishnet, target_base_path, city_folder_name)

            print(f'\nCreating data for {fishnet.geometry.size} new tiles..')
            for index, cell in fishnet.iterrows():
                cell_bounds = cell.geometry.bounds
                tile_boundary = str(shapely.box(cell_bounds[0], cell_bounds[1], cell_bounds[2], cell_bounds[3]))

                tile_id = str(index + 1).zfill(3)
                tile_folder_name = f'tile_{tile_id}'

                proc_array = _construct_tile_proc_array(task_index, task_method, source_base_path, target_base_path,
                                                        city_folder_name, tile_folder_name, cif_features,
                                                        tile_boundary, None, utc_offset)
                delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
                futures.append(delay_tile_array)

    
    # TODO consider processing every nth tile and return just those results
    delays_all_passed, results_df = _process_rows(futures)

    # Combine processing return values
    combined_results_df = pd.concat([combined_results_df, results_df])
    combined_delays_passed.append(delays_all_passed)
    
    # Write run_report
    report_file_path = report_results(enabled_processing_tasks_df, combined_results_df, target_base_path, city_folder_name)
    print(f'\nRun report written to {report_file_path}\n')

    return_code = 0 if all(combined_delays_passed) else 1

    if return_code == 0:
        if DEBUG:
            write_vrt_files(city_data, crs_str)
        return_str = "Processing encountered no errors."
    else:
        return_str = 'Processing encountered errors. See log file.'

    return return_code, return_str


def any_value_matches_in_dict_list(dict_list, target_string):
    for dictionary in dict_list:
        if target_string in dictionary.values():
            return True
    return False


def _construct_met_proc_array(task_index, source_base_path, city_folder_name, aoi_boundary, utc_offset, sampling_local_hours):
    proc_array = ['python', MET_PROCESSING_MODULE_PATH,
                  f'--task_index={task_index}',
                  f'--source_base_path={source_base_path}',
                  f'--city_folder_name={city_folder_name}',
                  f'--aoi_boundary={str(aoi_boundary)}',
                  f'--utc_offset={utc_offset}',
                  f'--sampling_local_hours={sampling_local_hours}'
                  ]

    return proc_array


def _construct_tile_proc_array(task_index, task_method, source_base_path, target_base_path, city_folder_name,
                               tile_folder_name, cif_features, tile_boundary, tile_resolution, utc_offset):
    proc_array = ['python', TILE_PROCESSING_MODULE_PATH,
                  f'--task_index={task_index}',
                  f'--task_method={task_method}',
                  f'--source_base_path={source_base_path}',
                  f'--target_base_path={target_base_path}',
                  f'--city_folder_name={city_folder_name}',
                  f'--tile_folder_name={tile_folder_name}',
                  f'--cif_features={cif_features}',
                  f'--tile_boundary={tile_boundary}',
                  f'--tile_resolution={tile_resolution}',
                  f'--utc_offset={utc_offset}'
                  ]

    return proc_array


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


def _process_rows(futures):
    if futures:
        # TODO chunk size??
        from dask.distributed import Client
        with Client(n_workers=int(mp.cpu_count() - 1),
                    threads_per_worker=1,
                    processes=False,
                    memory_limit='2GB',
                    asynchronous=False
                    ) as client:

            msg = f'*************See processing dashboard at {client.dashboard_link}'
            print(msg)
            _log_info_msg(msg)

            # TODO implement progress bar
            dc = dask.compute(*futures)

        all_passed, results_df, failed_task_ids, failed_task_details =parse_row_results(dc)

        if not all_passed:
            task_str = ','.join(map(str,failed_task_ids))
            count = len(failed_task_ids)
            msg =  f'FAILURE: There were {count} processing failures for tasks indices: ({task_str})'
            print(msg)

            for failed_run in failed_task_details:
                _log_failure(failed_run, '')

        return all_passed, results_df
    else:
        return True, None

def _log_info_msg(message):
    logging.info(message)

def _log_failure(message, e_msg):
    print('Failure. See log file.')
    logging.critical(f"**** FAILED execution with '{message}' ({e_msg})")

