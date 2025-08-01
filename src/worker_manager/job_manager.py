import os
import shutil
import subprocess
import multiprocessing as mp
import warnings
import pandas as pd
import shapely
import dask

from src.constants import SRC_DIR, FILENAME_ERA5_UMEP
from src.data_validation.manager import print_invalids
from src.data_validation.meteorological_data_validator import evaluate_meteorological_data
from src.worker_manager.ancillary_files import write_tile_grid, write_qgis_files
from src.worker_manager.graph_builder import get_aoi_fishnet, get_aoi_from_config
from src.workers.logger_tools import setup_logger, log_general_file_message
from src.worker_manager.reporter import parse_row_results, report_results
from src.workers.model_umep.worker_umep_met_processor import get_umep_met_data
from src.workers.model_upenn.worker_upenn_met_processor import get_upenn_met_data
from src.workers.worker_tools import create_folder

warnings.filterwarnings('ignore')
dask.config.set({'logging.distributed': 'warning'})

TILE_PROCESSING_MODULE_PATH = os.path.abspath(os.path.join(SRC_DIR, 'workers', 'worker_tile_processor.py'))


def start_jobs(non_tiled_city_data, existing_tiles_metrics):
    source_base_path = non_tiled_city_data.source_base_path
    target_base_path = non_tiled_city_data.target_base_path
    city_folder_name = non_tiled_city_data.folder_name_city_data

    custom_primary_filenames = non_tiled_city_data.custom_primary_filenames
    cif_primary_features = non_tiled_city_data.cif_primary_feature_list
    ctcm_intermediate_features = non_tiled_city_data.ctcm_intermediate_list

    logger = setup_logger(non_tiled_city_data.target_manager_log_path)
    log_general_file_message('Starting jobs', __file__, logger)

    aoi_boundary_polygon, tile_side_meters, tile_buffer_meters, utc_offset, config_crs = get_aoi_from_config(non_tiled_city_data)

    combined_results_df = pd.DataFrame(
        columns=['status', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code',
                 'start_time', 'run_duration_min'])
    combined_delays_passed = []

    # meteorological data
    if non_tiled_city_data.has_era_met_download:
        log_general_file_message('Retrieving ERA meteorological data', __file__, logger)
        sampling_local_hours = non_tiled_city_data.sampling_local_hours

        target_met_files_path = non_tiled_city_data.target_met_files_path
        if non_tiled_city_data.new_task_method == 'umep_solweig':
            return_code = get_umep_met_data(target_met_files_path, aoi_boundary_polygon, utc_offset, sampling_local_hours)
        else:
            return_code = get_upenn_met_data(target_met_files_path, aoi_boundary_polygon, utc_offset,
                                            sampling_local_hours)
        if return_code != 0:
            print("Stopping. Failed downloading ERA5 meteorological data")
            exit(1)

    # Transfer custom met files to target
    _transfer_custom_met_files(non_tiled_city_data)

    if non_tiled_city_data.new_task_method != 'upenn_model':
        invalids = evaluate_meteorological_data(non_tiled_city_data, in_target_folder=True)
        if invalids:
            print_invalids(invalids)
            print("Stopping. Identified invalid values in meteorological files(s)")
            exit(1)


    futures = []
    task_method = non_tiled_city_data.new_task_method

    # Retrieve CIF data
    if custom_primary_filenames:
        tile_unique_values = existing_tiles_metrics[['tile_name', 'boundary', 'avg_res', 'source_crs']].drop_duplicates()
        number_of_tiles = len(tile_unique_values.tile_name)

        # TODO  Assume customer files are always in UTM
        utm_crs = tile_unique_values['source_crs'].values[0]

        write_tile_grid(tile_unique_values, utm_crs, non_tiled_city_data.target_qgis_data_path, 'tile_grid')

        print(f'\nProcessing over {len(tile_unique_values)} existing tiles..')
        for index, tile_metrics in tile_unique_values.iterrows():
            tile_folder_name = tile_metrics['tile_name']
            tile_boundary = tile_metrics['boundary']
            tile_resolution = tile_metrics['avg_res']

            proc_array = _construct_tile_proc_array(task_method, source_base_path, target_base_path,
                                                    city_folder_name, tile_folder_name, cif_primary_features,
                                                    ctcm_intermediate_features, tile_boundary, utm_crs, tile_resolution,
                                                    utc_offset)

            log_general_file_message(f'Staging: {proc_array}', __file__, logger)

            delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            futures.append(delay_tile_array)
    else:
        tile_grid, unbuffered_tile_grid = get_aoi_fishnet(aoi_boundary_polygon, tile_side_meters, tile_buffer_meters, config_crs)
        number_of_tiles = tile_grid.shape[0]
        utm_crs = tile_grid.crs.srs

        write_tile_grid(tile_grid, utm_crs, non_tiled_city_data.target_qgis_data_path, 'tile_grid')
        if unbuffered_tile_grid is not None:
            write_tile_grid(unbuffered_tile_grid, utm_crs, non_tiled_city_data.target_qgis_data_path, 'unbuffered_tile_grid')

        print(f'\nCreating data for {tile_grid.geometry.size} new tiles..')
        for tile_index, cell in tile_grid.iterrows():
            cell_bounds = cell.geometry.bounds
            tile_boundary = str(shapely.box(cell_bounds[0], cell_bounds[1], cell_bounds[2], cell_bounds[3]))

            tile_id = str(tile_index + 1).zfill(3)
            tile_folder_name = f'tile_{tile_id}'

            proc_array = _construct_tile_proc_array(task_method, source_base_path, target_base_path,
                                                    city_folder_name, tile_folder_name, cif_primary_features,
                                                    ctcm_intermediate_features, tile_boundary, utm_crs, None,
                                                    utc_offset)

            log_general_file_message(f'Staging: {proc_array}', __file__, logger)

            delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            futures.append(delay_tile_array)

    log_general_file_message('Starting model processing', __file__, logger)
    number_number_of_tiles = number_of_tiles
    delays_all_passed, results_df = _process_rows(futures, number_number_of_tiles, logger)

    # Combine processing return values
    combined_results_df = pd.concat([combined_results_df, results_df])
    combined_delays_passed.append(delays_all_passed)

    # Write run_report
    report_file_path = report_results(task_method, combined_results_df, non_tiled_city_data.target_log_path,
                                      city_folder_name)
    print(f'\nRun report written to {report_file_path}\n')

    return_code = 0 if all(combined_delays_passed) or delays_all_passed else 1

    if return_code == 0 and delays_all_passed:
        log_general_file_message('Building QGIS viewer objects', __file__, logger)
        write_qgis_files(non_tiled_city_data, utm_crs)
        return_str = "Processing encountered no errors."
    else:
        return_str = 'Processing encountered errors. See log file.'

    log_general_file_message('Completing manager execution', __file__, logger)

    return return_code, return_str


def _transfer_custom_met_files(non_tiled_city_data):
    create_folder(non_tiled_city_data.target_met_files_path)
    for met_file in non_tiled_city_data.met_filenames:
        if not(non_tiled_city_data.has_era_met_download):
            source_path = os.path.join(non_tiled_city_data.source_met_files_path, met_file['filename'])
            target_path = os.path.join(non_tiled_city_data.target_met_files_path, met_file['filename'])
            shutil.copyfile(source_path, target_path)


def _construct_tile_proc_array(task_method, source_base_path, target_base_path, city_folder_name,
                               tile_folder_name, cif_primary_features, ctcm_intermediate_features,
                               tile_boundary, crs, tile_resolution, utc_offset):
    if cif_primary_features:
        cif_features = ','.join(cif_primary_features)
    else:
        cif_features = None

    if ctcm_intermediate_features:
        ctcm_features = ','.join(ctcm_intermediate_features)
    else:
        ctcm_features = None

    proc_array = ['python', TILE_PROCESSING_MODULE_PATH,
                  f'--task_method={task_method}',
                  f'--source_base_path={source_base_path}',
                  f'--target_base_path={target_base_path}',
                  f'--city_folder_name={city_folder_name}',
                  f'--tile_folder_name={tile_folder_name}',
                  f'--cif_primary_features={cif_features}',
                  f'--ctcm_intermediate_features={ctcm_features}',
                  f'--tile_boundary={tile_boundary}',
                  f'--crs={crs}',
                  f'--tile_resolution={tile_resolution}',
                  f'--utc_offset={utc_offset}'
                  ]
    return proc_array


def _process_rows(futures, number_of_units, logger):
    # See https://docs.dask.org/en/stable/deploying-python.html
    # https://blog.dask.org/2021/11/02/choosing-dask-chunk-sizes#what-to-watch-for-on-the-dashboard
    if futures:
        available_cpu_count = int(mp.cpu_count() - 1)
        num_workers = number_of_units + 1 if number_of_units < available_cpu_count else available_cpu_count

        from dask.distributed import Client
        with Client(n_workers=num_workers,
                    threads_per_worker=1,
                    processes=False,
                    memory_limit='2GB',
                    asynchronous=False
                    ) as client:

            msg = f'See processing dashboard at {client.dashboard_link}'
            log_general_file_message(msg, __file__, logger)

            # TODO implement progress bar
            try:
                dc = dask.compute(*futures)
            except Exception as e_msg:
                print(f'Dask failure for {futures}')

        all_passed, results_df, failed_task_ids, failed_task_details = parse_row_results(dc)

        if not all_passed:
            task_str = ','.join(map(str, failed_task_ids))
            count = len(failed_task_ids)
            msg = f'FAILURE: There were {count} processing failures for tasks indices: ({task_str})'
            log_general_file_message(msg, __file__, logger)
            print(msg)

            for failed_run in failed_task_details:
                log_general_file_message(failed_run, __file__, logger)

        return all_passed, results_df
    else:
        return True, None

