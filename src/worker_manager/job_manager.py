import os
import subprocess
import multiprocessing as mp
import warnings
import pandas as pd
import shapely
import dask
from shapely.lib import envelope

from src.constants import SRC_DIR, METHOD_TRIGGER_ERA5_DOWNLOAD
from src.worker_manager.ancillary_files import write_tile_grid, write_qgis_files
from src.worker_manager.graph_builder import get_aoi_fishnet, get_aoi
from src.workers.logger_tools import setup_logger, write_log_message
from src.worker_manager.reporter import parse_row_results, report_results
from src.worker_manager.tools import get_existing_tile_metrics
from src.workers.city_data import CityData

warnings.filterwarnings('ignore')
dask.config.set({'logging.distributed': 'warning'})

MET_PROCESSING_MODULE_PATH = os.path.abspath(
    os.path.join(SRC_DIR, 'workers', 'worker_meteorological_processor.py'))
TILE_PROCESSING_MODULE_PATH = os.path.abspath(os.path.join(SRC_DIR, 'workers', 'worker_tile_processor.py'))


def _add_buffer_hack(tile_boundary):
    # TODO Remove after fixing CIF-321
    # from shapely.geometry import Polygon
    # Define the outer envelop of the tile boundary

    tile_boundary_out = envelope(shapely.buffer(shapely.from_wkt(tile_boundary), 0.00005))

    return str(tile_boundary_out)


def start_jobs(source_base_path, target_base_path, city_folder_name):
    non_tiled_city_data = CityData(city_folder_name, None, source_base_path, target_base_path)
    source_city_path = str(os.path.join(source_base_path, city_folder_name))
    custom_primary_filenames = non_tiled_city_data.custom_primary_filenames
    cif_primary_features = non_tiled_city_data.cif_primary_feature_list
    ctcm_intermediate_features = non_tiled_city_data.ctcm_intermediate_list

    logger = setup_logger(non_tiled_city_data.target_manager_log_path)
    write_log_message('Starting jobs', __file__, logger)

    out_list = []

    aoi_boundary, tile_side_meters, tile_buffer_meters, utc_offset, crs_str = get_aoi(source_base_path,
                                                                                      city_folder_name)
    combined_results_df = pd.DataFrame(
        columns=['status', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code',
                 'start_time', 'run_duration_min'])
    combined_delays_passed = []

    has_era_met_download = any_value_matches_in_dict_list(non_tiled_city_data.met_filenames, METHOD_TRIGGER_ERA5_DOWNLOAD)
    # meteorological data
    if has_era_met_download:
        write_log_message('Retrieving ERA meteorological data', __file__, logger)
        sampling_local_hours = non_tiled_city_data.sampling_local_hours
        proc_array = _construct_met_proc_array(-1, target_base_path, city_folder_name, aoi_boundary, utc_offset,
                                               sampling_local_hours)
        delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
        met_futures = []
        met_futures.append(delay_tile_array)
        met_delays_all_passed, met_results_df = _process_rows(met_futures, logger)
        out_list.extend(met_results_df)

        combined_results_df = pd.concat([combined_results_df, met_results_df])
        combined_delays_passed.append(met_delays_all_passed)


    futures = []
    number_of_tiles = 1000  # Initialize as large number
    task_method = non_tiled_city_data.new_task_method

    # Retrieve CIF data
    if custom_primary_filenames:
        existing_tiles = get_existing_tile_metrics(source_city_path, custom_primary_filenames, project_to_wgs84=True)
        tile_unique_values = existing_tiles[['tile_name', 'boundary', 'avg_res', 'source_crs']].drop_duplicates()
        number_of_tiles = tile_unique_values.shape[0]

        # TODO update after fixing CIF-321
        source_crs = 'epsg:4326'
        # source_crs = tile_unique_values['source_crs'][0]

        write_tile_grid(tile_unique_values, source_crs, non_tiled_city_data.target_qgis_viewer_path)

        print(f'\nProcessing over {len(tile_unique_values)} existing tiles..')
        for index, tile_metrics in tile_unique_values.iterrows():
            tile_folder_name = tile_metrics['tile_name']
            tile_boundary = tile_metrics['boundary']
            tile_resolution = tile_metrics['avg_res']

            proc_array = _construct_tile_proc_array(task_method, source_base_path, target_base_path,
                                                    city_folder_name, tile_folder_name, cif_primary_features,
                                                    ctcm_intermediate_features, tile_boundary, tile_resolution,
                                                    utc_offset)

            write_log_message(f'Staging: {proc_array}', __file__, logger)

            delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            futures.append(delay_tile_array)
    else:
        fishnet = get_aoi_fishnet(aoi_boundary, tile_side_meters, tile_buffer_meters)
        number_of_tiles = fishnet.shape[0]

        # TODO update after fixing CIF-321
        source_crs = 'epsg:4326'
        write_tile_grid(fishnet, source_crs, non_tiled_city_data.target_qgis_viewer_path)

        print(f'\nCreating data for {fishnet.geometry.size} new tiles..')
        for tile_index, cell in fishnet.iterrows():
            cell_bounds = cell.geometry.bounds
            tile_boundary = str(shapely.box(cell_bounds[0], cell_bounds[1], cell_bounds[2], cell_bounds[3]))

            tile_boundary = _add_buffer_hack(tile_boundary)

            tile_id = str(tile_index + 1).zfill(3)
            tile_folder_name = f'tile_{tile_id}'

            proc_array = _construct_tile_proc_array(task_method, source_base_path, target_base_path,
                                                    city_folder_name, tile_folder_name, cif_primary_features,
                                                    ctcm_intermediate_features, tile_boundary, None,
                                                    utc_offset)

            write_log_message(f'Staging: {proc_array}', __file__, logger)

            delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
            futures.append(delay_tile_array)

    write_log_message('Starting model processing', __file__, logger)
    delays_all_passed, results_df = _process_rows(futures, number_of_tiles, logger)

    # Combine processing return values
    combined_results_df = pd.concat([combined_results_df, results_df])
    combined_delays_passed.append(delays_all_passed)

    # Write run_report
    report_file_path = report_results(task_method, combined_results_df, non_tiled_city_data.target_log_path,
                                      city_folder_name)
    print(f'\nRun report written to {report_file_path}\n')

    return_code = 0 if all(combined_delays_passed) or delays_all_passed else 1

    if return_code == 0 and delays_all_passed:
        write_log_message('Building QGIS viewer objects', __file__, logger)
        write_qgis_files(non_tiled_city_data, crs_str)
        return_str = "Processing encountered no errors."
    else:
        return_str = 'Processing encountered errors. See log file.'

    write_log_message('Completing manager execution', __file__, logger)

    return return_code, return_str


def any_value_matches_in_dict_list(dict_list, target_string):
    for dictionary in dict_list:
        if target_string in dictionary.values():
            return True
    return False


def _construct_met_proc_array(target_base_path, city_folder_name, aoi_boundary, utc_offset,
                              sampling_local_hours):
    proc_array = ['python', MET_PROCESSING_MODULE_PATH,
                  f'--target_base_path={target_base_path}',
                  f'--city_folder_name={city_folder_name}',
                  f'--aoi_boundary={str(aoi_boundary)}',
                  f'--utc_offset={utc_offset}',
                  f'--sampling_local_hours={sampling_local_hours}'
                  ]

    return proc_array


def _construct_tile_proc_array(task_method, source_base_path, target_base_path, city_folder_name,
                               tile_folder_name, cif_primary_features, ctcm_intermediate_features,
                               tile_boundary, tile_resolution, utc_offset):
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
                  f'--tile_resolution={tile_resolution}',
                  f'--utc_offset={utc_offset}'
                  ]
    return proc_array


def _process_rows(futures, number_of_tiles, logger):
    # See https://docs.dask.org/en/stable/deploying-python.html
    # https://blog.dask.org/2021/11/02/choosing-dask-chunk-sizes#what-to-watch-for-on-the-dashboard
    if futures:
        available_cpu_count = int(mp.cpu_count() - 1)
        num_workers = number_of_tiles+1 if number_of_tiles < available_cpu_count else available_cpu_count

        from dask.distributed import Client
        with Client(n_workers=num_workers,
                    threads_per_worker=1,
                    processes=False,
                    memory_limit='2GB',
                    asynchronous=False
                    ) as client:

            msg = f'See processing dashboard at {client.dashboard_link}'
            write_log_message(msg, __file__, logger)

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
            write_log_message(msg, __file__, logger)
            print(msg)

            for failed_run in failed_task_details:
                write_log_message(failed_run, __file__, logger)

        return all_passed, results_df
    else:
        return True, None

