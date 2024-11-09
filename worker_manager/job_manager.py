import os
import shutil
import subprocess
import multiprocessing as mp
import warnings
import pandas as pd
import geopandas as gpd
import shapely
from shapely import wkt
import dask
from pathlib import Path
from worker_manager.graph_builder import get_aoi_fishnet, get_cif_features, get_aoi
from worker_manager.reporter import parse_row_results, report_results, write_raster_vrt_file_for_folder, \
    find_files_with_substring_in_name
from src.src_tools import create_folder, get_existing_tiles
from workers.worker_tools import get_application_path, clean_folder
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

    aoi_boundary, tile_side_meters, tile_buffer_meters, utc_offset = get_aoi(source_base_path, city_folder_name)

    out_list = []

    combined_results_df = pd.DataFrame(
        columns=['status', 'task_index', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code',
                 'start_time', 'run_duration_min'])
    combined_delays_passed = []

    city_data = CityData(city_folder_name, None, source_base_path, target_base_path)

    _write_config_files(source_base_path, target_base_path, city_folder_name)

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

            _write_tile_grid(existing_tiles, target_base_path, city_folder_name)

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
            _write_tile_grid(fishnet, target_base_path, city_folder_name)

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
            _write_vrt_files(city_data, source_base_path, target_base_path, city_folder_name)
        return_str = "Processing encountered no errors."
    else:
        return_str = 'Processing encountered errors. See log file.'

    return return_code, return_str


def _write_vrt_files(cd1, source_path, target__path, city_folder_name):
    city_data = CityData(city_folder_name, None, source_path, target__path)

    target_viewer_folder = os.path.join(city_data.target_base_path, city_data.folder_name_city_data, '.qgis_viewer',
                                        'vrt_files')
    create_folder(target_viewer_folder)
    clean_folder(target_viewer_folder)

    source_folder = city_data.source_city_data_path
    dem_file_name = city_data.dem_tif_filename
    dsm_file_name = city_data.dsm_tif_filename
    tree_canopy_file_name = city_data.tree_canopy_tif_filename
    lulc_file_name = city_data.lulc_tif_filename
    source_files = [dem_file_name, dsm_file_name, tree_canopy_file_name, lulc_file_name]
    if source_files:
        write_raster_vrt_file_for_folder(source_folder, source_files, target_viewer_folder)

    # loop through met folders under tcm_results
    for met_file in city_data.met_files:
        if met_file.get('filename') == CityData.method_name_era5_download:
            met_file_name = CityData.filename_era5
        else:
            met_file_name = met_file.get('filename')

        met_folder_name = Path(met_file_name).stem
        target_tcm_folder = str(os.path.join(city_data.target_tcm_results_path, met_folder_name))
        target_tcm_first_tile_folder = os.path.join(target_tcm_folder, 'tile_001')

        if os.path.exists(target_tcm_first_tile_folder):
            shadow_files = find_files_with_substring_in_name(target_tcm_first_tile_folder, 'Shadow_', '.tif')
            write_raster_vrt_file_for_folder(target_tcm_folder, shadow_files, target_viewer_folder)

            shadow_files = find_files_with_substring_in_name(target_tcm_first_tile_folder, 'Tmrt_', '.tif')
            write_raster_vrt_file_for_folder(target_tcm_folder, shadow_files, target_viewer_folder)


def any_value_matches_in_dict_list(dict_list, target_string):
    for dictionary in dict_list:
        if target_string in dictionary.values():
            return True
    return False

def _write_config_files(source_base_path, target_base_path, city_folder_name):
    source_yml_config = os.path.join(source_base_path, city_folder_name, CityData.filename_method_parameters_config)
    source_csv_config = os.path.join(source_base_path, city_folder_name, CityData.filename_umep_city_processing_config)
    target_yml_config = os.path.join(target_base_path, city_folder_name, CityData.folder_name_results, CityData.filename_method_parameters_config)
    target_csv_config = os.path.join(target_base_path, city_folder_name, CityData.folder_name_results, CityData.filename_umep_city_processing_config)
    shutil.copyfile(source_yml_config, target_yml_config)
    shutil.copyfile(source_csv_config, target_csv_config)


def _write_tile_grid(tile_grid, target_base_path, city_folder_name):
    if isinstance(tile_grid,dict):
        modified_tile_grid = pd.DataFrame(columns=['id', 'geometry'])
        for key, value in tile_grid.items():
            poly = wkt.loads(str(value[0]))
            modified_tile_grid.loc[len(modified_tile_grid)] = [key, poly]
    else:
        # TODO figure out how to retain the index
        if 'fishnet_geometry' in tile_grid.columns:
            modified_tile_grid = tile_grid.drop(columns='fishnet_geometry', axis=1)
        else:
            modified_tile_grid = tile_grid
    projected_gdf = gpd.GeoDataFrame(modified_tile_grid, crs='EPSG:4326')

    target_file_name = 'tile_grid.geojson'
    target_path = str(os.path.join(target_base_path, city_folder_name, CityData.folder_name_results,
                            CityData.folder_name_preprocessed_data))
    create_folder(target_path)
    file_path = os.path.join(target_path, target_file_name)

    projected_gdf.to_file(file_path, driver='GeoJSON')


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

