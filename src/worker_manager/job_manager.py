import json
import os
import shutil
import subprocess
import multiprocessing as mp
import psutil
import warnings
from datetime import datetime
import asyncio
import pandas as pd
import shapely
import dask
from dask.distributed import Client, LocalCluster
from math import floor

from city_metrix.constants import GEOJSON_FILE_EXTENSION
from city_metrix.metrix_dao import get_city_boundaries, write_layer, get_bucket_name_from_s3_uri, \
    read_geojson_from_cache
from shapely import wkt

from src.constants import SRC_DIR, FILENAME_ERA5_UMEP, FILENAME_ERA5_UPENN, PRIOR_5_YEAR_KEYWORD, WGS_CRS, \
    FILENAME_UNBUFFERED_TILE_GRID, FILENAME_TILE_GRID, FILENAME_URBAN_EXTENT_BOUNDARY, S3_PUBLICATION_BUCKET, \
    FOLDER_NAME_PRIMARY_MET_FILES, FOLDER_NAME_QGIS_DATA
from src.data_validation.manager import print_invalids
from src.data_validation.meteorological_data_validator import evaluate_meteorological_umep_data
from src.worker_manager.ancillary_files import write_tile_grid, write_qgis_files
from src.worker_manager.graph_builder import get_aoi_fishnet, get_grid_dimensions
from src.worker_manager.tools import extract_function_and_params, get_s3_scenario_location, list_s3_subfolders, \
    download_s3_files
from src.workers.logger_tools import setup_logger, log_general_file_message
from src.worker_manager.reporter import parse_row_results, report_results
from src.workers.model_umep.worker_umep_met_processor import get_umep_met_data
from src.workers.model_upenn.worker_upenn_met_processor import get_upenn_met_data
from src.workers.worker_dao import cache_metadata_files, get_scenario_folder_key
from src.workers.worker_tile_processor import process_tile
from src.workers.worker_tools import create_folder

warnings.filterwarnings('ignore')
dask.config.set({'logging.distributed': 'warning'})

TILE_PROCESSING_MODULE_PATH = os.path.abspath(os.path.join(SRC_DIR, 'workers', 'worker_tile_processor.py'))

USABLE_CPU_FRACTION = 0.85 # Values was selected to keep memory below 90%, assuming 600m-width, 600m-buffer tiles


def start_jobs(non_tiled_city_data, existing_tiles_metrics, has_appendable_cache, processing_option):
    city_json_str = non_tiled_city_data.city_json_str
    source_base_path = non_tiled_city_data.source_base_path
    target_base_path = non_tiled_city_data.target_base_path
    city_folder_name = non_tiled_city_data.folder_name_city_data

    custom_primary_filenames = non_tiled_city_data.custom_primary_filenames
    cif_primary_features = non_tiled_city_data.cif_primary_feature_list
    ctcm_intermediate_features = non_tiled_city_data.ctcm_intermediate_list

    processing_method = non_tiled_city_data.processing_method

    utm_grid_bbox, tile_side_meters, tile_buffer_meters, seasonal_utc_offset, utm_grid_crs = (
        get_grid_dimensions(non_tiled_city_data))

    combined_results_df = pd.DataFrame(
        columns=['status', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code',
                 'start_time', 'run_duration_min'])
    combined_delays_passed = []

    if processing_option == 'run_pipeline':
        logger = setup_logger(non_tiled_city_data.target_manager_log_path)
        log_general_file_message('Starting jobs', __file__, logger)

    num_workers = floor(USABLE_CPU_FRACTION * mp.cpu_count())
    print(f"\nNumber of processing workers to be launched in Dask: {num_workers}")

    # Retrieve missing CIF data
    tile_count = 0
    tasks = []
    if custom_primary_filenames:
        # TODO Inactivating this first option except for run_pipeline while team focuses on baseline demo
        if processing_option != 'run_pipeline':
            return 0, ''

        no_layer_atts = existing_tiles_metrics[['tile_name', 'boundary', 'avg_res', 'custom_tile_crs']]
        tile_unique_values = no_layer_atts.drop_duplicates().reset_index(drop=True)

        # TODO  Assume custom files are always in UTM and all tiles have the same UTM
        custom_source_crs = tile_unique_values['custom_tile_crs'].values[0]

        write_tile_grid(tile_unique_values, non_tiled_city_data.target_qgis_data_path, FILENAME_TILE_GRID)

        if processing_method == 'grid_only':
            print(f'Stopping execution after locally writing grid files to {non_tiled_city_data.target_qgis_data_path}.')
            return 0, ''

        # Cache currently-available metadata to s3
        if non_tiled_city_data.city_json_str is not None and non_tiled_city_data.publishing_target in ('s3', 'both'):
            cache_metadata_files(non_tiled_city_data)

        tile_count = tile_unique_values.shape[0]
        print(f'\nProcessing over {len(tile_unique_values)} existing tiles..')
        for index, tile_metrics in tile_unique_values.iterrows():
            tile_folder_name = tile_metrics['tile_name']
            tile_boundary = tile_metrics['boundary']
            tile_resolution = tile_metrics['avg_res']

            proc_array = _construct_tile_proc_array(city_json_str, processing_method, source_base_path, target_base_path,
                                                    city_folder_name, tile_folder_name, cif_primary_features,
                                                    ctcm_intermediate_features, tile_boundary, tile_resolution,
                                                    seasonal_utc_offset, custom_source_crs)

            log_general_file_message(f'Staging: {proc_array}', __file__, logger)

            if processing_method == 'upenn_model':
                tasks.append((process_tile, proc_array))
            else:
                tasks.append((subprocess.run, proc_array))
                # delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
    else:
        if has_appendable_cache:
            tile_grid, unbuffered_tile_grid = _transfer_and_read_cached_qgis_files(non_tiled_city_data)
        else:
            tile_grid, unbuffered_tile_grid = (
                get_aoi_fishnet(utm_grid_bbox, tile_side_meters, tile_buffer_meters, utm_grid_crs))
            if processing_option == 'run_pipeline':
                # save the newly-created fishnet grid
                write_tile_grid(tile_grid, non_tiled_city_data.target_qgis_data_path, FILENAME_TILE_GRID)
                if unbuffered_tile_grid is not None:
                    write_tile_grid(unbuffered_tile_grid, non_tiled_city_data.target_qgis_data_path, FILENAME_UNBUFFERED_TILE_GRID)

        # Write urban_extent polygon to disk and thin tiles to internal and aoi-overlapping
        internal_tile_count = None
        internal_aoi_tile_count = None
        if non_tiled_city_data.city_json_str is not None:
            urban_extent_polygon = _get_and_write_city_boundary(non_tiled_city_data, unbuffered_tile_grid)
            tile_grid, internal_tile_count, internal_aoi_tile_count = _thin_city_tile_grid(non_tiled_city_data, unbuffered_tile_grid, tile_grid, urban_extent_polygon)

            if len(tile_grid) == 0 and processing_method != 'grid_only':
                print(f'Stopping execution since there are no tiles to further process.')
                return 0, ''
        else:
            urban_extent_polygon = None

        tile_count = tile_grid.shape[0]
        city_extent_count = unbuffered_tile_grid.shape[0]
        msg_str = f'\nProcessing will/would create {tile_count} new tiles'
        if internal_aoi_tile_count is not None:
            msg_str += f", of {internal_aoi_tile_count} tiles within the sub-area"
        if internal_tile_count is not None:
            msg_str += f", of {internal_tile_count} tiles internal to the city polygon"
        msg_str += f", of {city_extent_count} tiles within the full city grid."
        print(msg_str)

        # Do not list unprocessed tiles at this point and instead report at completion of processing
        if processing_method == 'grid_only' or processing_option == 'pre_check':
            if processing_method == 'grid_only':
                print(f'Stopping execution after locally writing grid files to {non_tiled_city_data.target_qgis_data_path}.')

            if non_tiled_city_data.city_json_str is not None:
                _print_unprocessed_internal_tile_names(non_tiled_city_data, unbuffered_tile_grid, urban_extent_polygon)
            return 0, ''

        # Cache currently-available metadata to s3
        if (has_appendable_cache is False and non_tiled_city_data.city_json_str is not None and
                non_tiled_city_data.publishing_target in ('s3', 'both')):
            cache_metadata_files(non_tiled_city_data)

        tile_grid.reset_index(drop=True, inplace=True)

        for tile_index, cell in tile_grid.iterrows():
            cell_bounds = cell.geometry.bounds
            tile_boundary = str(shapely.box(cell_bounds[0], cell_bounds[1], cell_bounds[2], cell_bounds[3]))
            tile_folder_name = cell.tile_name

            proc_array = _construct_tile_proc_array(city_json_str, processing_method, source_base_path, target_base_path,
                                                    city_folder_name, tile_folder_name, cif_primary_features,
                                                    ctcm_intermediate_features, tile_boundary, None,
                                                    seasonal_utc_offset, utm_grid_crs)

            log_general_file_message(f'Staging: {proc_array}', __file__, logger)

            if processing_method == 'upenn_model':
                tasks.append((process_tile, proc_array))
            else:
                tasks.append((subprocess.run, proc_array))
                # delay_tile_array = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)

    # meteorological data
    if processing_option == 'run_pipeline' and has_appendable_cache is False and non_tiled_city_data.has_era_met_download:
        import geopandas as gpd
        # convert to geographic coordinates for call to ERA5
        gdf = gpd.GeoDataFrame({'geometry': [utm_grid_bbox]}, crs=utm_grid_crs)
        geog_gdf = gdf.to_crs(WGS_CRS)
        geographic_grid_bbox_wkt = geog_gdf.geometry[0].wkt
        geographic_grid_bbox = wkt.loads(geographic_grid_bbox_wkt)

        log_general_file_message('Retrieving ERA meteorological data', __file__, logger)
        sampling_local_hours = non_tiled_city_data.sampling_local_hours

        start_date, end_date = _determine_era5_date_range(non_tiled_city_data.era5_date_range)

        target_met_files_path = non_tiled_city_data.target_met_files_path
        if processing_method == 'umep_solweig':
            return_code = get_umep_met_data(target_met_files_path, geographic_grid_bbox,
                                            start_date, end_date, seasonal_utc_offset, sampling_local_hours)
        else:
            return_code = get_upenn_met_data(target_met_files_path, geographic_grid_bbox,
                                             start_date, end_date, seasonal_utc_offset, sampling_local_hours)
        if return_code != 0:
            print("Stopping. Failed downloading ERA5 meteorological data")
            exit(1)

    # Transfer custom met files to target
    if processing_option == 'run_pipeline':
        if has_appendable_cache:
            _transfer_cached_met_files(non_tiled_city_data)
        else:
            if processing_method != 'grid_only':
                _transfer_custom_met_files(non_tiled_city_data)

        if processing_method == 'umep_solweig':
            invalids = evaluate_meteorological_umep_data(non_tiled_city_data, in_target_folder=True)
            if invalids:
                print_invalids(invalids)
                print("Stopping. Identified invalid values in meteorological files(s)")
                exit(1)

    print(f'\nCreating data for {tile_count} new tiles..')
    log_general_file_message('Starting model processing', __file__, logger)
    delays_all_passed, results_df = _process_tasks(tasks, logger)

    # Combine processing return values
    combined_results_df = pd.concat([combined_results_df, results_df])
    combined_delays_passed.append(delays_all_passed)

    # Write run_report
    report_file_path = report_results(processing_method, combined_results_df, non_tiled_city_data.target_log_path,
                                      city_folder_name)
    print(f'\nRun report written to {report_file_path}\n')

    # Cache final metadata to s3
    if non_tiled_city_data.city_json_str is not None and non_tiled_city_data.publishing_target in ('s3', 'both'):
        cache_metadata_files(non_tiled_city_data)

    return_code = 0 if all(combined_delays_passed) or delays_all_passed else 1

    # Write qgis files
    if return_code == 0:
        if non_tiled_city_data.publishing_target in ('local', 'both'):
            log_general_file_message('Building QGIS viewer objects', __file__, logger)
            write_qgis_files(non_tiled_city_data)
        return_str = "Processing encountered no errors."
    else:
        return_str = 'Processing encountered errors. See log file.'

    log_general_file_message('Completing manager execution', __file__, logger)

    if non_tiled_city_data.city_json_str is not None:
        _print_unprocessed_internal_tile_names(non_tiled_city_data, unbuffered_tile_grid, urban_extent_polygon)

    return return_code, return_str


def _process_tasks(tasks, logger):
    import platform
    # See https://docs.dask.org/en/stable/deploying-python.html
    # https://blog.dask.org/2021/11/02/choosing-dask-chunk-sizes#what-to-watch-for-on-the-dashboard

    if tasks:
        num_workers = floor(USABLE_CPU_FRACTION * mp.cpu_count() )

        os_name = platform.system()
        if os_name == 'Linux':
            # Ensure that memory is trimmed automatically. See https://distributed.dask.org/en/stable/worker-memory.html
            os.environ["MALLOC_TRIM_THRESHOLD_"] = "0"

            # This is sized for a 600m tile width with 600m buffer
            sized_limit_val = 25
            # Setting above the physical RAM limit of the box will for Dask to use swap as needed
            total_memory_bytes = psutil.virtual_memory().total
            total_memory_gb = total_memory_bytes / (1024 ** 3)
            # reduce to a fraction of available memory
            fraction_limit_val = int(total_memory_gb * 0.7)
            if sized_limit_val < fraction_limit_val:
                memory_limit = f"{sized_limit_val}GB"
            else:
                memory_limit = f"{fraction_limit_val}GB"
        else:
            memory_limit = '2GB'

        # run dask cluster
        results = asyncio.run(
            run_dask_tasks(
                tasks=tasks,
                memory_limit=memory_limit,
                n_workers=num_workers,
                threads_per_worker=1
            )
        )
        all_passed, results_df, failed_task_ids, failed_task_details = parse_row_results(results)

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

async def run_dask_tasks(tasks, memory_limit='2GB', n_workers=2, threads_per_worker=1):
    try:
        async with LocalCluster(n_workers=n_workers,
                                threads_per_worker=threads_per_worker,
                                memory_limit=memory_limit,
                                processes=True,  # Explicitly use multiprocessing
                                asynchronous=True) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                futures = [client.submit(func, *args) for func, args in tasks]
                results = await asyncio.gather(*futures)
                return results
    except Exception as e:
        # Log, re-raise, or handle gracefully
        print(f"Error during Dask execution: {e}")
        return None


def _get_and_write_city_boundary(non_tiled_city_data, unbuffered_tile_grid):
    city = json.loads(non_tiled_city_data.city_json_str)
    city_id = city['city_id']
    aoi_id = city['aoi_id']

    if aoi_id == 'urban_extent':
        utm_crs = unbuffered_tile_grid.crs
        urban_extent_gdf = get_city_boundaries(city_id, aoi_id).to_crs(utm_crs)

        # Save the boundary to disk
        urban_extent_file_path = os.path.join(non_tiled_city_data.target_qgis_data_path, FILENAME_URBAN_EXTENT_BOUNDARY)
        write_layer(urban_extent_gdf, urban_extent_file_path, GEOJSON_FILE_EXTENSION)

        urban_extent_polygon = (urban_extent_gdf['geometry']).to_crs(utm_crs)

        return urban_extent_polygon
    else:
        return None

def _print_unprocessed_internal_tile_names(non_tiled_city_data, unbuffered_tile_grid, urban_extent_polygon):
    city = json.loads(non_tiled_city_data.city_json_str)

    if urban_extent_polygon is not None:
        # Thin to tiles that are internal to the city polygon
        thinned_unbuffered_tile_grid = unbuffered_tile_grid[unbuffered_tile_grid.intersects(urban_extent_polygon[0])]

        # Thin to tiles not already in S3.
        bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)
        scenario_folder_key = get_scenario_folder_key(non_tiled_city_data)
        existing_folders = list_s3_subfolders(bucket_name, scenario_folder_key)
        existing_tiles = [s.replace(scenario_folder_key, "").replace('/', '') for s in existing_folders]

        thinned_unbuffered_tile_grid = (
            thinned_unbuffered_tile_grid)[~thinned_unbuffered_tile_grid['tile_name'].isin(existing_tiles)]

        unprocessed_tile_names = ', '.join(thinned_unbuffered_tile_grid['tile_name'])

        print(f'\nFYI - Set of unprocessed tiles in city grid which intersect city polygon:\n{unprocessed_tile_names}')


def _thin_city_tile_grid(non_tiled_city_data, unbuffered_tile_grid, tile_grid, urban_extent_polygon):
    city = json.loads(non_tiled_city_data.city_json_str)
    tile_method = city['tile_method']
    tile_function, tile_function_params = extract_function_and_params(tile_method)

    internal_tile_count = None
    internal_aoi_tile_count = None
    if urban_extent_polygon is not None:
        utm_crs = unbuffered_tile_grid.crs

        # Thin to tiles that are internal to the city polygon
        thinned_unbuffered_tile_grid = unbuffered_tile_grid[unbuffered_tile_grid.intersects(urban_extent_polygon[0])]
        internal_tile_count = len(thinned_unbuffered_tile_grid)

        # Thin to tiles that overlap the sub-area AOI
        if tile_function is None or tile_function == 'resume()':
            aoi_polygon = shapely.box(non_tiled_city_data.min_lon, non_tiled_city_data.min_lat,
                          non_tiled_city_data.max_lon, non_tiled_city_data.max_lat)

            if non_tiled_city_data.source_aoi_crs != utm_crs:
                import geopandas as gpd
                gdf = gpd.GeoDataFrame({'geometry': [aoi_polygon]}, crs=non_tiled_city_data.source_aoi_crs)
                gdf_projected = gdf.to_crs(epsg=utm_crs)
                aoi_polygon = gdf_projected.geometry.iloc[0]

            thinned_unbuffered_tile_grid = thinned_unbuffered_tile_grid[thinned_unbuffered_tile_grid.intersects(aoi_polygon)]
            internal_aoi_tile_count= len(thinned_unbuffered_tile_grid)

        if tile_function is not None:
            # Thin to tiles not already in S3.
            bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)
            scenario_folder_key = get_scenario_folder_key(non_tiled_city_data)
            existing_folders = list_s3_subfolders(bucket_name, scenario_folder_key)
            existing_tiles = [s.replace(scenario_folder_key, "").replace('/','') for s in existing_folders]

            thinned_unbuffered_tile_grid = (
                thinned_unbuffered_tile_grid)[~thinned_unbuffered_tile_grid['tile_name'].isin(existing_tiles)]

            if tile_function == 'tile_names()':
                tile_names = tile_function_params.split(",")
                thinned_unbuffered_tile_grid = (
                    thinned_unbuffered_tile_grid)[thinned_unbuffered_tile_grid['tile_name'].isin(tile_names)]

        # Also thin the tile_grid to the thinned unbuffered_tile_grid
        result_tile_grid = tile_grid[tile_grid['tile_name'].isin(thinned_unbuffered_tile_grid['tile_name'])]
    else:
        result_tile_grid = tile_grid

    return result_tile_grid, internal_tile_count, internal_aoi_tile_count


def _determine_era5_date_range(sampling_date_range:str):
    parsed_dates = sampling_date_range.split(',')

    current_year = datetime.now().year
    latest_complete_record_year = current_year - 1
    if parsed_dates[0] == PRIOR_5_YEAR_KEYWORD:
        # assume 5-year interval
        prior_interval_start_year = current_year - 6
        prior_interval_end_year = latest_complete_record_year
        sampling_start_date = datetime(prior_interval_start_year, 1, 1).strftime('%Y-%m-%d')
        sampling_end_date = datetime(prior_interval_end_year, 12, 31).strftime('%Y-%m-%d')
    else:
        sampling_start_date = datetime.strptime(parsed_dates[0].strip(), "%Y-%m-%d").date()
        sampling_end_date = datetime.strptime(parsed_dates[1].strip(), "%Y-%m-%d").date()

    return str(sampling_start_date), str(sampling_end_date)


def _transfer_custom_met_files(non_tiled_city_data):
    create_folder(non_tiled_city_data.target_met_files_path)
    for met_filename in non_tiled_city_data.met_filenames:
        if not(non_tiled_city_data.has_era_met_download and met_filename in [FILENAME_ERA5_UMEP, FILENAME_ERA5_UPENN]):
            source_path = os.path.join(non_tiled_city_data.source_met_files_path, met_filename)
            target_path = os.path.join(non_tiled_city_data.target_met_files_path, met_filename)
            shutil.copyfile(source_path, target_path)

def _transfer_cached_met_files(non_tiled_city_data):
    target_met_path = non_tiled_city_data.target_met_files_path
    bucket_name, scenario_folder_key = get_s3_scenario_location(non_tiled_city_data)
    met_files_folder_key = f"{scenario_folder_key}/metadata/{FOLDER_NAME_PRIMARY_MET_FILES}"

    download_s3_files(bucket_name, met_files_folder_key, target_met_path)

def _transfer_and_read_cached_qgis_files(non_tiled_city_data):
    target_qgis_path = non_tiled_city_data.target_qgis_data_path
    bucket_name, scenario_folder_key = get_s3_scenario_location(non_tiled_city_data)
    qgis_files_folder_key = f"{scenario_folder_key}/metadata/{FOLDER_NAME_QGIS_DATA}"

    download_s3_files(bucket_name, qgis_files_folder_key, target_qgis_path)

    tile_grid_uri = f"file://{target_qgis_path}/{FILENAME_TILE_GRID}"
    tile_grid = read_geojson_from_cache(tile_grid_uri)

    try:
        unbuffered_tile_grid_uri = f"file://{target_qgis_path}/{FILENAME_UNBUFFERED_TILE_GRID}"
        unbuffered_tile_grid = read_geojson_from_cache(unbuffered_tile_grid_uri)
    except:
        unbuffered_tile_grid = None

    return tile_grid, unbuffered_tile_grid

def _construct_tile_proc_array(city_json_str, processing_method, source_base_path, target_base_path, city_folder_name,
                               tile_folder_name, cif_primary_features, ctcm_intermediate_features,
                               tile_boundary, tile_resolution, seasonal_utc_offset, grid_crs):
    if cif_primary_features:
        cif_features = ','.join(cif_primary_features)
    else:
        cif_features = None

    if ctcm_intermediate_features:
        ctcm_features = ','.join(ctcm_intermediate_features)
    else:
        ctcm_features = None

    if processing_method == 'upenn_model':
        proc_array = [city_json_str, processing_method, source_base_path, target_base_path, city_folder_name, tile_folder_name,
                cif_features, ctcm_features,  tile_boundary, tile_resolution, seasonal_utc_offset, grid_crs]
    else:
        proc_array = ['python', TILE_PROCESSING_MODULE_PATH,
                      f'--city_json_str={city_json_str}',
                      f'--processing_method={processing_method}',
                      f'--source_base_path={source_base_path}',
                      f'--target_base_path={target_base_path}',
                      f'--city_folder_name={city_folder_name}',
                      f'--tile_folder_name={tile_folder_name}',
                      f'--cif_primary_features={cif_features}',
                      f'--ctcm_intermediate_features={ctcm_features}',
                      f'--tile_boundary={tile_boundary}',
                      f'--tile_resolution={tile_resolution}',
                      f'--seasonal_utc_offset={seasonal_utc_offset}',
                      f'--target_crs={grid_crs}'
                      ]

    return proc_array
