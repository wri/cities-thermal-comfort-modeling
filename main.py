import json
import os
import math

from src.data_validation.manager import validate_config
from src.data_validation.s3_postproc_results_validation import validate_tile_results
from src.worker_manager.ancillary_files import write_config_files
from src.worker_manager.ctcm_layer_staging import check_ctcm_staging, publish_ctcm_staging_files
from src.worker_manager.job_manager import start_jobs
from src.worker_manager.tools import get_existing_tile_metrics, get_aoi_area_in_square_meters, \
    extract_function_and_params, get_s3_scenario_location
from src.workers.city_data import CityData, get_yml_content
from src.workers.worker_tools import remove_folder, does_s3_folder_exist
from city_metrix import GeoZone
from city_metrix.metrix_model import GeoExtent

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""

def start_processing(source_base_path, target_base_path, city_folder_name, processing_option):
    abs_source_base_path = os.path.abspath(source_base_path)

    if processing_option == 'prep_check_ctcm_staging':
        non_tiled_city_data = CityData(None, city_folder_name, None, abs_source_base_path,
                                       None)
        return_code = check_ctcm_staging(non_tiled_city_data)
        return return_code
    elif processing_option == 'stage_ctcm_data':
        non_tiled_city_data = CityData(None, city_folder_name, None, abs_source_base_path,
                                       None)
        return_code = publish_ctcm_staging_files(non_tiled_city_data)
        return return_code
    elif processing_option == 'validate_ctcm_result_data':
        non_tiled_city_data = CityData(None, city_folder_name, None, abs_source_base_path,
                                       None)
        return_code = validate_tile_results(non_tiled_city_data)
        return return_code

    abs_target_base_path = os.path.abspath(target_base_path)

    # get city extent this one time since retrieval is slow
    city_geoextent = _get_city_geoextent(source_base_path, city_folder_name)

    non_tiled_city_data = CityData(city_geoextent, city_folder_name, None, abs_source_base_path, abs_target_base_path)

    existing_tiles_metrics = _get_existing_tiles(non_tiled_city_data)
    updated_aoi, config_return_code = validate_config(non_tiled_city_data, existing_tiles_metrics, city_geoextent, processing_option)

    if config_return_code != 0:
        raise ValueError('Invalid configuration(s). Stopping.')

    has_appendable_cache = False
    if non_tiled_city_data.publishing_target in ('s3', 'both'):
        bucket_name, scenario_folder_key = get_s3_scenario_location(non_tiled_city_data)
        does_s3_target_scenario_exist = does_s3_folder_exist(bucket_name, scenario_folder_key)
        city = json.loads(non_tiled_city_data.city_json_str)
        tile_method = city['tile_method']
        tile_function, _ = extract_function_and_params(tile_method)
        if does_s3_target_scenario_exist and tile_function is None:
            print(f"\n\n*** Aborting run since scenario folder ({scenario_folder_key}) already exists in S3 bucket: {bucket_name}.")
            print("*** You must choose a different scenario name, use a tile_method, or as extreme solution, remove the existing data in S3.\n")
            raise Exception('Collision with existing results in S3.')
        if does_s3_target_scenario_exist and tile_function is not None:
            has_appendable_cache = True

    # Print runtime estimate
    umep_solweig_raster_cell_count = _get_tile_raster_cell_count(non_tiled_city_data, existing_tiles_metrics)
    if umep_solweig_raster_cell_count is not None:
        x = umep_solweig_raster_cell_count
        if umep_solweig_raster_cell_count < 1E4:
            print(f'\nEstimated runtime for processing each tile is a few minutes or less for tiles with {x:,} raster cells.\n')
        else:
            if umep_solweig_raster_cell_count < 4E6:
                est_runtime_mins = math.ceil(3.0166957E-5*x +3.2689E1)
            else:
                est_runtime_mins = math.ceil(6.26228065090E-4*x -2.28907E3)
            _print_runtime_estimate(x, est_runtime_mins)

    if processing_option == 'run_pipeline':
        # Remove existing target scenario folder
        target_scenario_path = non_tiled_city_data.target_city_path
        remove_folder(target_scenario_path)

        # write configs and last run metrics
        write_config_files(non_tiled_city_data, updated_aoi)

        # Process data
        return_code, return_str = start_jobs(non_tiled_city_data, existing_tiles_metrics, has_appendable_cache)

        if return_code == 0:
            print(return_str)
        else:
            _highlighted_yellow_print(return_str)
        return return_code

def _get_city_geoextent(source_base_path, folder_name_city_data):
    yml_values = get_yml_content(source_base_path, folder_name_city_data)
    processing_area = yml_values[1]
    city_json_str = None if processing_area['city'] == 'None' else json.dumps(processing_area['city'])
    if city_json_str  is not None:
        city_geozone = GeoZone(geo_zone=city_json_str)
        city_geoextent = GeoExtent(city_geozone)
    else:
        city_geoextent = None
    return city_geoextent

def _get_existing_tiles(non_tiled_city_data):
    if non_tiled_city_data.custom_primary_feature_list:
        existing_tiles_metrics = (
            get_existing_tile_metrics(non_tiled_city_data.source_city_path,
                                      non_tiled_city_data.custom_primary_filenames))
    else:
        existing_tiles_metrics = None
    return existing_tiles_metrics


def _get_tile_raster_cell_count(non_tiled_city_data:CityData, existing_tiles_metrics):
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer = 0 if non_tiled_city_data.tile_buffer_meters is None else non_tiled_city_data.tile_buffer_meters
    if non_tiled_city_data.custom_primary_feature_list:
        # Get representative cell count
        cell_count = existing_tiles_metrics['cell_count'][0]
    elif tile_side_meters is not None:
        # Assume 1-m resolution
        cell_count = (tile_side_meters + (2*tile_buffer)) ** 2
    else:
        # Infer raster cell count from aoi
        square_meters = get_aoi_area_in_square_meters(non_tiled_city_data.min_lon, non_tiled_city_data.min_lat,
                                                      non_tiled_city_data.max_lon, non_tiled_city_data.max_lat)
        # Assume 1-meter resolution of target cif files
        cell_count = math.ceil(square_meters)
    return cell_count

def _print_runtime_estimate(cell_count, est_runtime_mins):
    est_runtime_hours = round(est_runtime_mins / 60, 1)
    print(f'\nEstimated runtime for processing of each tile is {est_runtime_hours} hours for {cell_count:,} raster cells per tile.')

def _highlighted_yellow_print(msg):
    print('\n\x1b[6;30;43m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--source_base_path', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--target_base_path', metavar='str', required=True,
                        help='the path to city-based target data')
    parser.add_argument('--city_folder_name', metavar='str', required=True,
                        help='name of source city_folder')
    valid_methods = ['run_pipeline', 'pre_check', 'prep_check_ctcm_staging','stage_ctcm_data', 'validate_ctcm_result_data']
    parser.add_argument('--processing_option', metavar='str', choices=valid_methods, required=True,
                        help=f'specifies type of configuration pre-check. Options are: {valid_methods}')
    args = parser.parse_args()

    start_processing(args.source_base_path, args.target_base_path, args.city_folder_name, args.processing_option)
