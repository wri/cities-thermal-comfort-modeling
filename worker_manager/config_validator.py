import math
import os

from worker_manager.graph_builder import get_cif_features
from src.src_tools import get_existing_tiles
from workers.city_data import CityData, parse_filenames_config, parse_processing_areas_config

def verify_fundamental_paths(source_base_path, target_path, city_folder_name):
    invalids = []
    if _verify_path(source_base_path) is False:
        msg = f'Invalid source base path: {source_base_path}'
        invalids.append(msg)

    city_path = os.path.join(source_base_path, city_folder_name)
    if _verify_path(city_path) is False:
        msg = f'Invalid source city path: {city_path}'
        invalids.append(msg)

    if _verify_path(target_path) is False:
        msg = f'Invalid target base path: {target_path}'
        invalids.append(msg)

    if invalids:
        return invalids

    config_processing_file_path = str(os.path.join(city_path, CityData.filename_umep_city_processing_config))
    if _verify_path(config_processing_file_path) is False:
        msg = f'Processing registry file does not exist as: {config_processing_file_path}'
        invalids.append(msg)

    return invalids


def _verify_path(path):
    is_valid = os.path.exists(path)
    return is_valid


def verify_processing_config(processing_config_df, source_base_path, target_base_path, city_folder_name, pre_check_option):
    invalids = []
    for index, config_row in processing_config_df.iterrows():
        enabled = str(config_row.enabled)
        valid_enabled = ['true', 'false']
        if enabled.lower() not in valid_enabled:
            invalids.append(f"Invalid 'enabled' column ({str(enabled)}) on row {index} in .config_umep_city_processing.csv. Valid values: {valid_enabled}")

    source_city_path = str(os.path.join(source_base_path, city_folder_name))
    custom_file_names, has_custom_features, cif_features = get_cif_features(source_city_path)

    for index, config_row in processing_config_df.iterrows():
        start_tile_id = config_row.start_tile_id
        end_tile_id = config_row.end_tile_id
        if not has_custom_features:
            existing_tiles = []
        else:
            existing_tiles = get_existing_tiles(source_city_path, custom_file_names, start_tile_id, end_tile_id)

        enabled = str(config_row.enabled)

        method = config_row.method
        valid_methods = CityData.processing_methods
        if method not in valid_methods:
            invalids.append(
                f"Invalid 'method' column ({method}) on row {index} in .config_umep_city_processing.csv. Valid values: {valid_methods}")

        dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_custom_features, feature_list = \
            parse_filenames_config(source_city_path, CityData.filename_method_parameters_config)

        if not has_custom_features:
            utc_offset, min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters = \
                parse_processing_areas_config(source_city_path, CityData.filename_method_parameters_config)

            if not isinstance(min_lon, float) or not isinstance(min_lat, float) or not isinstance(max_lon, float) or not isinstance(max_lat, float):
                msg = f'If there are no custom source tif files, then values in NewProcessingAOI section must be defined in {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            if (tile_side_meters != None and tile_side_meters != 'None' and
                    _is_tile_wider_than_half_aoi_side(min_lat, min_lon, max_lat, max_lon, tile_side_meters)):
                msg = f"Requested tile_side_meters cannot be larger than half the AOI side length in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_side_meters != None and tile_side_meters != 'None' and tile_side_meters < 200:
                msg = f"Requested tile_side_meters cannot be less than 200 meters in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_side_meters != None and tile_side_meters != 'None' and int(tile_side_meters) <= 10:
                msg = f"Both tile_side_meters must be greater than 10 in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_buffer_meters != None and tile_buffer_meters != 'None' and int(tile_buffer_meters) <= 10:
                msg = f"Both tile_buffer_meters must be greater than 10 in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

        else:
            for tile_folder_name, tile_dimensions in existing_tiles.items():
                if bool(enabled) or pre_check_option == 'pre_check_all':
                    try:
                        city_data = CityData(city_folder_name, tile_folder_name, source_base_path, target_base_path)
                    except Exception as e_msg:
                        invalids.append(e_msg)
                        break

                    prior_dsm = city_data.source_dsm_path
                    if 'dsm' not in cif_features and _verify_path(prior_dsm) is False and prior_dsm != 'None':
                        msg = f'Required source file: {prior_dsm} not found for row {index} in .config_umep_city_processing.csv.'
                        invalids.append(msg)

                    if method in CityData.processing_methods:
                        prior_tree_canopy = city_data.source_tree_canopy_path
                        if 'tree_canopy' not in cif_features and _verify_path(prior_tree_canopy) is False and prior_tree_canopy != 'None':
                            msg = f'Required source file: {prior_tree_canopy} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)

                    if method in ['solweig_only', 'solweig_full']:
                        prior_land_cover = city_data.source_land_cover_path
                        prior_dem = city_data.source_dem_path
                        if 'lulc' not in cif_features and _verify_path(prior_land_cover) is False and prior_land_cover != 'None':
                            msg = f'Required source file: {prior_land_cover} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        if 'dem' not in cif_features and _verify_path(prior_dem) is False and prior_dem != 'None':
                            msg = f'Required source file: {prior_dem} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        for met_file_row in city_data.met_files:
                            met_file = met_file_row.get('filename')
                            met_filepath = os.path.join(city_data.source_met_files_path, met_file)
                            if met_file != 'download_era5' and _verify_path(met_filepath) is False:
                                msg = f'Required meteorological file: {met_filepath} not found for method: {method} in .config_method_parameters.yml.'
                                invalids.append(msg)
                        utc_offset = city_data.utc_offset
                        if not -24 <= utc_offset <= 24:
                            msg = f'UTC-offset for: {met_file} not in -24 to 24 hours range as specified in .config_method_parameters.yml.'
                            invalids.append(msg)

                    if method in ['solweig_only']:
                        prior_svfszip = city_data.target_svfszip_path
                        prior_wallheight = city_data.target_wallheight_path
                        prior_wallaspect = city_data.target_wallaspect_path
                        if _verify_path(prior_svfszip) is False:
                            msg = f'Required source file: {prior_svfszip} currently not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        if _verify_path(prior_wallheight) is False:
                            msg = f'Required source file: {prior_wallheight} currently not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        if _verify_path(prior_wallaspect) is False:
                            msg = f'Required source file: {prior_wallaspect} currently not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)

    return invalids

def _is_tile_wider_than_half_aoi_side(min_lat, min_lon, max_lat, max_lon, tile_side_meters):
    center_lat = (min_lat + max_lat) / 2
    lon_degree_offset, lat_degree_offset = offset_meters_to_geographic_degrees(center_lat, tile_side_meters)

    is_tile_wider_than_half = False
    if (lon_degree_offset > (max_lon - min_lon)/2) or (lat_degree_offset > (max_lat - min_lat)/2):
        is_tile_wider_than_half = True

    return is_tile_wider_than_half


def _validate_basic_inputs(source_base_path, target_path, city_folder_name):
    invalids = verify_fundamental_paths(source_base_path, target_path, city_folder_name)
    if invalids:
        print('\n')
        _highlighted_print('------------ Invalid source/target folders ------------ ')
        for invalid in invalids:
            print(invalid)
        raise Exception("Stopped processing due to invalid source/target folders.")
    else:
        return 0

def _validate_config_inputs(processing_config_df, source_base_path, target_path, city_folder_name, pre_check_option):
    detailed_invalids = verify_processing_config(processing_config_df, source_base_path, target_path, city_folder_name, pre_check_option)
    if detailed_invalids:
        print('\n')
        _highlighted_print('------------ Invalid configurations ------------ ')
        for invalid in detailed_invalids:
            print(invalid)
        raise Exception("Stopped processing due to invalid configurations.")
    else:
        return 0

def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')

def offset_meters_to_geographic_degrees(decimal_latitude, length_m):
    earth_radius_m = 6378137
    rad = 180/math.pi

    lon_degree_offset = abs((length_m / (earth_radius_m * math.cos(math.pi*decimal_latitude/180))) * rad)
    lat_degree_offset = abs((length_m / earth_radius_m) * rad)

    return lon_degree_offset, lat_degree_offset
