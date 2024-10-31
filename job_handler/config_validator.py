import os

from job_handler.graph_builder import get_cif_features
from src.src_tools import get_existing_tiles
from workers.city_data import CityData, parse_filenames_config, parse_processing_areas_config


# valid_methods = ['no_pre_check', 'check_all', 'check_enabled_only']

def verify_fundamental_paths(source_base_path, target_path, city_folder_name):
    invalids = []
    if _verify_path(source_base_path) is False:
        msg = f'Invalid source base path: {source_base_path}'
        invalids.append(msg)

    city_path = str(os.path.join(source_base_path, city_folder_name))
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
            min_lon, min_lat, max_lon, max_lat, sub_division_cell_size = \
                parse_processing_areas_config(source_city_path, CityData.filename_method_parameters_config)
            lon_diff = max_lon - min_lon
            lat_diff = max_lat - min_lat
            max_bounds_side_size = lon_diff if lon_diff > lat_diff else lat_diff
            if not isinstance(min_lon, float) or not isinstance(min_lat, float) or not isinstance(max_lon, float) or not isinstance(max_lat, float):
                msg = f'If there are no custom source tif files, then values in NewProcessingAOI section must be defined in {CityData.filename_method_parameters_config}'
                invalids.append(msg)
            if sub_division_cell_size != None and sub_division_cell_size != 'None' and sub_division_cell_size > max_bounds_side_size:
                msg = f"Requested sub_division_cell_size cannot be larger than the boundary size in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)
        else:
            for tile_folder_name, tile_dimensions in existing_tiles.items():
                if bool(enabled) or pre_check_option == 'check_all':
                    try:
                        city_data = CityData(city_folder_name, tile_folder_name, source_base_path, target_base_path)
                    except Exception as e_msg:
                        invalids.append(e_msg)
                        break

                    prior_dsm = city_data.source_dsm_path
                    if _verify_path(prior_dsm) is False and prior_dsm != 'None':
                        msg = f'Required source file: {prior_dsm} not found for row {index} in .config_umep_city_processing.csv.'
                        invalids.append(msg)

                    if method in CityData.processing_methods:
                        prior_tree_canopy = city_data.source_tree_canopy_path
                        if _verify_path(prior_tree_canopy) is False and prior_tree_canopy != 'None':
                            msg = f'Required source file: {prior_tree_canopy} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)

                    if method in ['solweig_only', 'solweig_full']:
                        prior_land_cover = city_data.source_land_cover_path
                        prior_dem = city_data.source_dem_path
                        if _verify_path(prior_land_cover) is False and prior_land_cover != 'None':
                            msg = f'Required source file: {prior_land_cover} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        if _verify_path(prior_dem) is False and prior_dem != 'None':
                            msg = f'Required source file: {prior_dem} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        for met_file_row in city_data.met_files:
                            met_file = met_file_row.get('filename')
                            met_filepath = os.path.join(city_data.source_met_files_path, met_file)
                            if _verify_path(met_filepath) is False:
                                msg = f'Required meteorological file: {met_filepath} not found for method: {method} in .config_method_parameters.yml.'
                                invalids.append(msg)
                            utc_offset = met_file_row.get('utc_offset')
                            if not -24 <= utc_offset <= 24:
                                msg = f'UTC range for: {met_file} not in range for 24-hour offsets as specified in .config_method_parameters.yml.'
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