import os
import pandas as pd
from workers.city_data import CityData

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

    config_processing_file_path = str(os.path.join(city_path, CityData.file_name_umep_city_processing_config))
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
            invalids.append(f"Invalid enabled value ({str(enabled)}) on row {index}. Valid values: {valid_enabled}")

    for index, config_row in processing_config_df.iterrows():
        enabled = str(config_row.enabled)
        if bool(enabled) or pre_check_option == 'check_all':
            folder_name_tile_data = config_row.tile_folder_name
            city_data = CityData(city_folder_name, folder_name_tile_data, source_base_path, target_base_path)
            source_tile_path = city_data.source_tile_data_path
            if not os.path.isdir(source_tile_path):
                invalids.append(
                    f"tile folder ({str(folder_name_tile_data)}) on row {index} not found under '{source_base_path}'.")

    for index, config_row in processing_config_df.iterrows():
        enabled = str(config_row.enabled)
        if bool(enabled) or pre_check_option == 'check_all':
            method = config_row.method
            valid_methods = CityData.plugin_methods
            if method not in valid_methods:
                invalids.append(f"Invalid 'method' ({method}) on row {index}. Valid values: {valid_methods}")

    # check file dependencies
    for index, config_row in processing_config_df.iterrows():
        enabled = str(config_row.enabled)
        if bool(enabled) or pre_check_option == 'check_all':
            method = config_row.method
            folder_name_tile_data = config_row.tile_folder_name
            city_data = CityData(city_folder_name, folder_name_tile_data, source_base_path, target_base_path)

            prior_dsm = city_data.source_dsm_path
            if _verify_path(prior_dsm) is False:
                msg = f'Required source file: {prior_dsm} not found for row {index}.'
                invalids.append(msg)

            if method in ['skyview_factor', 'solweig_full', 'solweig_only']:
                prior_veg_canopy = city_data.source_veg_canopy_path
                if _verify_path(prior_veg_canopy) is False:
                    msg = f'Required source file: {prior_veg_canopy} not found for method: {method} on row {index}.'
                    invalids.append(msg)

            if method in ['solweig_only', 'solweig_full']:
                prior_land_cover = city_data.source_land_cover_path
                prior_dem = city_data.source_dem_path
                if _verify_path(prior_land_cover) is False:
                    msg = f'Required source file: {prior_land_cover} not found for method: {method} on row {index}.'
                    invalids.append(msg)
                if _verify_path(prior_dem) is False:
                    msg = f'Required source file: {prior_dem} not found for method: {method} on row {index}.'
                    invalids.append(msg)

            if method in ['solweig_only']:
                prior_svfszip = city_data.target_svfszip_path
                prior_wallheight = city_data.target_wallheight_path
                prior_wallaspect = city_data.target_wallaspect_path
                if _verify_path(prior_svfszip) is False:
                    msg = f'Required source file: {prior_svfszip} currently not found for method: {method} on row {index}.'
                    invalids.append(msg)
                if _verify_path(prior_wallheight) is False:
                    msg = f'Required source file: {prior_wallheight} currently not found for method: {method} on row {index}.'
                    invalids.append(msg)
                if _verify_path(prior_wallaspect) is False:
                    msg = f'Required source file: {prior_wallaspect} currently not found for method: {method} on row {index}.'
                    invalids.append(msg)

    return invalids

