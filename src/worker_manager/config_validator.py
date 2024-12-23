
import math
import numbers
import os
import rasterio
import pandas as pd

from src.worker_manager.graph_builder import get_cif_features
from src.worker_manager.tools import get_aoi_area_in_square_meters, get_existing_tiles, list_files_with_extension
from src.workers.city_data import CityData, parse_filenames_config, parse_processing_areas_config

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

    if source_base_path == target_path:
        msg = f'Source and target base paths cannot be the same.'
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


def _verify_processing_config(processing_config_df, source_base_path, target_base_path, city_folder_name, pre_check_option):
    invalids = []
    for index, config_row in processing_config_df.iterrows():
        enabled = str(config_row.enabled)
        valid_enabled = ['true', 'false']
        if enabled.lower() not in valid_enabled:
            invalids.append(f"Invalid 'enabled' column ({str(enabled)}) on row {index} in .config_umep_city_processing.csv. Valid values: {valid_enabled}")

    source_city_path = str(os.path.join(source_base_path, city_folder_name))
    custom_file_names, has_custom_features, cif_features = get_cif_features(source_city_path)

    cell_count = None
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

        dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_custom_features, custom_feature_list, cif_feature_list = \
            parse_filenames_config(source_city_path, CityData.filename_method_parameters_config)

        non_tiled_city_data = CityData(city_folder_name, None, source_base_path, None)

        if (not has_custom_features or
                any(d['filename'] == CityData.method_trigger_era5_download for d in non_tiled_city_data.met_filenames)):

            utc_offset, min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters = \
                parse_processing_areas_config(source_city_path, CityData.filename_method_parameters_config)

            if (not isinstance(min_lon, numbers.Number) or not isinstance(min_lat, numbers.Number) or
                    not isinstance(max_lon, numbers.Number) or not isinstance(max_lat, numbers.Number)):
                msg = f'If there are no custom source tif files, then values in NewProcessingAOI section must be defined in {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            if not (-180 <= min_lon <= 180) or not (-180 <= max_lon <= 180):
                msg = f'Min and max longitude values must be between -180 and 180 in ProcessingAOI section of {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            if not (-90 <= min_lat <= 90) or not (-90 <= max_lat <= 90):
                msg = f'Min and max latitude values must be between -90 and 90 in ProcessingAOI section of {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            if not (min_lon <= max_lon):
                msg = f'Min longitude must be less than max longitude in ProcessingAOI section of {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            if not (min_lat <= max_lat):
                msg = f'Min latitude must be less than max latitude in ProcessingAOI section of {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            # TODO improve this evaluation
            if abs(max_lon - min_lon) > 0.3 or abs(max_lon - min_lon) > 0.3:
                msg = f'Specified AOI must be less than 30km on a side in ProcessingAOI section of {CityData.filename_method_parameters_config}'
                invalids.append(msg)

            if (tile_side_meters is not None and
                    _is_tile_wider_than_half_aoi_side(min_lat, min_lon, max_lat, max_lon, tile_side_meters)):
                msg = f"Requested tile_side_meters cannot be larger than half the AOI side length in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_side_meters is not None and tile_side_meters < 150:
                msg = f"Requested tile_side_meters must be 100 meters or more in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_side_meters is not None and int(tile_side_meters) <= 10:
                msg = f"tile_side_meters must be greater than 10 in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_buffer_meters is not None and int(tile_buffer_meters) <= 10:
                msg = f"tile_buffer_meters must be greater than 10 in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_buffer_meters is not None and int(tile_buffer_meters) > 500:
                msg = f"tile_buffer_meters must be less than 500 in {CityData.filename_method_parameters_config}. Specify None if you don't want to subdivide the aoi."
                invalids.append(msg)

            if tile_side_meters is None and tile_buffer_meters is not None:
                msg = f"tile_buffer_meters must be None if tile_sider_meters is None in {CityData.filename_method_parameters_config}."
                invalids.append(msg)

        if has_custom_features:
            for tile_folder_name, tile_dimensions in existing_tiles.items():
                if bool(enabled) or pre_check_option == 'pre_check_all':
                    try:
                        city_data = CityData(city_folder_name, tile_folder_name, source_base_path, target_base_path)
                    except Exception as e_msg:
                        invalids.append(e_msg)
                        break

                    prior_dsm = city_data.source_dsm_path
                    if cif_features is not None and 'dsm' not in cif_features and _verify_path(prior_dsm) is False:
                        msg = f'Required source file: {prior_dsm} not found for row {index} in .config_umep_city_processing.csv.'
                        invalids.append(msg)

                    if method in CityData.processing_methods:
                        prior_tree_canopy = city_data.source_tree_canopy_path
                        if cif_features is not None and 'tree_canopy' not in cif_features and _verify_path(prior_tree_canopy) is False:
                            msg = f'Required source file: {prior_tree_canopy} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)

                    if method in ['solweig_only', 'solweig_full']:
                        prior_land_cover = city_data.source_land_cover_path
                        prior_dem = city_data.source_dem_path
                        if cif_features is not None and 'lulc' not in cif_features and _verify_path(prior_land_cover) is False:
                            msg = f'Required source file: {prior_land_cover} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        if cif_features is not None and 'dem' not in cif_features and _verify_path(prior_dem) is False:
                            msg = f'Required source file: {prior_dem} not found for method: {method} on row {index} in .config_umep_city_processing.csv.'
                            invalids.append(msg)
                        for met_file_row in city_data.met_filenames:
                            met_file = met_file_row.get('filename')
                            met_filepath = os.path.join(city_data.source_met_filenames_path, met_file)
                            if met_file != '<download_era5>' and _verify_path(met_filepath) is False:
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

                    full_metrics_df, named_consistency_metrics_df, unique_consistency_metrics_df = (
                        get_parameters_for_custom_tif_files(city_data, tile_folder_name, cif_feature_list))

                    if full_metrics_df['nodata'].isnull().any():
                        files_with_nan = full_metrics_df.loc[full_metrics_df['nodata'].isnull(), 'filename'].tolist()
                        files_with_nan_str = ','.join(map(str,files_with_nan))
                        msg = f"Folder {tile_folder_name} and possibly other folders has forbidden no_data='nan' in file(s) ({files_with_nan_str})."
                        invalids.append(msg)

                        break

                    if 'lulc' in custom_feature_list:
                        lulc_metrics = full_metrics_df.loc[full_metrics_df['filename'] == city_data.lulc_tif_filename]
                        if lulc_metrics is not None:
                            band_min = lulc_metrics['band_min'].values[0]
                            band_max = lulc_metrics['band_max'].values[0]
                            if band_min < 1 or band_max > 7:
                                msg = f"Folder {tile_folder_name} and possibly other folders has LULC ({city_data.lulc_tif_filename}) with values outside of range 1-7."
                                invalids.append(msg)

                                break

                    if unique_consistency_metrics_df.shape[0] > 1:
                        msg = f'TIF files in folder {tile_folder_name} and possibly other folders has inconsistent parameters with {unique_consistency_metrics_df.shape[0]} unique parameter variants.'
                        invalids.append(msg)

                        msg = f'TIF parameters: {named_consistency_metrics_df.to_json(orient='records')}'
                        invalids.append(msg)

                        msg = 'Stopping analysis at first set of inconsistent TIF files.'
                        invalids.append(msg)

                        break

        if has_custom_features:
            # Get representative cell count
            if config_row.method == 'solweig_full':
                if 'dem' in custom_feature_list:
                    representative_tif = dem_tif_filename
                elif 'dsm' in custom_feature_list:
                    representative_tif = dsm_tif_filename
                elif 'tree_canopy' in custom_feature_list:
                    representative_tif = tree_canopy_tif_filename
                else:
                    representative_tif = lulc_tif_filename

                tiff_file_path = os.path.join(source_city_path, CityData.folder_name_primary_data,
                                              CityData.folder_name_primary_raster_files, 'tile_001', representative_tif)
                with rasterio.open(tiff_file_path) as dataset:
                    width = dataset.profile["width"]
                    height = dataset.profile["height"]
                    cell_count = width * height
        elif non_tiled_city_data.max_lat is not None and non_tiled_city_data.max_lon is not None:
            # Infer raster cell count from aoi
            square_meters = get_aoi_area_in_square_meters(non_tiled_city_data.min_lon, non_tiled_city_data.min_lat,
                                                          non_tiled_city_data.max_lon, non_tiled_city_data.max_lat)
            # Assume 1-meter resolution of target cif files
            cell_count = math.ceil(square_meters)

    return cell_count, invalids


def get_parameters_for_custom_tif_files(city_data, tile_folder_name, cif_feature_list):
    import sys

    tile_folder = os.path.join(city_data.source_city_data_path, city_data.folder_name_primary_raster_files,
                               tile_folder_name)
    tif_files = list_files_with_extension(tile_folder, '.tif')

    processing_list = _get_list_of_existing_tifs_to_be_processed(city_data, cif_feature_list)

    filtered_existing_list = filter_list_by_another_list(tif_files, processing_list)

    full_metrics_df = pd.DataFrame(columns=['filename', 'crs', 'width', 'height', 'bounds', 'band_min', 'band_max', 'nodata'])
    for tif_file in filtered_existing_list:
        tif_file_path = os.path.join(tile_folder, tif_file)
        with rasterio.open(tif_file_path) as dataset:
            crs = dataset.crs.to_string()
            width = dataset.profile["width"]
            height = dataset.profile["height"]
            no_data = dataset.nodata if dataset.nodata is not None else ~sys.maxsize
            bounds = dataset.bounds

            band1 = dataset.read(1)
            band_min = band1.min()
            band_max = band1.max()

            new_row = {'filename': tif_file, 'crs': crs, 'width': width, 'height': height, 'bounds': bounds,
                       'band_min': band_min, 'band_max': band_max, 'nodata': no_data}
            full_metrics_df.loc[len(full_metrics_df)] = new_row

    # consistency_metrics_df = full_metrics_df[['crs', 'width', 'height', 'bounds']]
    consistency_metrics_df = full_metrics_df[['crs', 'width', 'height']]
    unique_consistency_metrics_df = consistency_metrics_df.drop_duplicates()

    # named_consistency_metrics_df = full_metrics_df[['filename', 'crs', 'width', 'height', 'bounds']]
    named_consistency_metrics_df = full_metrics_df[['filename', 'crs', 'width', 'height']]

    return full_metrics_df, named_consistency_metrics_df, unique_consistency_metrics_df


def filter_list_by_another_list(main_list, filter_list):
    return [item for item in main_list if item in filter_list]


def _get_list_of_existing_tifs_to_be_processed(city_data, cif_feature_list):
    filter_list = []
    if 'dem' not in cif_feature_list:
        filter_list.append(city_data.dem_tif_filename)
    if 'dsm' not in cif_feature_list:
        filter_list.append(city_data.dsm_tif_filename)
    if 'tree_canopy' not in cif_feature_list:
        filter_list.append(city_data.tree_canopy_tif_filename)
    if 'lulc' not in cif_feature_list:
        filter_list.append(city_data.lulc_tif_filename)

    return filter_list


def _is_tile_wider_than_half_aoi_side(min_lat, min_lon, max_lat, max_lon, tile_side_meters):
    center_lat = (min_lat + max_lat) / 2
    lon_degree_offset, lat_degree_offset = offset_meters_to_geographic_degrees(center_lat, tile_side_meters)

    is_tile_wider_than_half = False
    if (lon_degree_offset > (max_lon - min_lon)/2) or (lat_degree_offset > (max_lat - min_lat)/2):
        is_tile_wider_than_half = True

    return is_tile_wider_than_half


def validate_basic_inputs(source_base_path, target_path, city_folder_name):
    invalids = verify_fundamental_paths(source_base_path, target_path, city_folder_name)
    if invalids:
        print('\n')
        _highlighted_print('------------ Invalid source/target folders ------------ ')
        for invalid in invalids:
            print(invalid)
        raise Exception("Stopped processing due to invalid source/target folders.")
    else:
        return 0

def validate_config_inputs(processing_config_df, source_base_path, target_path, city_folder_name, pre_check_option):
    cell_count, detailed_invalids = _verify_processing_config(processing_config_df, source_base_path, target_path, city_folder_name, pre_check_option)
    if detailed_invalids:
        print('\n')
        _highlighted_print('------------ Invalid configurations ------------ ')
        for invalid in detailed_invalids:
            print(invalid)
        raise Exception("Stopped processing due to invalid configurations.")

    return cell_count, 0

def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')

def offset_meters_to_geographic_degrees(decimal_latitude, length_m):
    earth_radius_m = 6378137
    rad = 180/math.pi

    lon_degree_offset = abs((length_m / (earth_radius_m * math.cos(math.pi*decimal_latitude/180))) * rad)
    lat_degree_offset = abs((length_m / earth_radius_m) * rad)

    return lon_degree_offset, lat_degree_offset