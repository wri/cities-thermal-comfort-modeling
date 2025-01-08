import math
import numbers
import os
import rasterio
import pandas as pd
import geopandas as gpd
import shapely

from shapely.geometry import box
from src.constants import FILENAME_METHOD_YML_CONFIG, \
    FOLDER_NAME_PRIMARY_RASTER_FILES, METHOD_TRIGGER_ERA5_DOWNLOAD, PROCESSING_METHODS
from src.worker_manager.tools import get_aoi_area_in_square_meters, get_existing_tile_metrics, list_files_with_extension, \
    get_distance_between_geographic_points
from src.workers.city_data import CityData


def _verify_fundamental_paths(source_base_path, target_path, city_folder_name):
    invalids = []
    if _verify_path(source_base_path) is False:
        msg = f'Invalid source base path: {source_base_path}'
        invalids.append([msg, True])

    city_path = os.path.join(source_base_path, city_folder_name)
    if _verify_path(city_path) is False:
        msg = f'Invalid source city path: {city_path}'
        invalids.append([msg, True])

    if _verify_path(target_path) is False:
        msg = f'Invalid target base path: {target_path}'
        invalids.append([msg, True])

    if source_base_path == target_path:
        msg = f'Source and target base paths cannot be the same.'
        invalids.append([msg, True])

    if invalids:
        return invalids

    return invalids


def _verify_path(path):
    is_valid = os.path.exists(path)
    return is_valid


def _verify_processing_config(source_base_path, target_base_path, city_folder_name, processing_option):
    # Gather parameters to be evaluated
    source_city_path = str(os.path.join(source_base_path, city_folder_name))

    non_tiled_city_data = CityData(city_folder_name, None, source_base_path, target_base_path)

    # AOI metrics
    utc_offset = non_tiled_city_data.utc_offset
    aoi_min_lon = non_tiled_city_data.min_lon
    aoi_min_lat = non_tiled_city_data.min_lat
    aoi_max_lon = non_tiled_city_data.max_lon
    aoi_max_lat = non_tiled_city_data.max_lat
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer_meters = non_tiled_city_data.tile_buffer_meters

    # Source files
    custom_primary_features = non_tiled_city_data.custom_primary_feature_list
    custom_primary_filenames = non_tiled_city_data.custom_primary_filenames
    cif_features = non_tiled_city_data.cif_primary_feature_list
    custom_intermediate_features = non_tiled_city_data.custom_intermediate_list

    cell_count = None
    task_method = non_tiled_city_data.new_task_method

    invalids = []
    if task_method not in PROCESSING_METHODS:
        msg = f"Invalid 'method' value ({task_method}) in {FILENAME_METHOD_YML_CONFIG} file. Valid values: {PROCESSING_METHODS}"
        invalids.append([msg, True])

    # Evaluate AOI
    if (not isinstance(aoi_min_lon, numbers.Number) or not isinstance(aoi_min_lat, numbers.Number) or
            not isinstance(aoi_max_lon, numbers.Number) or not isinstance(aoi_max_lat, numbers.Number)):
        msg = f'Parameters in NewProcessingAOI section must be defined in {FILENAME_METHOD_YML_CONFIG}'
        invalids.append([msg, True])

    if not (-180 <= aoi_min_lon <= 180) or not (-180 <= aoi_max_lon <= 180):
        msg = f'Min and max longitude values must be between -180 and 180 in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append([msg, True])

    if not (-90 <= aoi_min_lat <= 90) or not (-90 <= aoi_max_lat <= 90):
        msg = f'Min and max latitude values must be between -90 and 90 in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append([msg, True])

    if not (aoi_min_lon <= aoi_max_lon):
        msg = f'Min longitude must be less than max longitude in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append([msg, True])

    if not (aoi_min_lat <= aoi_max_lat):
        msg = f'Min latitude must be less than max latitude in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append([msg, True])

    # TODO improve this evaluation
    if abs(aoi_max_lon - aoi_min_lon) > 0.3 or abs(aoi_max_lon - aoi_min_lon) > 0.3:
        msg = f'Specified AOI must be less than 30km on a side in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append([msg, True])

    if not custom_primary_features:
        if tile_side_meters is not None:
            is_tile_wider_than_half = _is_tile_wider_than_half_aoi_side(aoi_min_lat, aoi_min_lon, aoi_max_lat,
                                                                        aoi_max_lon, tile_side_meters)
            if is_tile_wider_than_half:
                msg = (f"Requested tile_side_meters cannot be larger than half the AOI side length in {FILENAME_METHOD_YML_CONFIG}."
                       f" Specify None if you don't want to subdivide the aoi.")
                invalids.append(msg)

        if tile_side_meters is not None and tile_side_meters < 150:
            msg = (f"Requested tile_side_meters must be 100 meters or more in {FILENAME_METHOD_YML_CONFIG}. "
                   f"Specify None if you don't want to subdivide the aoi.")
            invalids.append([msg, True])

        if tile_side_meters is not None and int(tile_side_meters) <= 10:
            msg = (f"tile_side_meters must be greater than 10 in {FILENAME_METHOD_YML_CONFIG}. "
                   f"Specify None if you don't want to subdivide the aoi.")
            invalids.append([msg, True])

        if tile_buffer_meters is not None and int(tile_buffer_meters) <= 10:
            msg = (f"tile_buffer_meters must be greater than 10 in {FILENAME_METHOD_YML_CONFIG}. "
                   f"Specify None if you don't want to subdivide the aoi.")
            invalids.append([msg, True])

        if tile_buffer_meters is not None and int(tile_buffer_meters) > 500:
            msg = (f"tile_buffer_meters must be less than 500 in {FILENAME_METHOD_YML_CONFIG}. S"
                   f"pecify None if you don't want to subdivide the aoi.")
            invalids.append([msg, True])

        if tile_side_meters is None and tile_buffer_meters is not None:
            msg = f"tile_buffer_meters must be None if tile_sider_meters is None in {FILENAME_METHOD_YML_CONFIG}."
            invalids.append([msg, True])

        if non_tiled_city_data.max_lat is not None and non_tiled_city_data.max_lon is not None:
            # Infer raster cell count from aoi
            square_meters = get_aoi_area_in_square_meters(non_tiled_city_data.min_lon, non_tiled_city_data.min_lat,
                                                          non_tiled_city_data.max_lon, non_tiled_city_data.max_lat)
            # Assume 1-meter resolution of target cif files
            cell_count = math.ceil(square_meters)


    # check that resolution and size of raster files in tile are consistent
    updated_aoi = None
    if custom_primary_features:
        if tile_side_meters:
            msg = f"tile_side_meters cannot be specified for a dataset with customer primary raster files."
            invalids.append([msg, True])

        if tile_buffer_meters:
            msg = f"tile_buffer_meters cannot be specified for a dataset with customer primary raster files."
            invalids.append([msg, True])

        existing_tiles_metrics = get_existing_tile_metrics(source_city_path, custom_primary_filenames, project_to_wgs84=True)

        tile_res_counts = existing_tiles_metrics.groupby('tile_name')['avg_res'].nunique().reset_index(name='unique_avg_res_count')
        non_consistent_res = tile_res_counts[tile_res_counts['unique_avg_res_count'] > 1]
        if non_consistent_res.shape[0] > 0:
            non_consistent_tiles = ', '.join(non_consistent_res['tile_name'])
            msg = f"Inconsistent raster resolutions found in files in these tiles: {non_consistent_tiles}"
            invalids.append([msg, True])

        tile_bounds_counts = existing_tiles_metrics.groupby('tile_name')['boundary'].nunique().reset_index(name='unique_boundary_count')
        non_consistent_boundary = tile_bounds_counts[tile_bounds_counts['unique_boundary_count'] > 1]
        if non_consistent_res.shape[0] > 0:
            non_consistent_tiles = ', '.join(non_consistent_boundary['tile_name'])
            msg = f"Inconsistent raster boundary found in files in these tiles: {non_consistent_tiles}"
            invalids.append([msg, True])

        unique_tile_names = pd.DataFrame(existing_tiles_metrics['tile_name'].unique(), columns=['tile_name'])
        for idx, tile_row in unique_tile_names.iterrows():
            tile_folder_name = tile_row['tile_name']
            try:
                tiled_city_data = CityData(city_folder_name, tile_folder_name, source_base_path, target_base_path)
            except Exception as e_msg:
                invalids.append(e_msg)
                break

            prior_dem = tiled_city_data.source_dem_path
            prior_dsm = tiled_city_data.source_dsm_path
            prior_lulc = tiled_city_data.source_land_cover_path
            prior_tree_canopy = tiled_city_data.source_tree_canopy_path

            if cif_features is not None and 'dem' not in cif_features and _verify_path(prior_dem) is False:
                msg = f'Specified custom source file: {prior_dem} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
                invalids.append([msg, True])

            if cif_features is not None and 'dsm' not in cif_features and _verify_path(prior_dsm) is False:
                msg = f'Specified custom  source file: {prior_dsm} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
                invalids.append([msg, True])

            if cif_features is not None and 'lulc' not in cif_features and _verify_path(prior_lulc) is False:
                msg = f'Specified custom  source file: {prior_lulc} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
                invalids.append([msg, True])

            if cif_features is not None and 'tree_canopy' not in cif_features and _verify_path(prior_tree_canopy) is False:
                msg = f'Specified custom  source file: {prior_tree_canopy} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
                invalids.append([msg, True])

            if task_method in PROCESSING_METHODS:
                prior_tree_canopy = tiled_city_data.source_tree_canopy_path
                if cif_features is not None and 'tree_canopy' not in cif_features and _verify_path(prior_tree_canopy) is False:
                    msg = f'Required source file: {prior_tree_canopy} not found as needed for method: {task_method} as specified in {FILENAME_METHOD_YML_CONFIG} file.'
                    invalids.append([msg, True])

            if task_method in ['umep_solweig']:
                prior_land_cover = tiled_city_data.source_land_cover_path
                prior_dem = tiled_city_data.source_dem_path
                if cif_features is not None and 'lulc' not in cif_features and _verify_path(prior_land_cover) is False:
                    msg = (f'Required source file: {prior_land_cover} not found for method: {task_method} as '
                           f'specified in {FILENAME_METHOD_YML_CONFIG} file.')
                    invalids.append([msg, True])
                if cif_features is not None and 'dem' not in cif_features and _verify_path(prior_dem) is False:
                    msg = (f'Required source file: {prior_dem} not found for method: {task_method} as '
                           f'specified in {FILENAME_METHOD_YML_CONFIG} file.')
                    invalids.append([msg, True])
                for met_file_row in tiled_city_data.met_filenames:
                    met_file = met_file_row.get('filename')
                    met_filepath = os.path.join(tiled_city_data.source_met_filenames_path, met_file)
                    if met_file != '<download_era5>' and _verify_path(met_filepath) is False:
                        msg = (f'Required meteorological file: {met_filepath} not found for '
                               f'method: {task_method} in .config_method_parameters.yml.')
                        invalids.append([msg, True])

                if not -24 <= utc_offset <= 24:
                    msg = f'UTC-offset for: {met_file} not in -24 to 24 hours range as specified in .config_method_parameters.yml.'
                    invalids.append([msg, True])

                if task_method in ['umep_solweig_only']:
                    prior_svfszip = tiled_city_data.target_svfszip_path
                    prior_wallheight = tiled_city_data.target_wallheight_path
                    prior_wallaspect = tiled_city_data.target_wallaspect_path
                    if _verify_path(prior_svfszip) is False:
                        msg = (f'Required source file: {prior_svfszip} currently not found for method: {task_method} '
                               f'as specified in {FILENAME_METHOD_YML_CONFIG} file.')
                        invalids.append([msg, True])
                    if _verify_path(prior_wallheight) is False:
                        msg = (f'Required source file: {prior_wallheight} currently not found for method: {task_method} '
                               f'as specified in {FILENAME_METHOD_YML_CONFIG} file.')
                        invalids.append([msg, True])
                    if _verify_path(prior_wallaspect) is False:
                        msg = (f'Required source file: {prior_wallaspect} currently not found for method: {task_method} '
                               f'as specified in {FILENAME_METHOD_YML_CONFIG} file.')
                        invalids.append([msg, True])

                full_metrics_df, named_consistency_metrics_df, unique_consistency_metrics_df = (
                    _get_parameters_for_custom_tif_files(tiled_city_data, tile_folder_name, cif_features))

                if full_metrics_df['nodata'].isnull().any():
                    files_with_nan = full_metrics_df.loc[full_metrics_df['nodata'].isnull(), 'filename'].tolist()
                    files_with_nan_str = ','.join(map(str,files_with_nan))
                    msg = (f"Folder {tile_folder_name} and possibly other folders has forbidden no_data='nan' "
                           f"in file(s) ({files_with_nan_str}).")
                    invalids.append([msg, True])

                if 'lulc' in custom_primary_features:
                    lulc_metrics = full_metrics_df.loc[full_metrics_df['filename'] == tiled_city_data.lulc_tif_filename]
                    if lulc_metrics is not None:
                        band_min = lulc_metrics['band_min'].values[0]
                        band_max = lulc_metrics['band_max'].values[0]
                        if band_min < 1 or band_max > 7:
                            msg = (f"Folder {tile_folder_name} and possibly other folders has "
                                   f"LULC ({tiled_city_data.lulc_tif_filename}) with values outside of range 1-7.")
                            invalids.append([msg, True])

                if unique_consistency_metrics_df.shape[0] > 1:
                    msg = (f'TIF files in folder {tile_folder_name} and possibly other folders has '
                           f'inconsistent parameters with {unique_consistency_metrics_df.shape[0]} unique parameter variants.')
                    invalids.append([msg, True])

                    msg = f'TIF parameters: {named_consistency_metrics_df.to_json(orient='records')}'
                    invalids.append([msg, True])

                    msg = 'Stopping analysis at first set of inconsistent TIF files.'
                    invalids.append([msg, True])

            # Get representative cell count
            cell_count = existing_tiles_metrics['cell_count'][0]

            # Only report AOI issues if there are no other failures
            aoi_invalids, updated_aoi = _evaluate_aoi_discrepancy(existing_tiles_metrics, aoi_min_lon, aoi_min_lat,
                                                                  aoi_max_lon, aoi_max_lat, processing_option)
            if aoi_invalids:
                invalids.append(aoi_invalids)


    if custom_intermediate_features:
        # wall dependencies
        if not ('wallaspect' in custom_intermediate_features and 'wallheight' in custom_intermediate_features):
            msg = 'Both wall_aspect_filename and wall_height_filename must both be specified if one of them is specified as not None.'
            invalids.append([msg, True])
        else:
            if 'dsm' in non_tiled_city_data.cif_primary_feature_list:
                msg = 'dsm_tif_filename cannot be None if wallaspect and wallheight are not None due, to dependency conflict.'
                invalids.append([msg, True])

        if 'skyview_factor' in custom_intermediate_features and 'dsm' in non_tiled_city_data.cif_primary_feature_list:
            msg = 'dsm_tif_filename cannot be None if skyview_factor_filename is not None, due to dependency conflict.'
            invalids.append([msg, True])

        if 'skyview_factor' in custom_intermediate_features and 'tree_canopy' in non_tiled_city_data.cif_primary_feature_list:
            msg = 'tree_canopy_tif_filename cannot be None if skyview_factor_filename is not None, due to dependency conflict.'
            invalids.append([msg, True])

        #TODO ensure that the dsm has not changed since last run using checksum???


    target_scenario_path = non_tiled_city_data.target_city_path

    return target_scenario_path, cell_count, updated_aoi, invalids

def _evaluate_aoi_discrepancy(existing_tiles_metrics, aoi_min_lon, aoi_min_lat, aoi_max_lon, aoi_max_lat, pre_check_option):
    max_tolerance_meters = 100

    tile_grid_total_bounds = gpd.GeoSeries(box(*existing_tiles_metrics.total_bounds))
    tile_grid_min_lon = round(tile_grid_total_bounds.geometry.bounds.minx[0],7)
    tile_grid_min_lat = round(tile_grid_total_bounds.geometry.bounds.miny[0],7)
    tile_grid_max_lon = round(tile_grid_total_bounds.geometry.bounds.maxx[0],7)
    tile_grid_max_lat = round(tile_grid_total_bounds.geometry.bounds.maxy[0],7)

    sw_distance = get_distance_between_geographic_points(aoi_min_lon, aoi_min_lat, tile_grid_min_lon, tile_grid_min_lat)
    ne_distance = get_distance_between_geographic_points(aoi_max_lon, aoi_max_lat, tile_grid_max_lon, tile_grid_max_lat)

    aoi_invalids = None
    updated_aoi = None
    if sw_distance > 0 or ne_distance > 0:
        max_offset = sw_distance if sw_distance >= ne_distance else ne_distance
        max_offset = round(max_offset, 2)

        aoi_notice = _construct_aoi_update_notice(tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat)
        if pre_check_option == 'pre_check':
            msg = (f'WARNING: Tile-grid extent differs from AOI specification in yml file. Investigate the discrepancy '
                   f'and consider updating the yml with: {aoi_notice}'
                   )
            aoi_invalids = [msg, False]
        else:
            if 0 < max_offset < max_tolerance_meters:
                # Notify user of offset and automatically update target yml
                msg = (f'WARNING: Tile-grid extent marginally differs from AOI specification in yml file with '
                       f'corner offset of at least {max_offset} meters. Automatically updating the target yml file to the tile-grid extent. '
                       f'Use the updated coordinates to avoid this warning in the future.')
                aoi_invalids = [msg, False]
                updated_aoi = shapely.box(tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat)
            else:
                # Stop processing and notify user
                msg = (f'Tile-grid extent substantially differs from AOI specification in yml file by maximum '
                       f'corner offset of at least {max_offset} meters. Investigate the discrepancy and consider '
                       f'updating the yml with: {aoi_notice}'
                       )
                aoi_invalids = [msg, True]

    return aoi_invalids, updated_aoi

def _construct_aoi_update_notice(tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat):
    aoi_notice = (f'\n  min_lon: {tile_grid_min_lon}\n  min_lat: {tile_grid_min_lat}'
                  f'\n  max_lon: {tile_grid_max_lon}\n  max_lat: {tile_grid_max_lat}')
    return aoi_notice


def _get_parameters_for_custom_tif_files(city_data, tile_folder_name, cif_feature_list):
    import sys

    tile_folder = os.path.join(city_data.source_city_primary_data_path, FOLDER_NAME_PRIMARY_RASTER_FILES,
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
    invalids = _verify_fundamental_paths(source_base_path, target_path, city_folder_name)
    if invalids:
        print('\n')
        print(' vvvvvvvvvvvv Invalid source/target folders vvvvvvvvvvvv ')
        _print_invalids(invalids)
        print(' ^^^^^^^^^^^^ Invalid source/target folders ^^^^^^^^^^^^ ')
        print('\n')
        raise Exception('Invalid configurations')
    else:
        return 0


def _invalid_has_fatal_error(detailed_invalids):
    unique_fatal_error = {t[1] for t in detailed_invalids}
    has_fatal_error = {True}.issubset(unique_fatal_error)
    return has_fatal_error


def validate_config_inputs(source_base_path, target_path, city_folder_name, processing_option):
    target_scenario_path, cell_count, update_aoi, detailed_invalids = (
        _verify_processing_config(source_base_path, target_path, city_folder_name, processing_option))
    if detailed_invalids:
        has_fatal_error = True if _invalid_has_fatal_error(detailed_invalids) else False

        head_msg = ' vvvvvvvvvvvv Invalid configurations vvvvvvvvvvvv '
        tail_msg = ' ^^^^^^^^^^^^ Invalid configurations ^^^^^^^^^^^^ '

        print('\n')
        _print_banner(has_fatal_error, head_msg)
        _print_invalids(detailed_invalids)
        _print_banner(has_fatal_error, tail_msg)
        print('\n')

        if has_fatal_error:
            raise Exception('Invalid configurations')
        else:
            return target_scenario_path, cell_count, update_aoi, 0
    else:
        return target_scenario_path, cell_count, update_aoi, 0


def _print_banner(has_fatal_error, msg):
    if has_fatal_error:
        print(msg)
    else:
        print((msg))


def _print_invalids(invalids):
    i=1
    for invalid in invalids:
        print(f'{i}) {invalid[0]}')
        i +=1


def offset_meters_to_geographic_degrees(decimal_latitude, length_m):
    earth_radius_m = 6378137
    rad = 180/math.pi

    lon_degree_offset = abs((length_m / (earth_radius_m * math.cos(math.pi*decimal_latitude/180))) * rad)
    lat_degree_offset = abs((length_m / earth_radius_m) * rad)

    return lon_degree_offset, lat_degree_offset