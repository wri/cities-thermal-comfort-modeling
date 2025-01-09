import math
import os
import rasterio
import pandas as pd

from src.constants import FILENAME_METHOD_YML_CONFIG, \
    FOLDER_NAME_PRIMARY_RASTER_FILES, METHOD_TRIGGER_ERA5_DOWNLOAD, PROCESSING_METHODS
from src.data_validation.tools import verify_path
from src.worker_manager.tools import  list_files_with_extension
from src.workers.city_data import CityData


def evaluate_custom_primary_config(non_tiled_city_data, existing_tiles_metrics):
    invalids = []

    custom_primary_features = non_tiled_city_data.custom_primary_feature_list
    if not custom_primary_features:
        return invalids

    source_base_path = non_tiled_city_data.source_base_path
    target_base_path = non_tiled_city_data.target_base_path
    city_folder_name = non_tiled_city_data.folder_name_city_data

    utc_offset = non_tiled_city_data.utc_offset
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer_meters = non_tiled_city_data.tile_buffer_meters

    cif_features = non_tiled_city_data.cif_primary_feature_list
    task_method = non_tiled_city_data.new_task_method


    if tile_side_meters:
        msg = f"tile_side_meters cannot be specified for a dataset with custom primary raster files."
        invalids.append((msg, True))

    if tile_buffer_meters:
        msg = f"tile_buffer_meters cannot be specified for a dataset with custom primary raster files."
        invalids.append((msg, True))

    tile_res_counts = existing_tiles_metrics.groupby('tile_name')['avg_res'].nunique().reset_index(name='unique_avg_res_count')
    non_consistent_res = tile_res_counts[tile_res_counts['unique_avg_res_count'] > 1]
    if non_consistent_res.shape[0] > 0:
        non_consistent_tiles = ', '.join(non_consistent_res['tile_name'])
        msg = f"Inconsistent raster resolutions found in files in these tiles: {non_consistent_tiles}"
        invalids.append((msg, True))

    tile_bounds_counts = existing_tiles_metrics.groupby('tile_name')['boundary'].nunique().reset_index(name='unique_boundary_count')
    non_consistent_boundary = tile_bounds_counts[tile_bounds_counts['unique_boundary_count'] > 1]
    if non_consistent_res.shape[0] > 0:
        non_consistent_tiles = ', '.join(non_consistent_boundary['tile_name'])
        msg = f"Inconsistent raster boundary found in files in these tiles: {non_consistent_tiles}"
        invalids.append((msg, True))

    # Loop through tiles
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

        if cif_features is not None and 'dem' not in cif_features and verify_path(prior_dem) is False:
            msg = f'Specified custom source file: {prior_dem} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
            invalids.append((msg, True))

        if cif_features is not None and 'dsm' not in cif_features and verify_path(prior_dsm) is False:
            msg = f'Specified custom  source file: {prior_dsm} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
            invalids.append((msg, True))

        if cif_features is not None and 'lulc' not in cif_features and verify_path(prior_lulc) is False:
            msg = f'Specified custom  source file: {prior_lulc} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
            invalids.append((msg, True))

        if cif_features is not None and 'tree_canopy' not in cif_features and verify_path(prior_tree_canopy) is False:
            msg = f'Specified custom  source file: {prior_tree_canopy} not found as specified in {FILENAME_METHOD_YML_CONFIG} file.'
            invalids.append((msg, True))

        if task_method in PROCESSING_METHODS:
            prior_tree_canopy = tiled_city_data.source_tree_canopy_path
            if cif_features is not None and 'tree_canopy' not in cif_features and verify_path(prior_tree_canopy) is False:
                msg = f'Required source file: {prior_tree_canopy} not found as needed for method: {task_method} as specified in {FILENAME_METHOD_YML_CONFIG} file.'
                invalids.append((msg, True))

        if task_method in ['umep_solweig']:
            prior_land_cover = tiled_city_data.source_land_cover_path
            prior_dem = tiled_city_data.source_dem_path
            if cif_features is not None and 'lulc' not in cif_features and verify_path(prior_land_cover) is False:
                msg = (f'Required source file: {prior_land_cover} not found for method: {task_method} as '
                       f'specified in {FILENAME_METHOD_YML_CONFIG} file.')
                invalids.append((msg, True))
            if cif_features is not None and 'dem' not in cif_features and verify_path(prior_dem) is False:
                msg = (f'Required source file: {prior_dem} not found for method: {task_method} as '
                       f'specified in {FILENAME_METHOD_YML_CONFIG} file.')
                invalids.append((msg, True))
            for met_file_row in tiled_city_data.met_filenames:
                met_file = met_file_row.get('filename')
                met_filepath = os.path.join(tiled_city_data.source_met_filenames_path, met_file)

                if met_file != '<download_era5>' and verify_path(met_filepath) is False:
                    msg = (f'Required meteorological file: {met_filepath} not found for '
                           f'method: {task_method} in .config_method_parameters.yml.')
                    invalids.append((msg, True))

            if not -24 <= utc_offset <= 24:
                msg = f'UTC-offset for: {met_file} not in -24 to 24 hours range as specified in .config_method_parameters.yml.'
                invalids.append((msg, True))

            if task_method in ['umep_solweig_only']:
                prior_svfszip = tiled_city_data.target_svfszip_path
                prior_wallheight = tiled_city_data.target_wallheight_path
                prior_wallaspect = tiled_city_data.target_wallaspect_path
                if verify_path(prior_svfszip) is False:
                    msg = (f'Required source file: {prior_svfszip} currently not found for method: {task_method} '
                           f'as specified in {FILENAME_METHOD_YML_CONFIG} file.')
                    invalids.append((msg, True))
                if verify_path(prior_wallheight) is False:
                    msg = (f'Required source file: {prior_wallheight} currently not found for method: {task_method} '
                           f'as specified in {FILENAME_METHOD_YML_CONFIG} file.')
                    invalids.append((msg, True))
                if verify_path(prior_wallaspect) is False:
                    msg = (f'Required source file: {prior_wallaspect} currently not found for method: {task_method} '
                           f'as specified in {FILENAME_METHOD_YML_CONFIG} file.')
                    invalids.append((msg, True))

            full_metrics_df, named_consistency_metrics_df, unique_consistency_metrics_df = (
                _get_parameters_for_custom_tif_files(tiled_city_data, tile_folder_name, cif_features))

            if full_metrics_df['nodata'].isnull().any():
                files_with_nan = full_metrics_df.loc[full_metrics_df['nodata'].isnull(), 'filename'].tolist()
                files_with_nan_str = ','.join(map(str,files_with_nan))
                msg = (f"Folder {tile_folder_name} and possibly other folders has forbidden no_data='nan' "
                       f"in file(s) ({files_with_nan_str}).")
                invalids.append((msg, True))

            if 'lulc' in custom_primary_features:
                lulc_metrics = full_metrics_df.loc[full_metrics_df['filename'] == tiled_city_data.lulc_tif_filename]
                if lulc_metrics is not None:
                    band_min = lulc_metrics['band_min'].values[0]
                    band_max = lulc_metrics['band_max'].values[0]
                    if band_min < 1 or band_max > 7:
                        msg = (f"Folder {tile_folder_name} and possibly other folders has "
                               f"LULC ({tiled_city_data.lulc_tif_filename}) with values outside of range 1-7.")
                        invalids.append((msg, True))

            if unique_consistency_metrics_df.shape[0] > 1:
                msg = (f'TIF files in folder {tile_folder_name} and possibly other folders has '
                       f'inconsistent parameters with {unique_consistency_metrics_df.shape[0]} unique parameter variants.')
                invalids.append((msg, True))

                msg = f'TIF parameters: {named_consistency_metrics_df.to_json(orient='records')}'
                invalids.append((msg, True))

                msg = 'Stopping analysis at first set of inconsistent TIF files.'
                invalids.append((msg, True))

        #TODO ensure that the dsm has not changed since last run using checksum???


    return invalids


def _get_parameters_for_custom_tif_files(city_data, tile_folder_name, cif_feature_list):
    import sys

    tile_folder = os.path.join(city_data.source_city_primary_data_path, FOLDER_NAME_PRIMARY_RASTER_FILES,
                               tile_folder_name)
    tif_files = list_files_with_extension(tile_folder, '.tif')

    processing_list = _get_list_of_existing_tifs_to_be_processed(city_data, cif_feature_list)

    filtered_existing_list = _filter_list_by_another_list(tif_files, processing_list)

    full_metrics_df = pd.DataFrame(columns=['filename', 'crs', 'width', 'height', 'bounds', 'band_min', 'band_max', 'nodata'])
    for tif_file in filtered_existing_list:
        tif_file_path = os.path.join(str(tile_folder), str(tif_file))
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

    consistency_metrics_df = full_metrics_df[['crs', 'width', 'height']]
    unique_consistency_metrics_df = consistency_metrics_df.drop_duplicates()

    named_consistency_metrics_df = full_metrics_df[['filename', 'crs', 'width', 'height']]

    return full_metrics_df, named_consistency_metrics_df, unique_consistency_metrics_df


def _filter_list_by_another_list(main_list, filter_list):
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