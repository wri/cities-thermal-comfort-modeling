import os.path
import shutil
import time
import rioxarray

from pathlib import Path
from src.constants import FILENAME_ERA5, METHOD_TRIGGER_ERA5_DOWNLOAD, PROCESSING_METHODS
from src.workers.city_data import CityData
from src.workers.worker_tools import create_folder, unpack_quoted_value, reverse_y_dimension_as_needed, save_tiff_file

PROCESSING_PAUSE_TIME_SEC = 10


def process_tile(task_method, source_base_path, target_base_path, city_folder_name, tile_folder_name,
                 cif_primary_features, ctcm_intermediate_features, tile_boundary, tile_resolution, utc_offset):
    tiled_city_data = CityData(city_folder_name, tile_folder_name, source_base_path, target_base_path)
    cif_primary_features = unpack_quoted_value(cif_primary_features)
    ctcm_intermediate_features = unpack_quoted_value(ctcm_intermediate_features)
    tile_resolution = unpack_quoted_value(tile_resolution)
    utc_offset = unpack_quoted_value(utc_offset)
    met_filenames = tiled_city_data.met_filenames

    def _execute_retrieve_cif_data(source_path, target_path, folder_city, folder_tile, cif_features,
                                   boundary, resolution):
        from source_cif_data_downloader import get_cif_data
        cif_stdout = \
            get_cif_data(source_path, target_path, folder_city, folder_tile, cif_features, boundary, resolution)
        return cif_stdout

    def _execute_umep_solweig_only_plugin(step_index, folder_city, folder_tile, source_path, target_path, met_names, offset_utc):
        from umep_plugin_processor import run_plugin

        out_list = []
        for met_file in met_names:
            if met_file.get('filename') == METHOD_TRIGGER_ERA5_DOWNLOAD:
                met_filename = FILENAME_ERA5
            else:
                met_filename = met_file.get('filename')

            solweig_stdout = run_plugin(step_index, 'umep_solweig_only', folder_city,
                                     folder_tile, source_path, target_path, met_filename, offset_utc)
            out_list.append(solweig_stdout)
        return out_list

    def _execute_umep_solweig_plugin_steps(folder_city, folder_tile, source_path, target_path, met_names,
                                           ctcm_intermediate_features, offset_utc):
        from umep_plugin_processor import run_plugin
        out_list = []
        ctcm_intermediates = ctcm_intermediate_features.split(',') if ctcm_intermediate_features is not None else None
        if ctcm_intermediates and ('wallasect' in ctcm_intermediates or 'wallheight' in ctcm_intermediates):
            this_stdout1 = run_plugin(1, 'wall_height_aspect', folder_city, folder_tile,
                       source_path, target_path, None, None)
            out_list.append(this_stdout1)

        if ctcm_intermediates and 'skyview_factor' in ctcm_intermediates:
            this_stdout2 = run_plugin(2, 'skyview_factor', folder_city, folder_tile,
                       source_path, target_path, None, None)
            out_list.append(this_stdout2)

        time.sleep(PROCESSING_PAUSE_TIME_SEC)
        this_stdout3 = _execute_umep_solweig_only_plugin(3, folder_city,
                                                    folder_tile, source_path, target_path, met_names, offset_utc)
        out_list.extend(this_stdout3)

        return out_list

    return_stdouts = []

    # transfer custom files
    if met_filenames:
        _transfer_met_files(tiled_city_data)
    if tiled_city_data.custom_primary_feature_list:
        _transfer_custom_files(tiled_city_data, tiled_city_data.custom_primary_feature_list)
    if tiled_city_data.custom_intermediate_list:
        _transfer_custom_files(tiled_city_data, tiled_city_data.custom_intermediate_list)

    # get cif data
    if cif_primary_features is not None:
        return_val = _execute_retrieve_cif_data(source_base_path, target_base_path, city_folder_name,
                                                tile_folder_name, cif_primary_features, tile_boundary,
                                                tile_resolution)
        return_stdouts.append(return_val)
        time.sleep(PROCESSING_PAUSE_TIME_SEC)

    if task_method != 'download_only':
        # ensure all source TIFF files have negative NS y direction
        ensure_y_dimension_direction(tiled_city_data)

        if task_method == 'umep_solweig':
            return_vals = _execute_umep_solweig_plugin_steps(city_folder_name, tile_folder_name,
                                                             source_base_path, target_base_path, met_filenames,
                                                             ctcm_intermediate_features, utc_offset)
            return_stdouts.extend(return_vals)
        elif task_method == 'umep_solweig_only':
            return_vals = _execute_umep_solweig_only_plugin(1, city_folder_name,
                                         tile_folder_name, source_base_path, target_base_path, met_filenames, utc_offset)
            return_stdouts.extend(return_vals)
        elif task_method in PROCESSING_METHODS:
            from umep_plugin_processor import run_plugin
            return_val = run_plugin(1, task_method, city_folder_name, tile_folder_name,
                       source_base_path, target_base_path, None, None)
            return_stdouts.append(return_val)
        else:
            return ''

    # Construct json of combined return values
    result_str = ','.join(return_stdouts)
    result_json = f'{{"Return_package": [{result_str}]}}'

    return result_json


def _transfer_met_files(city_data):
    create_folder(city_data.target_met_filenames_path)
    for met_file in city_data.met_filenames:
        if met_file != METHOD_TRIGGER_ERA5_DOWNLOAD:
            source_path = os.path.join(city_data.source_met_filenames_path, met_file['filename'])
            target_path = os.path.join(city_data.target_met_filenames_path, met_file['filename'])
            shutil.copyfile(source_path, target_path)


def _transfer_custom_files(city_data, custom_feature_list):
    source_paths = []
    for feature in custom_feature_list:
        if feature == 'dem':
            source_paths.append((city_data.source_dem_path, city_data.target_dem_path))
        elif feature == 'dsm':
            source_paths.append((city_data.source_dsm_path, city_data.target_dsm_path))
        elif feature == 'lulc':
            source_paths.append((city_data.source_land_cover_path, city_data.target_land_cover_path))
        elif feature == 'tree_canopy':
            source_paths.append((city_data.source_tree_canopy_path, city_data.target_tree_canopy_path))
        elif feature == 'wallaspect':
            source_paths.append((city_data.source_wallaspect_path, city_data.target_wallaspect_path))
        elif feature == 'wallheight':
            source_paths.append((city_data.source_wallheight_path, city_data.target_wallheight_path))
        elif feature == 'skyview_factor':
            source_paths.append((city_data.source_svfszip_path, city_data.target_svfszip_path))

    for file_paths in source_paths:
        to_tile_dir = Path(file_paths[1]).parent
        create_folder(to_tile_dir)
        shutil.copyfile(file_paths[0], file_paths[1])


def ensure_y_dimension_direction(city_data):
    tile_data_path = city_data.target_primary_tile_data_path

    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_dem_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_dsm_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_land_cover_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_tree_canopy_path)


def _enforce_tiff_upper_left_origin(tile_data_path, file_path):
    # Open into an xarray.DataArray
    geotiff_da = rioxarray.open_rasterio(file_path)
    geotiff_2d = geotiff_da.sel(band=1).squeeze()
    geotiff_da.close()

    was_reversed, reversed_arr = reverse_y_dimension_as_needed(geotiff_2d)
    if was_reversed:
        data_file = os.path.basename(file_path)
        save_tiff_file(reversed_arr, tile_data_path, data_file)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process tile.')
    parser.add_argument('--task_method', metavar='str', required=True, help='method to run')
    parser.add_argument('--source_base_path', metavar='path', required=True, help='folder for source data')
    parser.add_argument('--target_base_path', metavar='path', required=True, help='folder for writing data')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--tile_folder_name', metavar='str', required=True, help='name of tile folder')
    parser.add_argument('--cif_primary_features', metavar='str', required=True, help='coma-delimited list of cif features to retrieve')
    parser.add_argument('--ctcm_intermediate_features', metavar='str', required=True, help='coma-delimited list of intermediates to be created')
    parser.add_argument('--tile_boundary', metavar='str', required=True, help='geographic boundary of tile')
    parser.add_argument('--tile_resolution', metavar='str', required=True, help='resolution of tile in m.')
    parser.add_argument('--utc_offset', metavar='str', required=True, help='hour offset from utc')

    args = parser.parse_args()

    return_stdout =process_tile(args.task_method, args.source_base_path, args.target_base_path,
                                args.city_folder_name, args.tile_folder_name,
                                args.cif_primary_features, args.ctcm_intermediate_features,
                                args.tile_boundary, args.tile_resolution, args.utc_offset)

    print(return_stdout)

