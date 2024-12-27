import os.path
import shutil
import time
import rioxarray
from city_data import CityData
from src.constants import FILENAME_ERA5, METHOD_TRIGGER_ERA5_DOWNLOAD, PROCESSING_METHODS
from src.workers.worker_tools import create_folder, unpack_quoted_value
from worker_tools import reverse_y_dimension_as_needed, save_tiff_file

PROCESSING_PAUSE_TIME_SEC = 30


def process_tile(task_index, task_method, source_base_path, target_base_path, city_folder_name, tile_folder_name,
                 has_custom_features, cif_features, tile_boundary, tile_resolution, utc_offset):
    has_custom_features = unpack_quoted_value(has_custom_features)
    cif_features = unpack_quoted_value(cif_features)
    tile_resolution = unpack_quoted_value(tile_resolution)
    utc_offset = unpack_quoted_value(utc_offset)
    city_data = CityData(city_folder_name, tile_folder_name, source_base_path, target_base_path)
    met_filenames = city_data.met_filenames

    def _execute_retrieve_cif_data(task_idx, source_path, target_path, folder_city, folder_tile, features,
                                   boundary, resolution):
        from source_cif_data_downloader import get_cif_data
        cif_stdout = \
            get_cif_data(task_idx, source_path, target_path, folder_city, folder_tile, features, boundary, resolution)
        return cif_stdout

    def _execute_solweig_only_plugin(task_idx, step_index, folder_city, folder_tile, source_path, target_path, met_names, offset_utc):
        from umep_plugin_processor import run_plugin

        out_list = []
        for met_file in met_names:
            if met_file.get('filename') == METHOD_TRIGGER_ERA5_DOWNLOAD:
                met_filename = FILENAME_ERA5
            else:
                met_filename = met_file.get('filename')

            solweig_stdout = run_plugin(task_idx, step_index, 'solweig_only', folder_city,
                                     folder_tile, source_path, target_path, met_filename, offset_utc)
            out_list.append(solweig_stdout)
        return out_list

    def _execute_solweig_full_plugin_steps(task_idx, folder_city, folder_tile, source_path, target_path, met_names, offset_utc):
        from umep_plugin_processor import run_plugin
        out_list = []
        this_stdout1 = run_plugin(task_idx, 1, 'wall_height_aspect', folder_city, folder_tile,
                   source_path, target_path, None, None)
        out_list.append(this_stdout1)

        this_stdout2 = run_plugin(task_idx, 2, 'skyview_factor', folder_city, folder_tile,
                   source_path, target_path, None, None)
        out_list.append(this_stdout2)

        time.sleep(PROCESSING_PAUSE_TIME_SEC)
        this_stdout3 = _execute_solweig_only_plugin(task_idx, 3, folder_city,
                                                    folder_tile, source_path, target_path, met_names, offset_utc)
        out_list.extend(this_stdout3)

        return out_list

    return_stdouts = []

    # transfer custom files
    if met_filenames:
        _transfer_met_files(city_data)
    if has_custom_features:
        _transfer_raster_files(city_data)

    # get cif data
    if cif_features is not None:
        return_val = _execute_retrieve_cif_data(task_index, source_base_path, target_base_path, city_folder_name,
                                                tile_folder_name, cif_features, tile_boundary,
                                                tile_resolution)
        return_stdouts.append(return_val)
        time.sleep(PROCESSING_PAUSE_TIME_SEC)

    if task_method != 'cif_download_only':
        # ensure all source TIFF files have negative NS y direction
        ensure_y_dimension_direction(city_data)

        if task_method == 'solweig_full':
            return_vals = _execute_solweig_full_plugin_steps(task_index, city_folder_name, tile_folder_name,
                                                             source_base_path, target_base_path, met_filenames, utc_offset)
            return_stdouts.extend(return_vals)
        elif task_method == 'solweig_only':
            return_vals = _execute_solweig_only_plugin(task_index, 1, city_folder_name,
                                         tile_folder_name, source_base_path, target_base_path, met_filenames, utc_offset)
            return_stdouts.extend(return_vals)
        elif task_method in PROCESSING_METHODS:
            from umep_plugin_processor import run_plugin
            return_val = run_plugin(task_index, 1, task_method, city_folder_name, tile_folder_name,
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


def _transfer_raster_files(city_data):
    for dir in os.listdir(city_data.source_raster_files_path):
        if dir.startswith('tile_'):
            tile_name = dir
            source_raster_paths = []
            for feature in city_data.custom_feature_list:
                if feature == 'dem':
                    source_raster_paths.append((city_data.source_dem_path, city_data.target_dem_path))
                elif feature == 'dsm':
                    source_raster_paths.append((city_data.source_dsm_path, city_data.target_dsm_path))
                elif feature == 'lulc':
                    source_raster_paths.append((city_data.source_land_cover_path, city_data.target_land_cover_path))
                elif feature == 'tree_canopy':
                    source_raster_paths.append((city_data.source_tree_canopy_path, city_data.target_tree_canopy_path))

            to_tile_dir = os.path.join(city_data.target_raster_files_path, tile_name)
            create_folder(to_tile_dir)
            for file_paths in source_raster_paths:
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
    parser.add_argument('--task_index', metavar='str', required=True, help='index from the processor config file')
    parser.add_argument('--task_method', metavar='str', required=True, help='method to run')
    parser.add_argument('--source_base_path', metavar='path', required=True, help='folder for source data')
    parser.add_argument('--target_base_path', metavar='path', required=True, help='folder for writing data')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--tile_folder_name', metavar='str', required=True, help='name of tile folder')
    parser.add_argument('--has_custom_features', metavar='str', required=True, help='indicates if primary source has customer layers')
    parser.add_argument('--cif_features', metavar='str', required=True, help='coma-delimited list of cif features to retrieve')
    parser.add_argument('--tile_boundary', metavar='str', required=True, help='geographic boundary of tile')
    parser.add_argument('--tile_resolution', metavar='str', required=True, help='resolution of tile in m.')
    parser.add_argument('--utc_offset', metavar='str', required=True, help='hour offset from utc')

    args = parser.parse_args()

    return_stdout =process_tile(args.task_index, args.task_method, args.source_base_path, args.target_base_path,
                                args.city_folder_name, args.tile_folder_name, args.has_custom_features,
                                args.cif_features, args.tile_boundary, args.tile_resolution, args.utc_offset)

    print(return_stdout)

