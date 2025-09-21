import os.path
import shutil
import time
import rioxarray

from pathlib import Path

from src.constants import FILENAME_ERA5_UMEP, METHOD_TRIGGER_ERA5_DOWNLOAD, FILENAME_ERA5_UPENN
from src.workers.city_data import CityData
from src.workers.model_upenn.upenn_module_processor import run_upenn_module
from src.workers.worker_dao import cache_tile_files
from src.workers.worker_tools import create_folder, unpack_quoted_value, save_tiff_file, remove_file, \
    ctcm_standardize_y_dimension_direction

PROCESSING_PAUSE_TIME_SEC = 10

def process_tile(city_json_str, task_method, source_base_path, target_base_path, city_folder_name, tile_folder_name,
                 cif_primary_features, ctcm_intermediate_features, tile_boundary, tile_resolution, seasonal_utc_offset, grid_crs):
    tiled_city_data = CityData(None, city_folder_name, tile_folder_name, source_base_path, target_base_path)
    cif_primary_features = unpack_quoted_value(cif_primary_features)
    custom_primary_features = tiled_city_data.custom_primary_feature_list
    target_tcm_results_path = tiled_city_data.target_tcm_results_path
    ctcm_intermediate_features = unpack_quoted_value(ctcm_intermediate_features)
    tile_resolution = unpack_quoted_value(tile_resolution)
    seasonal_utc_offset = unpack_quoted_value(seasonal_utc_offset)
    met_filenames = tiled_city_data.met_filenames

    def _execute_retrieve_cif_data(cif_city_json_str, source_path, target_path, folder_city, folder_tile, cif_features,
                                   boundary, resolution, grid_crs):
        from source_cif_data_downloader import get_cif_data
        cif_stdout = \
            get_cif_data(cif_city_json_str, source_path, target_path, folder_city, folder_tile, cif_features, boundary,
                         resolution, grid_crs)
        return cif_stdout

    def _execute_mrt_method(step_index, task_method, folder_city, folder_tile, source_path, target_path, met_names, offset_utc):
        from src.workers.model_umep.umep_plugin_processor import run_umep_plugin

        out_list = []
        for met_filename in met_names:
            if met_filename == METHOD_TRIGGER_ERA5_DOWNLOAD:
                met_filename = FILENAME_ERA5_UPENN if task_method == 'upenn_model' else FILENAME_ERA5_UMEP
            else:
                met_filename = met_filename

            if task_method == 'umep_solweig':
                stdout = run_umep_plugin(step_index, task_method, folder_city, folder_tile, source_path, target_path,
                                         met_filename, offset_utc)
            else:
                stdout = run_upenn_module(step_index, task_method, folder_city, folder_tile, source_path, target_path,
                                         met_filename, offset_utc)

            out_list.append(stdout)
        return out_list

    def _execute_umep_solweig_plugin_steps(folder_city, folder_tile, source_path, target_path, met_names,
                                           ctcm_intermediate_features, offset_utc):
        from src.workers.model_umep.umep_plugin_processor import run_umep_plugin
        out_list = []
        ctcm_intermediates = ctcm_intermediate_features.split(',') if ctcm_intermediate_features is not None else None
        if ctcm_intermediates and ('wallasect' in ctcm_intermediates or 'wallheight' in ctcm_intermediates):
            this_stdout1 = run_umep_plugin(1, 'wall_height_aspect', folder_city, folder_tile,
                                           source_path, target_path, None, None)
            out_list.append(this_stdout1)

        if ctcm_intermediates and 'skyview_factor' in ctcm_intermediates:
            this_stdout2 = run_umep_plugin(2, 'skyview_factor', folder_city, folder_tile,
                                           source_path, target_path, None, None)
            out_list.append(this_stdout2)

        time.sleep(PROCESSING_PAUSE_TIME_SEC)
        this_stdout3 = _execute_mrt_method(3, 'umep_solweig', folder_city,
                                                    folder_tile, source_path, target_path, met_names, offset_utc)
        out_list.extend(this_stdout3)

        return out_list

    def _execute_upenn_model_steps(folder_city, folder_tile, source_path, target_path, met_names,
                                           ctcm_intermediate_features, offset_utc):
        out_list = []
        ctcm_intermediates = ctcm_intermediate_features.split(',') if ctcm_intermediate_features is not None else None
        if ctcm_intermediates and ('wallasect' in ctcm_intermediates or 'wallheight' in ctcm_intermediates):
            this_stdout1 = run_upenn_module(1, 'wall_height_aspect', folder_city, folder_tile,
                                            source_path, target_path, None, None)

            out_list.append(this_stdout1)

        if ctcm_intermediates and 'skyview_factor' in ctcm_intermediates:
            this_stdout2 = run_upenn_module(2, 'skyview_factor', folder_city, folder_tile,
                                            source_path, target_path, None, None)
            out_list.append(this_stdout2)

        time.sleep(PROCESSING_PAUSE_TIME_SEC)
        this_stdout3 = _execute_mrt_method(3, 'mrt', folder_city,
                                                    folder_tile, source_path, target_path, met_names, offset_utc)
        out_list.extend(this_stdout3)

        return out_list

    # transfer custom files
    if tiled_city_data.custom_primary_feature_list:
        _transfer_custom_files(tiled_city_data, tiled_city_data.custom_primary_feature_list)
    if tiled_city_data.custom_intermediate_list:
        _transfer_custom_files(tiled_city_data, tiled_city_data.custom_intermediate_list)

    # get cif data
    return_stdouts = []
    if cif_primary_features is not None:
        return_val = _execute_retrieve_cif_data(city_json_str, source_base_path, target_base_path, city_folder_name,
                                                tile_folder_name, cif_primary_features, tile_boundary,
                                                tile_resolution, grid_crs)
        return_stdouts.append(return_val)
        time.sleep(PROCESSING_PAUSE_TIME_SEC)

    if task_method != 'download_only':
        # ensure all source TIFF files have negative NS y-direction
        # TODO Commenting out for now until we have a better solution. See https://gfw.atlassian.net/browse/CDB-274
        # ensure_y_dimension_direction(tiled_city_data)

        if task_method == 'umep_solweig':
            return_vals = _execute_umep_solweig_plugin_steps(city_folder_name, tile_folder_name,
                                                             source_base_path, target_base_path, met_filenames,
                                                             ctcm_intermediate_features, seasonal_utc_offset)
            return_stdouts.extend(return_vals)
        elif task_method == 'upenn_model':
            return_vals = _execute_upenn_model_steps(city_folder_name, tile_folder_name,
                                                     source_base_path, target_base_path, met_filenames,
                                                     ctcm_intermediate_features, seasonal_utc_offset)
            return_stdouts.extend(return_vals)
        elif task_method in 'PROCESSING_METHODS':
            from src.workers.model_umep.umep_plugin_processor import run_umep_plugin
            return_val = run_umep_plugin(1, task_method, city_folder_name, tile_folder_name,
                                         source_base_path, target_base_path, None, None)
            return_stdouts.append(return_val)
        else:
            return ''

        # Remove buffered area from mrt results
        buffer_meters = tiled_city_data.tile_buffer_meters
        if len(custom_primary_features) == 0 and tiled_city_data.remove_mrt_buffer_for_final_output is True:
            _trim_mrt_buffer(target_tcm_results_path, tile_folder_name, met_filenames, tile_boundary, buffer_meters)

    # Cache project files in S3
    if tiled_city_data.city_json_str is not None and tiled_city_data.publishing_target in ('s3', 'both'):
        cache_tile_files(tiled_city_data)

    # Construct json of combined return values
    result_str = ','.join(return_stdouts)
    result_json = f'{{"Return_package": [{result_str}]}}'

    return result_json


def _trim_mrt_buffer(target_tcm_results_path, tile_folder_name, met_filenames, tile_boundary, buffer_meters):
    """
    See https://gfw.atlassian.net/browse/CDB-182 for logic behind this function.
    Briefly, the concept is that the code buffers out some hundreds of meters from a tiled area and then clips back to
    the given tiled area. The buffering provides the opportunity for a shadow from a remote building to extend back
    into the tiled area. The technique basically allows the broader context to affect the local space of the given tile.
    TODO investigate variable buffering to further ensure proper inclusion of shadows and improved performance https://gfw.atlassian.net/browse/CDB-206
    """
    from shapely import wkt
    import rasterio
    from rasterio.mask import mask
    import geopandas as gpd
    from shapely.geometry import box

    unbufferred_tile_boundary = wkt.loads(tile_boundary)
    minx, miny, maxx, maxy = unbufferred_tile_boundary.bounds
    west = minx + buffer_meters
    south = miny + buffer_meters
    east = maxx - buffer_meters
    north = maxy - buffer_meters
    bounds = (west, south, east, north)
    polygon = gpd.GeoSeries([box(*bounds)])

    for met_filename in met_filenames:
        file_stem = Path(met_filename).stem
        tile_path = str(os.path.join(target_tcm_results_path, file_stem, tile_folder_name))
        for file in os.listdir(tile_path):
            if file.endswith('.tif'):
                file_path = os.path.join(tile_path, file)
                with rasterio.open(file_path) as src:
                    out_image, out_transform = mask(src, polygon, crop=True)
                    out_meta = src.meta.copy()  # copy the metadata of the source DEM

                out_meta.update({
                    "driver": "COG",
                    "height": out_image.shape[1],  # height starts with shape[1]
                    "width": out_image.shape[2],  # width starts with shape[2]
                    "transform": out_transform
                })

                remove_file(file_path)
                with rasterio.open(file_path, 'w', **out_meta) as dst:
                    dst.write(out_image)
            else:
                continue

def _transfer_custom_files(tiled_city_data, custom_feature_list):
    source_paths = []
    for feature in custom_feature_list:
        if feature == 'albedo_cloud_masked':
            source_paths.append((tiled_city_data.source_albedo_cloud_masked, tiled_city_data.target_albedo_cloud_masked_path, 'file'))
        elif feature == 'dem':
            source_paths.append((tiled_city_data.source_dem_path, tiled_city_data.target_dem_path, 'file'))
        elif feature == 'dsm':
            source_paths.append((tiled_city_data.source_dsm_path, tiled_city_data.target_dsm_path, 'file'))
        elif feature == 'lulc':
            source_paths.append((tiled_city_data.source_lulc_path, tiled_city_data.target_lulc_path, 'file'))
        elif feature == 'open_urban':
            source_paths.append((tiled_city_data.source_open_urban_path, tiled_city_data.target_open_urban_path, 'file'))
        elif feature == 'tree_canopy':
            source_paths.append((tiled_city_data.source_tree_canopy_path, tiled_city_data.target_tree_canopy_path, 'file'))
        elif feature == 'wallaspect':
            source_paths.append((tiled_city_data.source_wallaspect_path, tiled_city_data.target_wallaspect_path, 'file'))
        elif feature == 'wallheight':
            source_paths.append((tiled_city_data.source_wallheight_path, tiled_city_data.target_wallheight_path, 'file'))
        elif feature == 'skyview_factor':
            obj_type = 'folder' if tiled_city_data.new_task_method == 'upenn_model' else 'file'
            source_paths.append((tiled_city_data.source_svfszip_path, tiled_city_data.target_svfszip_path, obj_type))

    for file_paths in source_paths:
        to_tile_dir = Path(file_paths[1]).parent
        create_folder(to_tile_dir)
        if file_paths[2] == 'file':
            shutil.copyfile(file_paths[0], file_paths[1])
        else:
            shutil.copytree(file_paths[0], file_paths[1])


def ensure_y_dimension_direction(city_data):
    tile_data_path = city_data.target_primary_tile_data_path

    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_dem_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_dsm_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_lulc_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_open_urban_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_tree_canopy_path)
    _enforce_tiff_upper_left_origin(tile_data_path, city_data.target_albedo_cloud_masked_path)


def _enforce_tiff_upper_left_origin(tile_data_path, file_path):
    # Open into an xarray.DataArray
    geotiff_da = rioxarray.open_rasterio(file_path)
    geotiff_2d = geotiff_da.sel(band=1).squeeze()
    geotiff_da.close()

    was_reversed, reversed_arr = ctcm_standardize_y_dimension_direction(geotiff_2d)
    if was_reversed:
        data_file = os.path.basename(file_path)
        save_tiff_file(reversed_arr, tile_data_path, data_file)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process tile.')
    parser.add_argument('--city_json_str', metavar='str', required=True, help='city to be processed')
    parser.add_argument('--task_method', metavar='str', required=True, help='method to run')
    parser.add_argument('--source_base_path', metavar='path', required=True, help='folder for source data')
    parser.add_argument('--target_base_path', metavar='path', required=True, help='folder for writing data')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--tile_folder_name', metavar='str', required=True, help='name of tile folder')
    parser.add_argument('--cif_primary_features', metavar='str', required=True, help='coma-delimited list of cif features to retrieve')
    parser.add_argument('--ctcm_intermediate_features', metavar='str', required=True, help='coma-delimited list of intermediates to be created')
    parser.add_argument('--tile_boundary', metavar='str', required=True, help='geographic boundary of tile')
    parser.add_argument('--tile_resolution', metavar='str', required=True, help='resolution of tile in m.')
    parser.add_argument('--seasonal_utc_offset', metavar='str', required=True, help='seasonal hour offset from utc')
    parser.add_argument('--grid_crs', metavar='str', required=True, help='CRS used for tile generation')

    args = parser.parse_args()

    return_stdout =process_tile(args.city_json_str, args.task_method, args.source_base_path, args.target_base_path,
                                args.city_folder_name, args.tile_folder_name,
                                args.cif_primary_features, args.ctcm_intermediate_features,
                                args.tile_boundary, args.tile_resolution, args.seasonal_utc_offset, args.grid_crs)

    print(return_stdout)
