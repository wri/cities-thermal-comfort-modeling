import os
# import dask
# import subprocess
import pandas as pd
import shapely
#
# from src.src_tools import get_application_path
from workers.city_data import parse_processing_areas_config, CityData, parse_filenames_config
#
# CIF_DATA_MODULE_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'source_cif_data_downloader.py'))
# PLUGIN_MODULE_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'umep_plugin_processor.py'))
#
def _build_source_dataframes(source_base_path, city_folder_name):
    config_processing_file_path = str(os.path.join(source_base_path, city_folder_name, CityData.filename_umep_city_processing_config))
    processing_config_df = pd.read_csv(config_processing_file_path)

    return processing_config_df


def _get_aoi_fishnet(source_base_path, city_folder_name):
    source_city_path = str(os.path.join(source_base_path, city_folder_name))

    min_lon, min_lat, max_lon, max_lat, tile_side_size_meters = \
        parse_processing_areas_config(source_city_path, CityData.filename_method_parameters_config)

    # aoi_boundary = str(shapely.box(min_lon, min_lat, max_lon, max_lat))

    from city_metrix.layers.layer import create_fishnet_grid
    if tile_side_size_meters is None or tile_side_size_meters == 'None':
        lon_diff = max_lon - min_lon
        lat_diff = max_lat - min_lat
        tile_side_size_meters = lon_diff if lon_diff > lat_diff else lat_diff

    fishnet = create_fishnet_grid(min_lon, min_lat, max_lon, max_lat, tile_side_size_meters)

    return fishnet


def get_cif_features(source_city_path):
    dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_custom_features, cif_feature_list =\
        parse_filenames_config(source_city_path, CityData.filename_method_parameters_config)

    custom_file_names = []
    if 'dem' not in cif_feature_list:
        custom_file_names.append(dem_tif_filename)
    if 'dsm' not in cif_feature_list:
        custom_file_names.append(dsm_tif_filename)
    if 'tree_canopy' not in cif_feature_list:
        custom_file_names.append(tree_canopy_tif_filename)
    if 'lulc' not in cif_feature_list:
        custom_file_names.append(lulc_tif_filename)

    cif_features = ','.join(cif_feature_list)
    return custom_file_names, has_custom_features, cif_features

