import os
import shapely

from src.workers.city_data import CityData
from src.workers.source_cif_data_downloader import get_cif_data
from src.workers.worker_tools import remove_folder, remove_file
from tests.testing_tools import SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, is_valid_output_file

crs = 'EPSG:4326'
min_lon= -122.701
min_lat= 45.520
max_lon= -122.699
max_lat= 45.521
tile_boundary = str(shapely.box(min_lon, min_lat, max_lon, max_lat))

folder_name_city_data = 'USA_Portland_cif_umep'
folder_name_tile_data = 'tile_00099'
city_data = CityData(folder_name_city_data, folder_name_tile_data, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
tile_data_path = city_data.target_primary_tile_data_path

DEBUG = False

def test_get_cif_non_terrain_data():
    cif_feature_list = ['lulc', 'tree_canopy']
    cif_features = ','.join(cif_feature_list)

    remove_output_files(cif_feature_list)

    get_cif_data(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, folder_name_city_data, folder_name_tile_data,
                 cif_features, tile_boundary,  crs,None)

    if 'lulc' in cif_feature_list:
        expected_file = os.path.join(tile_data_path, city_data.lulc_tif_filename)
        print(expected_file)
        assert is_valid_output_file(expected_file)

    if 'tree_canopy' in cif_feature_list:
        expected_file = os.path.join(tile_data_path, city_data.tree_canopy_tif_filename)
        print(expected_file)
        assert is_valid_output_file(expected_file)

    if not DEBUG:
        remove_output_files(cif_feature_list)
        remove_folder(tile_data_path)
        remove_folder(city_data.target_city_parent_path)


def test_get_cif_terrain_data():
    cif_feature_list = ['dem', 'dsm']
    cif_features = ','.join(cif_feature_list)

    remove_output_files(cif_feature_list)
    get_cif_data(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, folder_name_city_data, folder_name_tile_data,
                 cif_features, tile_boundary,  crs,None)


    if 'dem' in cif_feature_list:
        expected_file =  os.path.join(tile_data_path, city_data.dem_tif_filename)
        print(expected_file)
        assert is_valid_output_file(expected_file)

    if 'dsm' in cif_feature_list:
        expected_file =  os.path.join(tile_data_path, city_data.dsm_tif_filename)
        print(expected_file)
        assert is_valid_output_file(expected_file)

    if not DEBUG:
        remove_output_files(cif_feature_list)
        remove_folder(tile_data_path)
        remove_folder(city_data.target_city_parent_path)


def remove_output_files(feature_list):
    if 'lulc' in feature_list:
        remove_file(city_data.target_lulc_path)
    if 'tree_canopy' in feature_list:
        remove_file(city_data.target_tree_canopy_path)
    if 'dem' in feature_list:
        remove_file(city_data.target_dem_path)
    if 'dsm' in feature_list:
        remove_file(city_data.target_dsm_path)

