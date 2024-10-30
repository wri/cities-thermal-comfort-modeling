import os

import shapely

from src.tools import get_application_path
from test.tools import is_valid_output_file

from src.src_tools import remove_file
from workers.city_data import CityData
from workers.source_cif_data_downloader import get_cif_data

min_lon = 4.901190775092289
min_lat = 52.37197831356116
max_lon = 4.908300489273159
max_lat = 52.37520954271636
aoi_boundary = str(shapely.box(min_lon, min_lat, max_lon, max_lat))

app_path = get_application_path()
output_base_path = str(os.path.join(app_path, 'sample_cities'))
folder_name_city_data = 'NLD_Amsterdam'
folder_name_tile_data = 'tile_001'
city_data = CityData(folder_name_city_data, folder_name_tile_data, output_base_path, None)
tile_data_path = city_data.source_tile_data_path
test_task_index = -1

def test_get_cif_non_terrain_data():
    # feature_list = ['era5', 'lulc', 'tree_canopy']
    feature_list = ['lulc', 'tree_canopy']
    features = ','.join(feature_list)

    remove_output_files(feature_list)
    get_cif_data(test_task_index, output_base_path, folder_name_city_data, folder_name_tile_data, aoi_boundary, features)

    # if 'era5' in feature_list:
    #     expected_file = os.path.join(tile_data_path, CityData)
    #     assert is_valid_output_file(expected_file)

    if 'lulc' in feature_list:
        expected_file = os.path.join(tile_data_path, city_data.lulc_tif_filename)
        assert is_valid_output_file(expected_file)

    if 'tree_canopy' in feature_list:
        expected_file = os.path.join(tile_data_path, city_data.tree_canopy_tif_filename)
        assert is_valid_output_file(expected_file)

    # remove_output_files(feature_list)


def test_get_cif_terrain_data():
    feature_list = ['dem', 'dsm']
    features = ','.join(feature_list)

    remove_output_files(feature_list)
    get_cif_data(test_task_index, output_base_path, folder_name_city_data, folder_name_tile_data, aoi_boundary, features)

    if 'dem' in feature_list:
        expected_file =  os.path.join(tile_data_path, city_data.dem_tif_filename)
        assert is_valid_output_file(expected_file)

    if 'dsm' in feature_list:
        expected_file =  os.path.join(tile_data_path, city_data.dsm_tif_filename)
        assert is_valid_output_file(expected_file)

    # remove_output_files(feature_list)


def remove_output_files(feature_list):
    # if 'era5' in feature_list:
    #     remove_file(os.path.join(tile_data_path, city_data))
    if 'lulc' in feature_list:
        remove_file(os.path.join(tile_data_path, city_data.lulc_tif_filename))
    if 'tree_canopy' in feature_list:
        remove_file(os.path.join(tile_data_path, city_data.tree_canopy_tif_filename))
    if 'dem' in feature_list:
        remove_file(os.path.join(tile_data_path, city_data.dem_tif_filename))
    if 'dsm' in feature_list:
        remove_file(os.path.join(tile_data_path, city_data.dsm_tif_filename))


