import os
from src.tools import get_application_path
from test.tools import is_valid_output_file
from workers.city_data import CityData
from workers.source_cif_non_terrain_data_downloader import get_cif_data as get_other_data
from workers.source_cif_terrain_data_downloader import get_cif_data as get_terrain_data
from workers.tools import clean_folder, remove_file

aoi_boundary = [(4.901190775092289, 52.37520954271636), (4.901190775092289, 52.37197831356116),
                (4.908300489273159, 52.37197831356116), (4.908300489273159, 52.37520954271636),
                (4.901190775092289, 52.37520954271636)]

app_path = get_application_path()
out_base_path = str(os.path.join(app_path, 'sample_cities'))
folder_name_city_data = 'NLD_Amsterdam'
folder_name_tile_data = 'tile_001'
city_data = CityData(folder_name_city_data, folder_name_tile_data, out_base_path, None)
tile_data_path = city_data.source_tile_data_path

def test_get_cif_non_terrain_data():
    retrieve_era5 = False
    retrieve_lulc = True
    retrieve_tree_canopy = True

    remove_non_terrain_files(retrieve_era5, retrieve_lulc, retrieve_tree_canopy)
    get_other_data(out_base_path, folder_name_city_data, folder_name_tile_data, aoi_boundary,
                   retrieve_era5, retrieve_lulc, retrieve_tree_canopy)

    if retrieve_era5:
        expected_file = os.path.join(tile_data_path, CityData.filename_cif_era5)
        assert is_valid_output_file(expected_file)

    if retrieve_lulc:
        expected_file = os.path.join(tile_data_path, CityData.filename_cif_lulc)
        assert is_valid_output_file(expected_file)

    if retrieve_tree_canopy:
        expected_file = os.path.join(tile_data_path, CityData.filename_cif_tree_canopy)
        assert is_valid_output_file(expected_file)

    remove_non_terrain_files(retrieve_era5, retrieve_lulc, retrieve_tree_canopy)


def test_get_cif_terrain_data():
    retrieve_dem = True
    retrieve_dsm = True

    remove_terrain_files(retrieve_dem, retrieve_dsm)
    get_terrain_data(out_base_path, folder_name_city_data, folder_name_tile_data, aoi_boundary,
                     retrieve_dem, retrieve_dsm)

    if retrieve_dem:
        expected_file =  os.path.join(tile_data_path, CityData.filename_cif_dem)
        assert is_valid_output_file(expected_file)

    if retrieve_dsm:
        expected_file =  os.path.join(tile_data_path, CityData.filename_cif_dsm_ground_build)
        assert is_valid_output_file(expected_file)

    remove_terrain_files(retrieve_dem, retrieve_dsm)


def remove_non_terrain_files(retrieve_era5, retrieve_lulc, retrieve_tree_canopy):
    if retrieve_era5:
        remove_file(os.path.join(tile_data_path, city_data.filename_cif_era5))
    if retrieve_lulc:
        remove_file(os.path.join(tile_data_path, city_data.filename_cif_lulc))
    if retrieve_tree_canopy:
        remove_file(os.path.join(tile_data_path, city_data.filename_cif_tree_canopy))


def remove_terrain_files(retrieve_dem, retrieve_dsm):
    if retrieve_dem:
        remove_file(os.path.join(tile_data_path, city_data.filename_cif_dem))
    if retrieve_dsm:
        remove_file(os.path.join(tile_data_path, city_data.filename_cif_dsm_ground_build))
