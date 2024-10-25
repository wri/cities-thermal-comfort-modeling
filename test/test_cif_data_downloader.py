import os
from src.tools import get_application_path
from workers.source_cif_met_veg_data_downloader import get_cif_data as get_met_veg_data
from workers.source_cif_terrain_data_downloader import get_cif_data as get_terrain_data


def test_get_cif_terrain_data():
    ## Name of the area of interest
    app_path = get_application_path()
    target_path = str(os.path.join(app_path, 'test', 'test_results'))

    folder_name_city_data = 'NLD_Amsterdam'
    aoi_boundary = [(4.901190775092289, 52.37520954271636), (4.901190775092289, 52.37197831356116), (4.908300489273159, 52.37197831356116), (4.908300489273159, 52.37520954271636), (4.901190775092289, 52.37520954271636)]
    folder_name_tile_data = 'tile_001'

    get_terrain_data(target_path, folder_name_city_data, folder_name_tile_data, aoi_boundary)


def test_get_cif_met_veg_data():
    ## Name of the area of interest
    app_path = get_application_path()
    target_path = str(os.path.join(app_path, 'test', 'test_results'))

    folder_name_city_data = 'NLD_Amsterdam'
    aoi_boundary = [(4.901190775092289, 52.37520954271636), (4.901190775092289, 52.37197831356116), (4.908300489273159, 52.37197831356116), (4.908300489273159, 52.37520954271636), (4.901190775092289, 52.37520954271636)]
    folder_name_tile_data = 'tile_001'

    get_met_veg_data(target_path, folder_name_city_data, folder_name_tile_data, aoi_boundary)
