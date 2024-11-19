import os

from worker_manager.ancillary_files import write_vrt_files
from workers.city_data import CityData


def test_vrt_writer():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'sample_cities')
    city_folder_name = 'ZAF_Capetown_small_tile'
    city_data = CityData(city_folder_name, None, source_base_path, target_base_path)

    crs_str = 'EPSG:32734'
    write_vrt_files(city_data, crs_str)

def test_vrt_writer_amsterdam():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'sample_cities')
    city_folder_name = 'NLD_Amsterdam'
    city_data = CityData(city_folder_name, None, source_base_path, target_base_path)

    crs_str = 'EPSG:32631'
    write_vrt_files(city_data, crs_str)

