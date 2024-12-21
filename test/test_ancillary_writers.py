from test.testing_tools import SAMPLE_CITIES_PATH
from src.worker_manager.ancillary_files import write_qgis_files
from src.workers.city_data import CityData


def test_vrt_writer():
    city_folder_name = 'ZAF_Capetown_small_tile'
    city_data = CityData(city_folder_name, None, SAMPLE_CITIES_PATH, SAMPLE_CITIES_PATH)

    crs_str = 'EPSG:32734'
    write_qgis_files(city_data, crs_str)

def test_vrt_writer_amsterdam():
    city_folder_name = 'NLD_Amsterdam'
    city_data = CityData(city_folder_name, None, SAMPLE_CITIES_PATH, SAMPLE_CITIES_PATH)

    crs_str = 'EPSG:32631'
    write_qgis_files(city_data, crs_str)


