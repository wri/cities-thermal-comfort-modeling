from test.testing_tools import SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR
from src.worker_manager.ancillary_files import write_qgis_files
from src.workers.city_data import CityData


def test_vrt_writer():
    city_folder_name = 'ZAF_Capetown_small_tile'
    non_tiled_city_data = CityData(city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    crs_str = 'EPSG:32734'
    write_qgis_files(non_tiled_city_data, crs_str)

def test_vrt_writer_amsterdam():
    city_folder_name = 'NLD_Amsterdam'
    non_tiled_city_data = CityData(city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    crs_str = 'EPSG:32631'
    write_qgis_files(non_tiled_city_data, crs_str)


