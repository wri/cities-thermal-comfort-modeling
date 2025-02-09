from pytest_check import equal # https://github.com/okken/pytest-check

from src.workers.worker_tools import remove_folder
from test.testing_tools import SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, file_count_in_vrt_directory
from src.worker_manager.ancillary_files import write_qgis_files
from src.workers.city_data import CityData


def test_vrt_writer():
    city_folder_name = 'ZAF_Capetown_small_tile'
    non_tiled_city_data = CityData(city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    # Hack the paths to point to the source data rather than target data
    hacked_city_data = _create_hacked_city_data(non_tiled_city_data)

    remove_folder(hacked_city_data.target_city_parent_path)

    try:
        crs = 'EPSG:32734'
        write_qgis_files(hacked_city_data, crs)

        vrt_count = file_count_in_vrt_directory(hacked_city_data)

        expected_count = 4
        equal(vrt_count, expected_count, msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        remove_folder(hacked_city_data.target_city_parent_path)


def test_vrt_writer_amsterdam():
    city_folder_name = 'NLD_Amsterdam_custom_tiled'
    non_tiled_city_data = CityData(city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    # Hack the paths to point to the source data rather than target data
    hacked_city_data = _create_hacked_city_data(non_tiled_city_data)

    remove_folder(hacked_city_data.target_city_parent_path)

    try:
        crs = 'EPSG:32631'
        write_qgis_files(hacked_city_data, crs)

        vrt_count = file_count_in_vrt_directory(hacked_city_data)

        expected_count = 4
        equal(vrt_count, expected_count, msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        remove_folder(hacked_city_data.target_city_parent_path)


def _create_hacked_city_data(city_data):
    hacked_city_data = city_data
    hacked_city_data.target_city_primary_data_path = hacked_city_data.source_city_primary_data_path
    hacked_city_data.target_city_path = hacked_city_data.source_city_path
    hacked_city_data.target_intermediate_data_path = hacked_city_data.source_intermediate_data_path
    return hacked_city_data
