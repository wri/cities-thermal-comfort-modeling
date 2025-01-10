import os
import pytest
from pytest_check import equal # https://github.com/okken/pytest-check

from src.constants import DATA_DIR, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from test.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

# test fpr era5

RUN_CORE_TESTS_ONLY = False

def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(20, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_tiled_custom_city():
    source_city_folder_name = 'NLD_Amsterdam_custom_tiled'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(14, vrt_count, msg=f"Expected VRT count of 13 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_mixed_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(14, vrt_count, msg=f"Expected VRT count of 13 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_untiled_full_cif():
    source_city_folder_name = 'ZAF_Capetown_full_cif'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(14, vrt_count, msg=f"Expected VRT count of 14 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(14, vrt_count, msg=f"Expected VRT count of 13 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_custom_city_with_full_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_full_intermediates'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(20, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_custom_city_with_mixed_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_mixed_intermediates'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(20, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        # # TODO remove this false assertion once ERA5 is working again after fixing CIF-321
        # assert False
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)



def file_count_in_vrt_directory(city_data):
    vrt_dir = os.path.join(city_data.target_qgis_viewer_path, 'vrt_files')
    lst = os.listdir(vrt_dir)
    number_files = len(lst)
    return number_files
