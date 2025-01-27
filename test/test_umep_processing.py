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
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(19, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_tiled_custom_city():
    source_city_folder_name = 'NLD_Amsterdam_custom_tiled'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(19, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_mixed_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(13, vrt_count, msg=f"Expected VRT count of 13 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def test_untiled_full_cif():
    source_city_folder_name = 'ZAF_Capetown_full_cif'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(13, vrt_count, msg=f"Expected VRT count of 13 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(13, vrt_count, msg=f"Expected VRT count of 13 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_custom_city_with_full_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_full_intermediates'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(19, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_custom_city_with_mixed_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_mixed_intermediates'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        equal(19, vrt_count, msg=f"Expected VRT count of 19 files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def file_count_in_vrt_directory(non_tiled_city_data):
    vrt_dir = os.path.join(non_tiled_city_data.target_qgis_viewer_path, 'vrt_files')
    lst = os.listdir(vrt_dir)
    number_files = len(lst)
    return number_files
