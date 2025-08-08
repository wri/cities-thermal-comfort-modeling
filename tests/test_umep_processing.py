import os
import pytest
from pytest_check import equal # https://github.com/okken/pytest-check

from src.constants import DATA_DIR, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, compare_raster_data

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

# TODO Add test fpr era5

RUN_CORE_TESTS_ONLY = False


def test_tropical_full_cif_city():
    source_city_folder_name = 'BRA_Rio_de_Janeiro_full_cif'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 17
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
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
        expected_count = 17
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_tiled_buffered_cif_city_umep():
    source_city_folder_name = 'NLD_Amsterdam_buftile_cif_umep'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 17
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
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
        expected_count = 16
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_portland_swath_cif_umep():
    source_city_folder_name = 'USA_Portland_swath_cif_umep'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

@pytest.mark.skipif(RUN_CORE_TESTS_ONLY == True, reason='Skipping since RUN_CORE_TESTS_ONLY set to True')
def test_USA_WashingtonDC_cif_umep():
    source_city_folder_name = 'USA_WashingtonDC_cif_umep'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
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

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 8
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def test_ZAF_Capetown_cif_local_era5_umep():
    source_city_folder_name = 'ZAF_Capetown_cif_local_era5_umep'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 17
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
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
        expected_count = 16
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 22
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
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
        expected_count = 22
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
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
        expected_count = 22
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def test_USA_Philadelphia_downtown_cif_umep():
    source_city_folder_name = r'USA_Philadelphia_downtown_cif_umep'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        # mrt_file_name = 'Tmrt_2023_250_1200D.tif'
        # expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        # target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)
        #
        # mrt_files_are_equal, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)
        #
        # if not mrt_files_are_equal:
        #     print(f"bad run: mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")
        #
        # assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_ZAF_Capetown_cif_umep():
    source_city_folder_name = r'ZAF_Capetown_cif_umep'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        # mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        # expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        # target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)
        #
        # mrt_files_are_equal, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)
        #
        # if not mrt_files_are_equal:
        #     print(f"bad run: mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")
        #
        # assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)
