import os
from pytest_check import equal
from src.constants import DATA_DIR
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, compare_raster_data

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

RUN_CORE_TESTS_ONLY = False

def test_tiled_buffered_cif_city_upenn():
    source_city_folder_name = 'NLD_Amsterdam_buftile_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 13
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_USA_Philadelphia_downtown_cif_upenn():
    source_city_folder_name = r'USA_Philadelphia_downtown_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_250_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)

        mrt_files_are_equal, error_number, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: error_number: {error_number}, mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_USA_Portland_cif_upenn():
    source_city_folder_name = r'USA_Portland_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)

        mrt_files_are_equal, error_number, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: error_number: {error_number}, mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_USA_Portland_swath_cif_upenn():
    source_city_folder_name = r'USA_Portland_swath_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)

        mrt_files_are_equal, error_number, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: error_number: {error_number}, mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_USA_WashingtonDC_cif_upenn():
    source_city_folder_name = r'USA_WashingtonDC_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_247_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)

        mrt_files_are_equal, error_number, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: error_number: {error_number}, mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def test_ZAF_Capetown_cif_upenn():
    source_city_folder_name = r'ZAF_Capetown_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)

        mrt_files_are_equal, error_number, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: error_number: {error_number}, mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_ZAF_Capetown_cif_local_era5_upenn():
    source_city_folder_name = 'ZAF_Capetown_cif_local_era5_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 13
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

