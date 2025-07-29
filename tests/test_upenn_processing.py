import os
import pytest
from pytest_check import equal # https://github.com/okken/pytest-check

from src.constants import DATA_DIR
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, compare_raster_data

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

RUN_CORE_TESTS_ONLY = False

def test_ZAF_Capetown_full_cif_upenn():
    source_city_folder_name = r'ZAF_Capetown_full_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_001', mrt_file_name)

        mrt_files_are_equal, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        assert return_code == 0
        assert vrt_count == 12

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_USA_Philadelphia_downtown_cif_upenn():
    source_city_folder_name = r'USA_Philadelphia_downtown_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        mrt_file_name = 'Tmrt_2022_182_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'nrel_philadelphia', 'tile_001', mrt_file_name)

        mrt_files_are_equal, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

def test_USA_WashingtonDC_cif_upenn():
    source_city_folder_name = r'USA_WashingtonDC_cif_upenn'
    non_tiled_city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    remove_folder(non_tiled_city_data.target_city_parent_path)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        mrt_file_name = 'Tmrt_2022_182_1200D.tif'
        expected_mrt_file = os.path.join(non_tiled_city_data.source_city_path, 'expected_results', mrt_file_name)
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'nrel_washingtondc', 'tile_001', mrt_file_name)

        mrt_files_are_equal, mrt_diff_count, rounded_mrt_diff_count = compare_raster_data(expected_mrt_file, target_met_file)

        if not mrt_files_are_equal:
            print(f"bad run: mrt_diff_count:{mrt_diff_count}, mrt_rounded_diff_count:{rounded_mrt_diff_count}")

        assert mrt_files_are_equal

        assert return_code == 0
        assert vrt_count == 13

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)
