import os

from src.constants import DATA_DIR
from src.workers.worker_tools import remove_folder, create_folder
from test.testing_tools import run_main, verify_expected_output_folders, SAMPLE_CITIES_PATH
from src.workers.city_data import CityData

CLEANUP_RESULTS=True

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)


def test_untiled_full_cif():
    source_city_folder_name = 'ZAF_Capetown_full_cif'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    primary_data = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)
            remove_folder(primary_data)


def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    primary_data = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)
            remove_folder(primary_data)


def test_mixed_cif_city():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)


def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    primary_data = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        assert return_code == 0
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)
            remove_folder(primary_data)


