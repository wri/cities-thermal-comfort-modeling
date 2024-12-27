import os

from main import start_processing
from src.constants import DATA_DIR, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES
from src.workers.worker_tools import remove_folder, create_folder
from test.testing_tools import run_main, verify_expected_output_folders, SAMPLE_CITIES_SOURCE_DIR

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name)
        vrt_count = file_count_in_vrt_directory(target_city_dir)

        assert return_code == 0
        assert has_valid_results
        assert vrt_count == 19
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)


def test_untiled_full_cif():
    source_city_folder_name = 'ZAF_Capetown_full_cif'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name)
        vrt_count = file_count_in_vrt_directory(target_city_dir)

        assert return_code == 0
        assert has_valid_results
        assert vrt_count == 13
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)
            remove_folder(primary_data)


def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name)
        vrt_count = file_count_in_vrt_directory(target_city_dir)

        assert return_code == 0
        assert has_valid_results
        assert vrt_count == 13
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
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name)
        vrt_count = file_count_in_vrt_directory(target_city_dir)

        assert return_code == 0
        assert has_valid_results
        assert vrt_count == 13
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)


def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'
    target_city_dir = os.path.join(SCRATCH_TARGET_DIR, source_city_folder_name)
    remove_folder(target_city_dir)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        assert return_code == 0
        # TODO remove this false assertion once ERA5 is working again
        assert False
    finally:
        if CLEANUP_RESULTS:
            remove_folder(target_city_dir)
            remove_folder(primary_data)


def file_count_in_vrt_directory(target_city_dir):
    vrt_dir = os.path.join(target_city_dir, '.qgis_viewer', 'vrt_files')
    lst = os.listdir(vrt_dir)
    number_files = len(lst)
    return number_files
