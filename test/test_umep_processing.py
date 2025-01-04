import os

from src.constants import DATA_DIR, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from test.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

# test fpr era5



def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        assert return_code == 0
        assert vrt_count == 19
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

        assert return_code == 0
        assert vrt_count == 13
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        assert return_code == 0
        assert vrt_count == 13
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_tiled_custom_city():
    source_city_folder_name = 'NLD_Amsterdam_custom_tiled'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        assert return_code == 0
        assert vrt_count == 13
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_mixed_cif_city():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        assert return_code == 0
        assert vrt_count == 13
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_custom_city_with_full_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_full_intermediates'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        assert return_code == 0
        assert vrt_count == 19
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_custom_city_with_mixed_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_mixed_intermediates'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(city_data)

        assert return_code == 0
        assert vrt_count == 19
    finally:
        if CLEANUP_RESULTS:
            remove_folder(city_data.target_city_parent_path)


def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'
    city_data = CityData(source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    primary_data = os.path.join(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES)
    remove_folder(primary_data)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        assert return_code == 0
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
