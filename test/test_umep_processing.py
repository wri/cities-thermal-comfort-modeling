import os

from src.worker_manager.tools import clean_folder
from src.workers.worker_tools import remove_folder
from test.testing_tools import run_main, verify_expected_output_folders, SAMPLE_CITIES_PATH
from src.workers.city_data import CityData

CLEANUP_RESULTS=True


def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    city_folder = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name)
    results_data_folder = os.path.join(city_folder, CityData.folder_name_results)
    clean_folder(results_data_folder)

    try:
        return_code = run_main(source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SAMPLE_CITIES_PATH, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_target_folders(results_data_folder, city_folder)


def test_untiled_full_cif():
    source_city_folder_name = 'ZAF_Capetown_full_cif'

    city_folder = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name)
    results_data_folder = os.path.join(city_folder, CityData.folder_name_results)
    clean_folder(results_data_folder)

    try:
        return_code = run_main(source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SAMPLE_CITIES_PATH, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_target_folders(results_data_folder, city_folder)
            primary_source_folder = os.path.join(city_folder, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
            remove_folder(primary_source_folder)


def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'

    city_folder = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name)
    results_data_folder = os.path.join(city_folder, CityData.folder_name_results)
    clean_folder(results_data_folder)

    try:
        return_code = run_main(source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SAMPLE_CITIES_PATH, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_target_folders(results_data_folder, city_folder)
            primary_source_folder = os.path.join(city_folder, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
            remove_folder(primary_source_folder)


def test_mixed_cif_city():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'

    city_folder = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name)
    results_data_folder = os.path.join(city_folder, CityData.folder_name_results)
    clean_folder(results_data_folder)

    try:
        return_code = run_main(source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, source_city_folder_name, 'run_pipeline')

        has_valid_results = verify_expected_output_folders(SAMPLE_CITIES_PATH, SAMPLE_CITIES_PATH, source_city_folder_name)
        assert return_code == 0
        assert has_valid_results
    finally:
        if CLEANUP_RESULTS:
            remove_target_folders(results_data_folder, city_folder)


def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'

    primary_data = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
    clean_folder(primary_data)

    city_folder = os.path.join(SAMPLE_CITIES_PATH, source_city_folder_name)
    results_data_folder = os.path.join(city_folder, CityData.folder_name_results)
    clean_folder(results_data_folder)

    try:
        return_code = run_main(source_city_folder_name, 'run_pipeline')
        # return_code = start_processing(SAMPLE_CITIES_PATH, source_city_folder_name, 'run_pipeline')

        assert return_code == 0
    finally:
        if CLEANUP_RESULTS:
            remove_target_folders(results_data_folder, city_folder)
            primary_source_folder = os.path.join(city_folder, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
            remove_folder(primary_source_folder)


def remove_target_folders(results_data_folder, city_folder):
    remove_folder(os.path.join(city_folder, '.logs'))
    remove_folder(os.path.join(city_folder, '.qgis_viewer'))
    remove_folder(results_data_folder)
