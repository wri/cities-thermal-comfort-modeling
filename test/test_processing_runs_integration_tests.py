import os
import pandas as pd

from main import start_processing
from src.src_tools import remove_folder, clean_folder
from test.testing_tools import is_valid_output_directory
from workers.city_data import CityData
import pytest


def test_custom_city():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    target_results = os.path.join(base_path, source_city_folder_name, CityData.folder_name_results, CityData.folder_name_results)
    clean_folder(target_results)

    return_code = start_processing(base_path, source_city_folder_name, 'run_pipeline')

    has_valid_results = _verify_expected_output_folders(base_path, base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results


def test_cif_city():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'NLD_Amsterdam'

    target_results = os.path.join(base_path, source_city_folder_name, CityData.folder_name_results, CityData.folder_name_results)
    clean_folder(target_results)

    return_code = start_processing(base_path, source_city_folder_name, 'run_pipeline')

    has_valid_results = _verify_expected_output_folders(base_path, base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results


def test_mixed_cif_city():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'

    target_results = os.path.join(base_path, source_city_folder_name, CityData.folder_name_results, CityData.folder_name_results)
    clean_folder(target_results)

    return_code = start_processing(base_path, source_city_folder_name, 'run_pipeline')

    has_valid_results = _verify_expected_output_folders(base_path, base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results


def test_download_only_cif_city():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'NLD_Amsterdam_download_only'

    primary_data = os.path.join(base_path, source_city_folder_name, CityData.folder_name_source_data, CityData.folder_name_primary_source_data)
    clean_folder(primary_data)
    target_results = os.path.join(base_path, source_city_folder_name, CityData.folder_name_results, CityData.folder_name_results)
    clean_folder(target_results)

    return_code = start_processing(base_path, source_city_folder_name, 'run_pipeline')

    assert return_code == 0


def test_cif_city_check():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'NLD_Amsterdam'

    target_results = os.path.join(base_path, source_city_folder_name, CityData.folder_name_results, CityData.folder_name_results)
    clean_folder(target_results)

    return_code = start_processing(base_path, source_city_folder_name, 'pre_check_all')

    has_valid_results = _verify_expected_output_folders(base_path, base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results


def test_custom_city_check_all():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    return_code = start_processing(base_path, source_city_folder_name, 'pre_check_all')

    assert return_code == 0


def test_main_check_all_failure():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'missing_city')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    with pytest.raises(Exception):
        start_processing(base_path, source_city_folder_name, 'pre_check_all')


def test_main_check_invalid_city_1_configs():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'XXX_Invalid_city_1'

    with pytest.raises(Exception):
        start_processing(base_path, source_city_folder_name, 'pre_check_all')


def test_main_check_invalid_city_2_configs():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'XXX_Invalid_city_2'

    with pytest.raises(Exception):
        start_processing(base_path, source_city_folder_name, 'pre_check_all')


def test_main_check_enabled_only_failure():
    package_folder = os.path.dirname(os.getcwd())
    base_path = os.path.join(package_folder, 'missing_city')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    with pytest.raises(Exception):
        start_processing(base_path, source_city_folder_name, 'check_enabled_only')


def _verify_expected_output_folders(source_base_path, target_base_path, source_city_folder_name):
    enabled_target_folder = []
    config_processing_file_path = str(os.path.join(source_base_path, source_city_folder_name, CityData.filename_umep_city_processing_config))
    processing_config_df = pd.read_csv(config_processing_file_path)
    for index, config_row in processing_config_df.iterrows():
        enabled = bool(config_row.enabled)
        if enabled:
            # Use representative tile
            city_data = CityData(source_city_folder_name, 'tile_001', source_base_path, target_base_path)
            result_folder = city_data.target_tile_data_path
            enabled_target_folder.append(result_folder)

    unique_target_folders = set(enabled_target_folder)
    expected_target_folder_count = len(unique_target_folders)
    actual_target_folder_count = 0
    has_content = False
    for folder in unique_target_folders:
        out_directory = os.path.abspath(folder)
        if is_valid_output_directory(out_directory) is True:
            actual_target_folder_count += 1

    return True if expected_target_folder_count == actual_target_folder_count else False
