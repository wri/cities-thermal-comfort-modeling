import os
import pandas as pd

from test.test_tools import is_valid_output_directory
from workers.city_data import CityData
from main import main
import pytest


def test_custom_city():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'test', 'test_results')
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    return_code = main(source_base_path, target_base_path, source_city_folder_name, 'no_pre_check')

    has_valid_results = _verify_expected_output_folders(source_base_path, target_base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results


def test_cif_city():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'test', 'test_results')
    source_city_folder_name = 'NLD_Amsterdam'
    return_code = main(source_base_path, target_base_path, source_city_folder_name, 'no_pre_check')

    has_valid_results = _verify_expected_output_folders(source_base_path, target_base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results

def test_mixed_cif_city():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'test', 'test_results')
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'
    return_code = main(source_base_path, target_base_path, source_city_folder_name, 'no_pre_check')

    has_valid_results = _verify_expected_output_folders(source_base_path, target_base_path, source_city_folder_name)
    assert return_code == 0
    assert has_valid_results


def test_custom_city_check_all():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    return_code = main(source_base_path, target_base_path, source_city_folder_name, 'check_all')

    assert return_code == 0


def test_main_check_all_failure():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'missing_city')
    target_base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    with pytest.raises(Exception):
        main(source_base_path, target_base_path, source_city_folder_name, 'check_all')


def test_main_check_invalid_configs():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'sample_cities')
    target_base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'XXX_Invalid_city'

    with pytest.raises(Exception):
        main(source_base_path, target_base_path, source_city_folder_name, 'check_all')


def test_main_check_enabled_only_failure():
    package_folder = os.path.dirname(os.getcwd())
    source_base_path = os.path.join(package_folder, 'missing_city')
    target_base_path = os.path.join(package_folder, 'sample_cities')
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    with pytest.raises(Exception):
        main(source_base_path, target_base_path, source_city_folder_name, 'check_enabled_only')


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
