import os
import subprocess
import pandas as pd

from src.workers.city_data import CityData
from src.constants import DATA_DIR, ROOT_DIR


def is_valid_output_directory(path):
    is_valid = True if os.path.isdir(path) and len(os.listdir(path)) > 0 else False
    return is_valid

def is_valid_output_file(file_path):
    is_valid = True if os.path.isfile(file_path) and os.path.getsize(file_path) > 0 else False
    return is_valid

SAMPLE_CITIES_PATH = str(os.path.join(DATA_DIR, 'sample_cities'))

def run_main(target_base_path:str, source_city_folder_name:str, processing_option:str):
    command = ['python', os.path.join(ROOT_DIR, 'main.py'),
               '--source_base_path', SAMPLE_CITIES_PATH,
               '--target_base_path', target_base_path,
               '--city_folder_name', source_city_folder_name,
               '--processing_option', processing_option]
    proc_results = subprocess.run(command, capture_output=True, text=True)
    return_code = proc_results.returncode
    return return_code


def verify_expected_output_folders(source_sample_cities_path, target_sample_cities_path, source_city_folder_name):
    enabled_target_folder = []
    config_processing_file_path = str(os.path.join(source_sample_cities_path, source_city_folder_name, CityData.filename_umep_city_processing_config))
    processing_config_df = pd.read_csv(config_processing_file_path)
    for index, config_row in processing_config_df.iterrows():
        enabled = bool(config_row.enabled)
        if enabled:
            # Use representative tile
            city_data = CityData(source_city_folder_name, 'tile_001', source_sample_cities_path, target_sample_cities_path)
            result_folder = city_data.target_preprocessed_tile_data_path
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

