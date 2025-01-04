import os
import subprocess
import pandas as pd

from src.workers.city_data import CityData
from src.constants import DATA_DIR, ROOT_DIR, FILENAME_PROCESSING_CSV_CONFIG

SAMPLE_CITIES_SOURCE_DIR = str(os.path.join(DATA_DIR, 'sample_cities'))
SCRATCH_TARGET_DIR = str(os.path.join(DATA_DIR, 'scratch_target'))


def is_valid_output_directory(path):
    is_valid = True if os.path.isdir(path) and len(os.listdir(path)) > 0 else False
    return is_valid

def is_valid_output_file(file_path):
    is_valid = True if os.path.isfile(file_path) and os.path.getsize(file_path) > 0 else False
    return is_valid


def run_main(target_base_path:str, source_city_folder_name:str, processing_option:str):
    command = ['python', os.path.join(ROOT_DIR, 'main.py'),
               '--source_base_path', SAMPLE_CITIES_SOURCE_DIR,
               '--target_base_path', target_base_path,
               '--city_folder_name', source_city_folder_name,
               '--processing_option', processing_option]
    proc_results = subprocess.run(command, capture_output=True, text=True)
    return_code = proc_results.returncode
    return return_code



