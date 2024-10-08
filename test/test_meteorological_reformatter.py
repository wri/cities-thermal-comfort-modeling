import pytest
import os
from src.met_file_reformatter import reformat

SOURCE_PATH = os.path.join(os.path.dirname(os.getcwd()), 'sample_cities')

def test_file():
    city_name = 'ZAF_Capetown'
    utc_offset = 2
    source_folder = os.path.join(SOURCE_PATH, city_name, 'met_files', 'source_files')
    target_folder = os.path.join(SOURCE_PATH, city_name, 'met_files')
    reformat(source_folder, target_folder, utc_offset)

