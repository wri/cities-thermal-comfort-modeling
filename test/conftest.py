import pytest
import os
import tempfile

from src.CityData import instantiate_city_data
from src.tools import remove_folder

SOURCE_PATH = os.path.join(os.path.dirname(os.getcwd()), 'sample_cities')
CITY_FOLDER_NAME = 'ZAF_Capetown_small_tile'
TEMP_TARGET_FOLDER = tempfile.mkdtemp()

@ pytest.fixture
def setup_city_data():
    city_data = instantiate_city_data(CITY_FOLDER_NAME, SOURCE_PATH, TEMP_TARGET_FOLDER)
    yield city_data

@pytest.fixture(autouse=True)
def cleanup_files(monkeypatch):
    remove_folder(TEMP_TARGET_FOLDER)