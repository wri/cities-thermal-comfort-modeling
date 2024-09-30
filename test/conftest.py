import pytest
import os

from src.CityData import instantiate_city_data

PACKAGE_FOLDER = os.path.dirname(os.getcwd())
SOURCE_PATH = os.path.join(PACKAGE_FOLDER, 'sample_cities')
TARGET_PATH = os.path.join(PACKAGE_FOLDER, 'test', 'resources')


@ pytest.fixture
def city_data():
    city_folder_name = 'ZAF_Capetown'
    city_data = instantiate_city_data(city_folder_name, SOURCE_PATH, TARGET_PATH)
    return city_data
