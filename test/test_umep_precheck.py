import os

from main import start_processing
from src.constants import DATA_DIR
from src.workers.worker_tools import create_folder
from test.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR
import pytest

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)

def test_cif_city_check():
    source_city_folder_name = 'NLD_Amsterdam'

    return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'pre_check_all')
    # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, '', source_city_folder_name, 'pre_check_all')

    assert return_code == 0


def test_custom_city_check_all():
    source_city_folder_name = 'ZAF_Capetown_small_tile'

    return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'pre_check_all')
    # return_code = start_processing(SAMPLE_CITIES_SOURCE_DIR, source_city_folder_name, 'pre_check_all')

    assert return_code == 0


def test_main_check_invalid_city_1_configs():
    source_city_folder_name = 'XXX_Invalid_city_1'

    with pytest.raises(Exception):
        start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'pre_check_all')


def test_main_check_invalid_city_2_configs():
    source_city_folder_name = 'XXX_Invalid_city_2'

    with pytest.raises(Exception):
        start_processing(SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR, source_city_folder_name, 'pre_check_all')

