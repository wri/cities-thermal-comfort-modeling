import os
import pytest
from city_metrix.metrix_dao import get_bucket_name_from_s3_uri, delete_s3_folder_if_exists

from src.constants import DATA_DIR, S3_PUBLICATION_BUCKET
from src.worker_manager.tools import list_s3_subfolders
from src.workers.city_data import CityData
from src.workers.worker_dao import get_scenario_folder_key
from src.workers.worker_tools import remove_folder, create_folder
from tests.conftest import RUN_COMPLEX_MODEL_SUITE
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, does_file_signature_match

CLEANUP_RESULTS=True

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)
bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)

@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_capetown_two_part():
    global scenario_uri, non_tiled_city_data_1, non_tiled_city_data_2
    try:
        source_city_folder_name = 'ZAF_Capetown_urbext_part1_upenn'
        non_tiled_city_data_1 = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR,
                                         SCRATCH_TARGET_DIR)

        scenario_folder_key = get_scenario_folder_key(non_tiled_city_data_1)
        scenario_parent_key = os.path.dirname(scenario_folder_key)
        scenario_uri = f"s3://{bucket_name}/{scenario_parent_key}/"

        # remove the target s3 folder prior to execution
        delete_s3_folder_if_exists(scenario_uri)

        # run and part 1
        return_code_1 = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        existing_folders = list_s3_subfolders(bucket_name, scenario_folder_key)
        folder_count = len(existing_folders)
        assert return_code_1 == 0
        assert folder_count == 3 # metadata plus 2 tiles

        # run and test part 2
        source_city_folder_name2 = 'ZAF_Capetown_urbext_part2_upenn'
        non_tiled_city_data_2 = CityData(None, source_city_folder_name2, None, SAMPLE_CITIES_SOURCE_DIR,
                                       SCRATCH_TARGET_DIR)
        return_code_2 = run_main(SCRATCH_TARGET_DIR, source_city_folder_name2, 'run_pipeline')

        existing_folders = list_s3_subfolders(bucket_name, scenario_folder_key)
        folder_count = len(existing_folders)
        assert return_code_2 == 0
        assert folder_count == 5 # metadata plus 4 tiles

    finally:
        if CLEANUP_RESULTS:
            delete_s3_folder_if_exists(scenario_uri)
            remove_folder(non_tiled_city_data_1.target_city_parent_path)
            remove_folder(non_tiled_city_data_2.target_city_parent_path)