import os
from city_metrix.metrix_dao import get_bucket_name_from_s3_uri, delete_s3_folder_if_exists

from src.constants import DATA_DIR, S3_PUBLICATION_BUCKET
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, does_file_signature_match

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)

# @pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_evaluate_capetown_ctcm_staging():
    source_city_folder_name = 'ZAF_Capetown_urbext_subarea_upenn'
    return_code = run_main('', source_city_folder_name, 'prep_check_ctcm_staging')

    assert return_code == 0


# def test_evaluate_ushuaia_ctcm_staging():
#     source_city_folder_name = 'ARG_Ushuaia_urbext_upenn'
#     return_code = run_main('', source_city_folder_name, 'prep_check_ctcm_staging')
#
#     assert return_code != 0


def test_stage_ushuaia():
    source_city_folder_name = 'ARG_Ushuaia_urbext_upenn'
    return_code = run_main('', source_city_folder_name, 'stage_ctcm_data')

    assert return_code == 0


def test_validate_result_tiles_teresina():
    source_city_folder_name = 'BRA_Teresina_city_subarea_upenn'
    return_code = run_main('', source_city_folder_name, 'validate_ctcm_result_data')

    assert return_code == 0
