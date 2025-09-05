import os
import pytest
from src.constants import DATA_DIR
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from tests.conftest import RUN_COMPLEX_MODEL_SUITE
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, does_file_signature_match
CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_teresina_city_upenn():
    source_city_folder_name = 'BRA_Teresina_city_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

    #     mrt_file_name = 'Tmrt_2019_206_1200D.tif'
    #     target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
    #                                    mrt_file_name)
    #     expected_signature = {'band_checksums': ['1ff68119baee0ffb8d01ddf398e3af64'], 'count': 1, 'crs': 'EPSG:32631',
    #                           'dtype': ('float32',), 'full_checksum': '1ff68119baee0ffb8d01ddf398e3af64', 'height': 171,
    #      'transform': (1.0, 0.0, 629418.2692492839, 0.0, -1.0, 5804285.748513707, 0.0, 0.0, 1.0), 'width': 171}
    #     mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
    #     assert mrt_signatures_are_equal
    #
    #     vrt_count = file_count_in_vrt_directory(non_tiled_city_data)
    #
    #     assert return_code == 0
    #     assert vrt_count == 16
    finally:
        pass
    #     if CLEANUP_RESULTS:
    #         remove_folder(non_tiled_city_data.target_city_parent_path)

