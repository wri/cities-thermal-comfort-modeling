import os

from src.constants import DATA_DIR
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, \
    does_file_signature_match, get_geotiff_signature

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)


def test_ZAF_Capetown_cif_upenn():
    source_city_folder_name = r'ZAF_Capetown_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260835.95995819467, 0.0, -1.0, 6243703.498204948, 0.0, 0.0, 1.0),
                              'width': 102, 'height': 115, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['324aaaa2166f661cf700ff980bed0ef0'],
                              'full_checksum': '324aaaa2166f661cf700ff980bed0ef0'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


def test_ZAF_Capetown_cif_umep():
    source_city_folder_name = r'ZAF_Capetown_cif_umep'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        # expected_signature = get_geotiff_signature(target_met_file)

        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260835.95995819467, 0.0, -1.0, 6243703.498204948, 0.0, 0.0, 1.0),
                              'width': 102, 'height': 115, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['f209f87e8a7b8b1a4cd31cd5adea5a68'],
                              'full_checksum': 'f209f87e8a7b8b1a4cd31cd5adea5a68'}

        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

