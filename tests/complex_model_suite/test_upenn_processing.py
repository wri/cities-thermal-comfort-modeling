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
def test_tiled_buffered_cif_city_upenn():
    source_city_folder_name = 'NLD_Amsterdam_buftile_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2019_206_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'band_checksums': ['1ff68119baee0ffb8d01ddf398e3af64'], 'count': 1, 'crs': 'EPSG:32631',
                              'dtype': ('float32',), 'full_checksum': '1ff68119baee0ffb8d01ddf398e3af64', 'height': 171,
         'transform': (1.0, 0.0, 629418.2692492839, 0.0, -1.0, 5804285.748513707, 0.0, 0.0, 1.0), 'width': 171}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_USA_Philadelphia_downtown_cif_upenn():
    source_city_folder_name = r'USA_Philadelphia_downtown_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_250_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'band_checksums': ['ca6705e191dd847e6beb297c886b494b'], 'count': 1, 'crs': 'EPSG:32618', 'dtype': ('float32',), 'full_checksum': 'ca6705e191dd847e6beb297c886b494b', 'height': 512, 'transform': (1.0, 0.0, 485133.5636835794, 0.0, -1.0, 4422429.002821511, 0.0, 0.0, 1.0), 'width': 461}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_USA_Portland_cif_upenn():
    source_city_folder_name = r'USA_Portland_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'band_checksums': ['bdc11fbaf199fd160e6be589a76f4553'], 'count': 1, 'crs': 'EPSG:32610', 'dtype': ('float32',), 'full_checksum': 'bdc11fbaf199fd160e6be589a76f4553', 'height': 183, 'transform': (1.0, 0.0, 523899.00692041893, 0.0, -1.0, 5041804.175985137, 0.0, 0.0, 1.0), 'width': 257}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_USA_Portland_swath_cif_upenn():
    source_city_folder_name = r'USA_Portland_swath_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'band_checksums': ['d5f5e30bd4b8f39d4037098576c4c867'], 'count': 1, 'crs': 'EPSG:32610', 'dtype': ('float32',), 'full_checksum': 'd5f5e30bd4b8f39d4037098576c4c867', 'height': 374, 'transform': (1.0, 0.0, 523139.01534016046, 0.0, -1.0, 5041128.822227225, 0.0, 0.0, 1.0), 'width': 892}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_USA_WashingtonDC_cif_upenn():
    source_city_folder_name = r'USA_WashingtonDC_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_247_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'band_checksums': ['3000ba66f05344352462ab48eb77b39e'], 'count': 1, 'crs': 'EPSG:32618', 'dtype': ('float32',), 'full_checksum': '3000ba66f05344352462ab48eb77b39e', 'height': 707, 'transform': (1.0, 0.0, 324731.4891801595, 0.0, -1.0, 4309158.4549594745, 0.0, 0.0, 1.0), 'width': 858}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_ZAF_Capetown_cif_local_era5_upenn():
    source_city_folder_name = 'ZAF_Capetown_cif_local_era5_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'band_checksums': ['3e0e5bbedc56aa562e4e54d41a5dd68a'], 'count': 1, 'crs': 'EPSG:32734', 'dtype': ('float32',), 'full_checksum': '3e0e5bbedc56aa562e4e54d41a5dd68a', 'height': 115, 'transform': (1.0, 0.0, 260835.20466451097, 0.0, -1.0, 6243703.887301971, 0.0, 0.0, 1.0), 'width': 103}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

