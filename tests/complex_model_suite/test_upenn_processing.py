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
def test_ZAF_Capetown_small_mixed_cif_upenn():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_75_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)

        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260738.78469714464, 0.0, -1.0, 6243803.498204948, 0.0, 0.0, 1.0),
                              'width': 296, 'height': 315, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['89842f72ab60d295741932c2678b5c60'],
                              'full_checksum': '89842f72ab60d295741932c2678b5c60'}

        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 15
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_tiled_buffered_cif_city_upenn():
    source_city_folder_name = 'NLD_Amsterdam_buftile_cif_upenn'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2019_206_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32631',
                              'transform': (1.0, 0.0, 629427.0, 0.0, -1.0, 5804286.0, 0.0, 0.0, 1.0),
                              'width': 170, 'height': 170, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['80d87e88ef5c1f51ce5f5f609a2e4ab1'],
                              'full_checksum': '80d87e88ef5c1f51ce5f5f609a2e4ab1'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        # Commenting out due to pyqt4 error reported in response
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
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_250_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32618',
                              'transform': (1.0, 0.0, 485134.27940224315, 0.0, -1.0, 4422427.786648174, 0.0, 0.0, 1.0),
                              'width': 460, 'height': 510, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['ccf4a7bbf40e0ca37b95adee7a89ffe3'],
                              'full_checksum': 'ccf4a7bbf40e0ca37b95adee7a89ffe3'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        # Commenting out due to pyqt4 error reported in response
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
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32610',
                              'transform': (1.0, 0.0, 523900.53136494954, 0.0, -1.0, 5041803.745006356, 0.0, 0.0, 1.0),
                              'width': 255, 'height': 183, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['e1308c5b52c11f2ba2e2dfc44f2eaf4c'],
                              'full_checksum': 'e1308c5b52c11f2ba2e2dfc44f2eaf4c'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        # Commenting out due to pyqt4 error reported in response
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
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32610',
                              'transform': (1.0, 0.0, 523140.79269659996, 0.0, -1.0, 5041128.5871465625, 0.0, 0.0, 1.0),
                              'width': 889, 'height': 373, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['df74f73b398c22daa244e534e49134b2'],
                              'full_checksum': 'df74f73b398c22daa244e534e49134b2'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        # Commenting out due to pyqt4 error reported in response
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
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_247_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32618',
                              'transform': (1.0, 0.0, 324731.0, 0.0, -1.0, 4309140.0, 0.0, 0.0, 1.0),
                              'width': 857, 'height': 669, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['e62acaa434b89aa526ec598a1e791091'],
                              'full_checksum': 'e62acaa434b89aa526ec598a1e791091'}

        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        # Commenting out due to pyqt4 error reported in response
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
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)

        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260838.78469714464, 0.0, -1.0, 6243703.498204948, 0.0, 0.0, 1.0),
                              'width': 97, 'height': 115, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['28c6862f22c27fef5d502635affaf7f2'],
                              'full_checksum': '28c6862f22c27fef5d502635affaf7f2'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)

        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

