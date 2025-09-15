import os
import pytest
from pytest_check import equal # https://github.com/okken/pytest-check

from src.constants import DATA_DIR
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, create_folder
from tests.conftest import RUN_COMPLEX_MODEL_SUITE
from tests.testing_tools import run_main, SAMPLE_CITIES_SOURCE_DIR, file_count_in_vrt_directory, \
    does_file_signature_match, get_geotiff_signature

CLEANUP_RESULTS=False

SCRATCH_TARGET_DIR = os.path.join(DATA_DIR, 'scratch_target')
create_folder(SCRATCH_TARGET_DIR)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_tropical_full_cif_city():
    source_city_folder_name = 'BRA_Rio_de_Janeiro_full_cif'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2022_365_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32723',
                              'transform': (1.0, 0.0, 686601.6011767144, 0.0, -1.0, 7465969.5343720205, 0.0, 0.0, 1.0),
                              'width': 310, 'height': 246, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['f54fca31439a4954cfc0759e1ef467fa'],
                              'full_checksum': 'f54fca31439a4954cfc0759e1ef467fa'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_tiled_cif_city():
    source_city_folder_name = 'NLD_Amsterdam'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_189_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32631',
                              'transform': (1.0, 0.0, 629418.0, 0.0, -1.0, 5804286.0, 0.0, 0.0, 1.0),
                              'width': 170, 'height': 170, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['4178f0d4ce1b6019d96719f105d8b04e'],
                              'full_checksum': '4178f0d4ce1b6019d96719f105d8b04e'}

        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_tiled_buffered_cif_city_umep():
    source_city_folder_name = 'NLD_Amsterdam_buftile_cif_umep'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2019_206_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32631',
                              'transform': (1.0, 0.0, 629418.0, 0.0, -1.0, 5804286.0, 0.0, 0.0, 1.0),
                              'width': 170, 'height': 170, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['adcbc49f7b49adc5b8b3e6f456ba845f'],
                              'full_checksum': 'adcbc49f7b49adc5b8b3e6f456ba845f'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_tiled_custom_city():
    source_city_folder_name = 'NLD_Amsterdam_custom_tiled'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_189_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32631',
                              'transform': (1.0, 0.0, 629318.3245797384, 0.0, -1.0, 5804463.641838525, 0.0, 0.0, 1.0),
                              'width': 450, 'height': 450, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['c64b531dafe2257e2b5760227bc4c413'],
                              'full_checksum': 'c64b531dafe2257e2b5760227bc4c413'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_portland_swath_cif_umep():
    source_city_folder_name = 'USA_Portland_swath_cif_umep'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_226_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32610',
                              'transform': (1.0, 0.0, 523139.428703723, 0.0, -1.0, 5041128.5871465625, 0.0, 0.0, 1.0),
                              'width': 891, 'height': 373, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['29b091b5dbb5c766da83f903753826a4'],
                              'full_checksum': '29b091b5dbb5c766da83f903753826a4'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_USA_WashingtonDC_cif_umep():
    source_city_folder_name = 'USA_WashingtonDC_cif_umep'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_247_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32618',
                              'transform': (1.0, 0.0, 324731.0, 0.0, -1.0, 4309158.0, 0.0, 0.0, 1.0),
                              'width': 857, 'height': 705, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['8ce408dfcd6f6912239a584e9369a1a9'],
                              'full_checksum': '8ce408dfcd6f6912239a584e9369a1a9'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_download_only_cif_city():
    source_city_folder_name = 'ZAF_Capetown_cif_download_only'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        equal(0, return_code, msg=f"Expected 0 for return code, but actual return code is {return_code}")
        expected_count = 8
        equal(vrt_count, expected_count,
              msg=f"Expected VRT count of {expected_count} files, but actual count is {vrt_count}")
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_ZAF_Capetown_cif_local_era5_umep():
    source_city_folder_name = 'ZAF_Capetown_cif_local_era5_umep'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
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


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_mixed_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_mixed_cif'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0),
                              'width': 302, 'height': 315, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['9ee65c52c838e349a7329e8f2b3617bd'],
                              'full_checksum': '9ee65c52c838e349a7329e8f2b3617bd'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 16
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_custom_city():
    source_city_folder_name = 'ZAF_Capetown_small_tile'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0),
                              'width': 302, 'height': 315, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['49512ce8d8a484c8537ff35e4c8efb2c'],
                              'full_checksum': '49512ce8d8a484c8537ff35e4c8efb2c'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 22
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_custom_city_with_full_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_full_intermediates'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_1_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0),
                              'width': 302, 'height': 315, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['49512ce8d8a484c8537ff35e4c8efb2c'],
                              'full_checksum': '49512ce8d8a484c8537ff35e4c8efb2c'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 22
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_custom_city_with_mixed_intermediates():
    source_city_folder_name = 'ZAF_Capetown_with_mixed_intermediates'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)

    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2022_20_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_20jan2022', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32734',
                              'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0),
                              'width': 302, 'height': 315, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['d474ea7177771fe0d378f9286739a6a6'],
                              'full_checksum': 'd474ea7177771fe0d378f9286739a6a6'}

        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 22
    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)


@pytest.mark.skipif(RUN_COMPLEX_MODEL_SUITE is False, reason=f"Skipping since RUN_FULL_TEST_SUITE set to {RUN_COMPLEX_MODEL_SUITE}")
def test_USA_Philadelphia_downtown_cif_umep():
    source_city_folder_name = r'USA_Philadelphia_downtown_cif_umep'
    non_tiled_city_data = CityData(None, source_city_folder_name, None, SAMPLE_CITIES_SOURCE_DIR, SCRATCH_TARGET_DIR)
    try:
        remove_folder(non_tiled_city_data.target_city_parent_path)
        return_code = run_main(SCRATCH_TARGET_DIR, source_city_folder_name, 'run_pipeline')

        mrt_file_name = 'Tmrt_2023_250_1200D.tif'
        target_met_file = os.path.join(non_tiled_city_data.target_tcm_results_path, 'met_era5_hottest_days', 'tile_00001',
                                       mrt_file_name)
        expected_signature = {'crs': 'EPSG:32618',
                              'transform': (1.0, 0.0, 485134.27940224315, 0.0, -1.0, 4422428.667472388, 0.0, 0.0, 1.0),
                              'width': 460, 'height': 512, 'count': 1, 'dtype': ('float32',),
                              'band_checksums': ['56b2647f2e2bda193bb06f7dbeee0676'],
                              'full_checksum': '56b2647f2e2bda193bb06f7dbeee0676'}
        is_matched, actual_file_signature = does_file_signature_match(expected_signature, target_met_file)
        assert is_matched, f"Expected signature does not match actual: ({actual_file_signature})"

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

