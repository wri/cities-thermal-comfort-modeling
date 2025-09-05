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
        expected_signature = {'band_checksums': ['4840945959b4759decf80ac85dfa54c6'], 'count': 1, 'crs': 'EPSG:32723', 'dtype': ('float32',), 'full_checksum': '4840945959b4759decf80ac85dfa54c6', 'height': 246, 'transform': (1.0, 0.0, 686601.2556551297, 0.0, -1.0, 7465970.0709804855, 0.0, 0.0, 1.0), 'width': 311}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['d2b96b57cb0a6ed3c6e225d50958389a'], 'count': 1, 'crs': 'EPSG:32631', 'dtype': ('float32',), 'full_checksum': 'd2b96b57cb0a6ed3c6e225d50958389a', 'height': 171, 'transform': (1.0, 0.0, 629418.2692492839, 0.0, -1.0, 5804285.748513707, 0.0, 0.0, 1.0), 'width': 171}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['738000a292965c3bf6fbde06c8551602'], 'count': 1, 'crs': 'EPSG:32631', 'dtype': ('float32',), 'full_checksum': '738000a292965c3bf6fbde06c8551602', 'height': 171, 'transform': (1.0, 0.0, 629418.2692492839, 0.0, -1.0, 5804285.748513707, 0.0, 0.0, 1.0), 'width': 171}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['c64b531dafe2257e2b5760227bc4c413'], 'count': 1, 'crs': 'EPSG:32631', 'dtype': ('float32',), 'full_checksum': 'c64b531dafe2257e2b5760227bc4c413', 'height': 450, 'transform': (1.0, 0.0, 629318.3245797384, 0.0, -1.0, 5804463.641838525, 0.0, 0.0, 1.0), 'width': 450}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['a2515a6fd0393b23d3af997413613e23'], 'count': 1, 'crs': 'EPSG:32610', 'dtype': ('float32',), 'full_checksum': 'a2515a6fd0393b23d3af997413613e23', 'height': 374, 'transform': (1.0, 0.0, 523139.01534016046, 0.0, -1.0, 5041128.822227225, 0.0, 0.0, 1.0), 'width': 892}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['248171c754df8bb07ca5f16f23117891'], 'count': 1, 'crs': 'EPSG:32618', 'dtype': ('float32',), 'full_checksum': '248171c754df8bb07ca5f16f23117891', 'height': 707, 'transform': (1.0, 0.0, 324731.4891801595, 0.0, -1.0, 4309158.4549594745, 0.0, 0.0, 1.0), 'width': 858}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['6a540a521ef4d07158f4a5cfba77caf5'], 'count': 1, 'crs': 'EPSG:32734', 'dtype': ('float32',), 'full_checksum': '6a540a521ef4d07158f4a5cfba77caf5', 'height': 115, 'transform': (1.0, 0.0, 260835.20466451097, 0.0, -1.0, 6243703.887301971, 0.0, 0.0, 1.0), 'width': 103}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['fc95e1f47b07f3d43bcd4220dc12f449'], 'count': 1, 'crs': 'EPSG:32734', 'dtype': ('float32',), 'full_checksum': 'fc95e1f47b07f3d43bcd4220dc12f449', 'height': 315, 'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0), 'width': 302}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['49512ce8d8a484c8537ff35e4c8efb2c'], 'count': 1, 'crs': 'EPSG:32734', 'dtype': ('float32',), 'full_checksum': '49512ce8d8a484c8537ff35e4c8efb2c', 'height': 315, 'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0), 'width': 302}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['49512ce8d8a484c8537ff35e4c8efb2c'], 'count': 1, 'crs': 'EPSG:32734', 'dtype': ('float32',), 'full_checksum': '49512ce8d8a484c8537ff35e4c8efb2c', 'height': 315, 'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0), 'width': 302}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['d474ea7177771fe0d378f9286739a6a6'], 'count': 1, 'crs': 'EPSG:32734', 'dtype': ('float32',), 'full_checksum': 'd474ea7177771fe0d378f9286739a6a6', 'height': 315, 'transform': (1.0, 0.0, 260735.20466451097, 0.0, -1.0, 6243802.887301971, 0.0, 0.0, 1.0), 'width': 302}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

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
        expected_signature = {'band_checksums': ['ce74c8dc89cd374488b0604fcaa96838'], 'count': 1, 'crs': 'EPSG:32618', 'dtype': ('float32',), 'full_checksum': 'ce74c8dc89cd374488b0604fcaa96838', 'height': 512, 'transform': (1.0, 0.0, 485133.5636835794, 0.0, -1.0, 4422429.002821511, 0.0, 0.0, 1.0), 'width': 461}
        mrt_signatures_are_equal = does_file_signature_match(expected_signature, target_met_file)
        assert mrt_signatures_are_equal

        vrt_count = file_count_in_vrt_directory(non_tiled_city_data)

        assert return_code == 0
        assert vrt_count == 17

    finally:
        if CLEANUP_RESULTS:
            remove_folder(non_tiled_city_data.target_city_parent_path)

