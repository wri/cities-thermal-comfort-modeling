import faulthandler
import os
import shutil

import pytest
import tempfile
from pathlib import Path

from src.CityData import instantiate_city_data
from src.umep_for_processing_plugins import UmepProcessingQgisPlugins
from test.tools import is_valid_output_file, is_valid_output_directory

SOURCE_PATH = os.path.join(os.path.dirname(os.getcwd()), 'sample_cities')
CITY_FOLDER_NAME = 'ZAF_Capetown_small_tile'
from src.qgis_initializer import QgisHandler
qh = QgisHandler(0)

faulthandler.enable()

@pytest.fixture
def startup_teardown():
    qgis_app = qh.qgis_app
    UPP = UmepProcessingQgisPlugins(qgis_app)
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    yield UPP, temp_dir
    # Cleanup: delete the directory after the test
    # UPP.shutdown_qgis()
    # shutil.rmtree(temp_dir)

def test_wall_height_aspect(startup_teardown):
    runID = 'test_wall_dimensions'
    UPP = startup_teardown[0]
    temp_dir = startup_teardown[1]
    city_data = instantiate_city_data(CITY_FOLDER_NAME, 'tile1', SOURCE_PATH, temp_dir)
    return_code = UPP.generate_wall_height_aspect(runID, city_data)

    out_directory = city_data.target_preprocessed_data_path
    is_valid_directory = is_valid_output_directory(out_directory)
    wallheight_path = city_data.wallheight_path
    wallaspect_path = city_data.wallaspect_path
    wall_height_file_exists = os.path.isfile(wallheight_path)
    wall_aspect_file_exists = os.path.isfile(wallaspect_path)

    assert return_code == 0
    assert is_valid_directory
    assert wall_height_file_exists and wall_aspect_file_exists


def test_skyview_factor(startup_teardown):
    runID = 'test_skyview_factor'
    UPP = startup_teardown[0]
    temp_dir = startup_teardown[1]

    city_data = instantiate_city_data(CITY_FOLDER_NAME, 'tile1', SOURCE_PATH, temp_dir)
    return_code = UPP.generate_skyview_factor_files(runID, city_data)

    out_directory = city_data.target_preprocessed_data_path
    is_valid_directory = is_valid_output_directory(out_directory)
    skyview_zip_path = city_data.svfszip_path
    skyview_zip_exists = os.path.isfile(skyview_zip_path)

    assert return_code == 0
    assert is_valid_directory
    assert skyview_zip_exists


# Prerequisites: The above tests for building dimensions and skview must be run prior to running this test
# since solweige depends on output from above tests.
def test_solweig_generator(startup_teardown):
    runID = 'test_solweig'
    step = 0
    met_file_name = 'met_20jan2022.txt'
    utc_offset = 2

    UPP = startup_teardown[0]
    temp_dir = startup_teardown[1]

    city_data = instantiate_city_data(CITY_FOLDER_NAME, 'tile1', SOURCE_PATH, temp_dir)

    UPP.generate_wall_height_aspect(runID, city_data)
    UPP.generate_skyview_factor_files(runID, city_data)
    return_code = UPP.generate_solweig(runID, step, city_data, met_file_name, utc_offset)

    out_directory = os.path.join(city_data.target_tcm_results_path, Path(met_file_name).stem)
    is_valid_directory = is_valid_output_directory(out_directory)
    tmrt_average_file_path = os.path.join(out_directory, 'Tmrt_average.tif')
    tmrt_average_file_exists = os.path.isfile(tmrt_average_file_path)

    assert return_code == 0
    assert is_valid_directory
    assert tmrt_average_file_exists


# def test_shadow_generation():
#     city_project_name = 'SWE_Goteborg'
#     start_year = 2022
#     start_month = 3
#     start_day = 15
#     number_of_days_in_run = 2
#
#     project_path = os.path.dirname(os.getcwd())
#     project_data_path = os.path.join(project_path, 'test', 'resources', city_project_name)
#     source_data_path = os.path.join(project_data_path, 'source_data')
#     dsm_tif = os.path.join(source_data_path, 'DSM_KRbig.tif')
#     cdsm_tif = os.path.join(source_data_path, 'CDSM_KRbig.asc')
#     out_path = os.path.join(project_data_path, 'oneoff_test_results','Shadow_Results.tif')
#
#     runID = 'test'
#     UPP.generate_aggregated_shadows(runID, None, dsm_tif, cdsm_tif, start_year, start_month, start_day, number_of_days_in_run, out_path)
#
#     assert is_valid_output_file(out_path) == True
#     out_folder = os.path.dirname(os.path.abspath(out_path))
#     remove_folder(out_folder)
