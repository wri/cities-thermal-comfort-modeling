import faulthandler
import os
import pytest

from src.umep_for_processing_plugins import UmepProcessingQgisPlugins
from src.tools import remove_folder, clean_folder
from test.tools import is_valid_output_file, is_valid_output_directory

faulthandler.enable()

UPP = UmepProcessingQgisPlugins()

def test_wall_height_aspect(setup_city_data):
    runID = 'test_wall_dimensions'

    return_code = UPP.generate_wall_height_aspect(runID, setup_city_data)

    out_directory = setup_city_data.preprocessed_data_path
    wallheight_path = setup_city_data.wallheight_path
    wallaspect_path = setup_city_data.wallaspect_path
    wall_height_file_exists = os.path.isfile(wallheight_path)
    wall_aspect_file_exists = os.path.isfile(wallaspect_path)

    assert return_code == 0
    assert is_valid_output_directory(out_directory) == True
    assert wall_height_file_exists and wall_aspect_file_exists


def test_skyview_factor(setup_city_data):
    runID = 'test_skyview_factor'

    return_code = UPP.generate_skyview_factor_files(runID, setup_city_data)
    assert return_code == 0

    out_directory = setup_city_data.preprocessed_data_path
    skyview_zip_path = setup_city_data.svfszip_path
    skyview_zip_exists = os.path.isfile(skyview_zip_path)

    assert is_valid_output_directory(out_directory) == True
    assert skyview_zip_exists


# Prerequisites: The above tests for building dimensions and skview must be run prior to running this test
# since solweige depends on output from above tests.
def test_solweig_generator(setup_city_data):
    runID = 'test_solweig'
    step = 0
    met_file_path = os.path.join(setup_city_data.met_files_path, 'met_20jan2022.txt')
    utc_offset = 2

    UPP.generate_wall_height_aspect(runID, setup_city_data)
    UPP.generate_skyview_factor_files(runID, setup_city_data)
    return_code = UPP.generate_solweig(runID, step, setup_city_data, met_file_path, utc_offset)
    assert return_code == 0

    out_directory = setup_city_data.tcm_results_path
    tmrt_average_file_path = os.path.join(setup_city_data.tcm_results_path, 'Tmrt_average.tif')
    tmrt_average_file_exists = os.path.isfile(tmrt_average_file_path)

    assert is_valid_output_directory(out_directory) == True
    assert tmrt_average_file_exists
    # _tcm_cleanup(city_data)

# def _tcm_cleanup(city_data):
#     remove_folder(city_data.preprocessed_data_path)
#     remove_folder(city_data.tcm_results_path)

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
