# import faulthandler
# import os
# import shutil
#
# import pytest
# import tempfile
# from pathlib import Path
#
# from src_old.CityData import instantiate_city_data
# from src_old.umep_for_processing_plugins import UmepProcessingQgisPlugins
# from test.tools import is_valid_output_file, is_valid_output_directory
#
# SOURCE_PATH = os.path.join(os.path.dirname(os.getcwd()), 'sample_cities')
# folder_name_city_data = 'ZAF_Capetown_small_tile'
# from src_old.qgis_initializer import QgisHandler
# QH = QgisHandler(0)
# UMEP_PLUGIN = UmepProcessingQgisPlugins(QH.qgis_app)
#
# faulthandler.enable()
#
# @pytest.fixture
# def startup_teardown():
#     # Create a temporary directory
#     temp_dir = tempfile.mkdtemp()
#     yield temp_dir
#     # Cleanup: delete the directory after the test
#     qa = QH.qgis_app
#     qa.exitQgis()
#     shutil.rmtree(temp_dir)
#
# def test_wall_height_aspect(startup_teardown):
#     task_index = 'test_wall_dimensions'
#     temp_dir = startup_teardown
#     city_data = instantiate_city_data(folder_name_city_data, 'tile1', SOURCE_PATH, temp_dir)
#     return_code = UMEP_PLUGIN.generate_wall_height_aspect(task_index, city_data)
#
#     target_wallheight_path = city_data.target_wallheight_path
#     target_wallaspect_path = city_data.target_wallaspect.path
#     wall_height_file_exists = os.path.isfile(target_wallheight_path)
#     wall_aspect_file_exists = os.path.isfile(target_wallaspect_path)
#
#     assert return_code == 0
#     assert wall_height_file_exists and wall_aspect_file_exists
#
#
# def test_skyview_factor(startup_teardown):
#     task_index = 'test_skyview_factor'
#     temp_dir = startup_teardown
#     city_data = instantiate_city_data(folder_name_city_data, 'tile1', SOURCE_PATH, temp_dir)
#     return_code = UMEP_PLUGIN.generate_skyview_factor_files(task_index, city_data)
#
#     skyview_zip_path = city_data.target_svfszip_path
#     skyview_zip_exists = os.path.isfile(skyview_zip_path)
#
#     assert return_code == 0
#     assert skyview_zip_exists
#
#
# # Prerequisites: The above tests for building dimensions and skview must be run prior to running this test
# # since solweige depends on output from above tests.
# def test_solweig_generator(startup_teardown):
#     task_index = 'test_solweig'
#     step = 0
#     met_filename = 'met_20jan2022.txt'
#     seasonal_utc_offset = 2
#
#     temp_dir = startup_teardown
#     city_data = instantiate_city_data(folder_name_city_data, 'tile1', SOURCE_PATH, temp_dir)
#
#     UMEP_PLUGIN.generate_wall_height_aspect(task_index, city_data)
#     UMEP_PLUGIN.generate_skyview_factor_files(task_index, city_data)
#     return_code = UMEP_PLUGIN.generate_solweig(task_index, step, city_data, met_filename, seasonal_utc_offset)
#
#     # out_directory = os.path.join(city_data.target_tcm_results_path, Path(met_filename).stem)
#
#     target_met_folder = os.path.join(city_data.target_tcm_results_path, Path(met_filename).stem, city_data.folder_name_tile_data)
#     tmrt_average_file_path = os.path.join(target_met_folder, 'Tmrt_average.tif')
#     tmrt_average_file_exists = os.path.isfile(tmrt_average_file_path)
#
#     assert return_code == 0
#     assert tmrt_average_file_exists
#
#
# # def test_shadow_generation():
# #     city_project_name = 'SWE_Goteborg'
# #     start_year = 2022
# #     start_month = 3
# #     start_day = 15
# #     number_of_days_in_run = 2
# #
# #     project_path = os.path.dirname(os.getcwd())
# #     project_data_path = os.path.join(project_path, 'test', 'resources', city_project_name)
# #     source_data_path = os.path.join(project_data_path, 'source_data')
# #     dsm_tif = os.path.join(source_data_path, 'DSM_KRbig.tif')
# #     cdsm_tif = os.path.join(source_data_path, 'CDSM_KRbig.asc')
# #     out_path = os.path.join(project_data_path, 'oneoff_test_results','Shadow_Results.tif')
# #
# #     task_index = 'test'
# #     UPP.generate_aggregated_shadows(task_index, None, dsm_tif, cdsm_tif, start_year, start_month, start_day, number_of_days_in_run, out_path)
# #
# #     assert is_valid_output_file(out_path) == True
# #     out_folder = os.path.dirname(os.path.abspath(out_path))
# #     remove_folder(out_folder)
