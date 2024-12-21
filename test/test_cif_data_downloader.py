# import os
#
# import shapely
#
# from src.workers.city_data import CityData
# from src.workers.source_cif_data_downloader import get_cif_data
# from test.testing_tools import is_valid_output_file, SAMPLE_CITIES_PATH
# from src.workers.worker_tools import remove_file, remove_folder
#
# min_lon = 4.901190775092289
# min_lat = 52.37197831356116
# max_lon = 4.908300489273159
# max_lat = 52.37520954271636
# tile_boundary = str(shapely.box(min_lon, min_lat, max_lon, max_lat))
#
# folder_name_city_data = 'NLD_Amsterdam'
# folder_name_tile_data = 'tile_099'
# city_data = CityData(folder_name_city_data, folder_name_tile_data, SAMPLE_CITIES_PATH, None)
# tile_data_path = city_data.source_tile_data_path
# test_task_index = -1
#
# DEBUG = True
#
# def test_get_cif_non_terrain_data():
#     # feature_list = ['era5', 'lulc', 'tree_canopy']
#     cif_feature_list = ['lulc', 'tree_canopy']
#     cif_features = ','.join(cif_feature_list)
#     has_custom_features = False
#
#     remove_output_files(cif_feature_list)
#
#     get_cif_data(test_task_index, SAMPLE_CITIES_PATH, folder_name_city_data, folder_name_tile_data, has_custom_features,
#                  cif_features, tile_boundary, None)
#
#     # if 'era5' in feature_list:
#     #     expected_file = os.path.join(tile_data_path, CityData)
#     #     assert is_valid_output_file(expected_file)
#
#     if 'lulc' in cif_feature_list:
#         expected_file = os.path.join(tile_data_path, city_data.lulc_tif_filename)
#         print(expected_file)
#         assert is_valid_output_file(expected_file)
#
#     if 'tree_canopy' in cif_feature_list:
#         expected_file = os.path.join(tile_data_path, city_data.tree_canopy_tif_filename)
#         print(expected_file)
#         assert is_valid_output_file(expected_file)
#
#     if not DEBUG:
#         remove_output_files(cif_feature_list)
#         remove_folder(tile_data_path)
#
#
# def test_get_cif_terrain_data():
#     cif_feature_list = ['dem', 'dsm']
#     cif_features = ','.join(cif_feature_list)
#     has_custom_features = False
#
#     remove_output_files(cif_feature_list)
#     get_cif_data(test_task_index, SAMPLE_CITIES_PATH, folder_name_city_data, folder_name_tile_data, has_custom_features,
#                  cif_features, tile_boundary, None)
#
#     if 'dem' in cif_feature_list:
#         expected_file =  os.path.join(tile_data_path, city_data.dem_tif_filename)
#         print(expected_file)
#         assert is_valid_output_file(expected_file)
#
#     if 'dsm' in cif_feature_list:
#         expected_file =  os.path.join(tile_data_path, city_data.dsm_tif_filename)
#         print(expected_file)
#         assert is_valid_output_file(expected_file)
#
#     if not DEBUG:
#         remove_output_files(cif_feature_list)
#         remove_folder(tile_data_path)
#
#
# def remove_output_files(feature_list):
#     # if 'era5' in feature_list:
#     #     remove_file(os.path.join(tile_data_path, city_data))
#     if 'lulc' in feature_list:
#         remove_file(os.path.join(tile_data_path, city_data.lulc_tif_filename))
#     if 'tree_canopy' in feature_list:
#         remove_file(os.path.join(tile_data_path, city_data.tree_canopy_tif_filename))
#     if 'dem' in feature_list:
#         remove_file(os.path.join(tile_data_path, city_data.dem_tif_filename))
#     if 'dsm' in feature_list:
#         remove_file(os.path.join(tile_data_path, city_data.dsm_tif_filename))
#
