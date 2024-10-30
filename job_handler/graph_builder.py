import os
# import dask
# import subprocess
import pandas as pd
import shapely
#
# from src.src_tools import get_application_path
from workers.city_data import parse_processing_areas_config, CityData, parse_filenames_config
#
# CIF_DATA_MODULE_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'source_cif_data_downloader.py'))
# PLUGIN_MODULE_PATH = os.path.abspath(os.path.join(get_application_path(), 'workers', 'umep_plugin_processor.py'))
#
def _build_source_dataframes(source_base_path, city_folder_name):
    config_processing_file_path = str(os.path.join(source_base_path, city_folder_name, CityData.filename_umep_city_processing_config))
    processing_config_df = pd.read_csv(config_processing_file_path)

    return processing_config_df

def _get_aoi_dimensions(source_base_path, city_folder_name):
    source_city_path = str(os.path.join(source_base_path, city_folder_name))

    min_lon, min_lat, max_lon, max_lat, cell_size = \
        parse_processing_areas_config(source_city_path, CityData.filename_method_parameters_config)

    # aoi_boundary = str(shapely.box(min_lon, min_lat, max_lon, max_lat))

    from city_metrix.layers.layer import create_fishnet_grid
    fishnet = create_fishnet_grid(min_lon, min_lat, max_lon, max_lat, cell_size)

    return fishnet

def _get_cif_features(source_city_path):
    dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_no_custom_features, feature_list =\
        parse_filenames_config(source_city_path, CityData.filename_method_parameters_config)

    custom_file_names = []
    if 'dem' not in feature_list:
        custom_file_names.append(dem_tif_filename)
    if 'dsm' not in feature_list:
        custom_file_names.append(dsm_tif_filename)
    if 'tree_canopy' not in feature_list:
        custom_file_names.append(tree_canopy_tif_filename)
    if 'lulc' not in feature_list:
        custom_file_names.append(lulc_tif_filename)

    if has_no_custom_features:
        return custom_file_names, has_no_custom_features, None
    else:
        features = ','.join(feature_list)
        return custom_file_names, has_no_custom_features, features


# def _build_processing_graphs(enabled_processing_task_df, source_base_path, target_base_path, city_folder_name):
#     pre_proc_delayed_results = []
#     delayed_results = []
#     solweig_delayed_results = []
#     for index, config_row in enabled_processing_task_df.iterrows():
#         task_index = index
#         task_method = config_row.method
#         tile_folder_name = config_row.tile_folder_name
#         # start_tile_id
#         # end_tile_id
#
#         #TODO
#         tile_boundary = _get_aoi_dimensions(source_base_path, city_folder_name)
#
#         pre_proc_array = _build_pre_processing_steps(task_index, 0, source_base_path, city_folder_name, tile_folder_name, tile_boundary)
#         if pre_proc_array:
#             pre_delayed_result = dask.delayed(subprocess.run)(pre_proc_array, capture_output=True, text=True)
#             pre_proc_delayed_results.append(pre_delayed_result)
#
#         if task_method == 'solweig_full':
#             dep_delayed_results, solweig_task_delayed_results = \
#                 _build_solweig_full_dependency(task_index, city_folder_name, tile_folder_name, source_base_path, target_base_path)
#             delayed_results.extend(dep_delayed_results)
#             solweig_delayed_results.extend(solweig_task_delayed_results)
#         elif task_method == 'solweig_only':
#             delayed_result = _build_solweig_only_steps(task_index, 1, city_folder_name,
#                                                        tile_folder_name, source_base_path, target_base_path)
#             delayed_results.append(delayed_result)
#         else:
#             proc_array = _construct_proc_array(task_index, 1, task_method, city_folder_name, tile_folder_name,
#                                   source_base_path, target_base_path)
#             delayed_result = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
#             delayed_results.append(delayed_result)
#
#     return pre_proc_delayed_results, delayed_results, solweig_delayed_results
#

#

#
#
# def _build_pre_processing_steps(task_index, step_index, source_base_path, city_folder_name, tile_folder_name, tile_boundary):
#     source_city_path = str(os.path.join(source_base_path, city_folder_name))
#     features = _get_cif_features(source_city_path)
#
#     if features:
#         pre_processing_delayed_results = \
#             _construct_pre_proc_array(task_index, step_index, city_folder_name, tile_folder_name, source_base_path, tile_boundary, features)
#
#         return pre_processing_delayed_results
#     else:
#         return None
#

#
# def _construct_pre_proc_array(task_index, step_index, folder_name_city_data, folder_name_tile_data, output_base_path, aoi_boundary, features):
#     proc_array = ['python', CIF_DATA_MODULE_PATH,
#                   f'--task_index={task_index}', f'--step_index={step_index}',
#                   f'--output_base_path={output_base_path}',
#                   f'--folder_name_city_data={folder_name_city_data}',
#                   f'--folder_name_tile_data={folder_name_tile_data}',
#                   f'--aoi_boundary={aoi_boundary}',
#                   f'--features={features}'
#                   ]
#     return proc_array
#
#
# def _build_solweig_full_dependency(task_index, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
#     dep_delayed_results = []
#     proc_array = _construct_proc_array(task_index, 1, 'wall_height_aspect', folder_name_city_data, folder_name_tile_data,
#                                        source_base_path, target_base_path, None, None)
#     walls = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
#     dep_delayed_results.append(walls)
#
#     proc_array = _construct_proc_array(task_index, 2, 'skyview_factor', folder_name_city_data, folder_name_tile_data,
#                                        source_base_path, target_base_path, None, None)
#     skyview = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
#     dep_delayed_results.append(skyview)
#
#     solweig_delayed_results = _build_solweig_only_steps(task_index, 3, folder_name_city_data,
#                                                         folder_name_tile_data, source_base_path, target_base_path)
#
#     return dep_delayed_results, solweig_delayed_results
#
#
# def _build_solweig_only_steps(task_index, step_index, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
#     city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
#
#     delayed_result = []
#     for met_file in city_data.met_files:
#         met_filename = met_file.get('filename')
#         utc_offset = met_file.get('utc_offset')
#
#         proc_array = _construct_proc_array(task_index, step_index, 'solweig_only', folder_name_city_data, folder_name_tile_data,
#                                            source_base_path, target_base_path, met_filename, utc_offset)
#         solweig = dask.delayed(subprocess.run)(proc_array, capture_output=True, text=True)
#         delayed_result.append(solweig)
#     return delayed_result
#
#
# def _construct_proc_array(task_index, step_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path,
#                           met_filename=None, utc_offset=None):
#     proc_array = ['python', PLUGIN_MODULE_PATH,
#                   f'--task_index={task_index}', f'--step_index={step_index}', f'--step_method={step_method}',
#                   f'--folder_name_city_data={folder_name_city_data}',
#                   f'--folder_name_tile_data={folder_name_tile_data}',
#                   f'--source_data_path={source_base_path}', f'--target_path={target_base_path}',
#                   f'--met_filename={met_filename}', f'--utc_offset={utc_offset}']
#     return proc_array
