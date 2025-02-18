import os
from src.constants import FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_INTERMEDIATE_DATA, FOLDER_NAME_PRIMARY_MET_FILES, \
    FOLDER_NAME_PRIMARY_RASTER_FILES, FOLDER_NAME_UMEP_TCM_RESULTS
from src.workers.config_processor import *


class CityData:

    def __new__(cls, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
        obj = super().__new__(cls)

        obj.folder_name_city_data = folder_name_city_data
        obj.folder_name_tile_data = folder_name_tile_data

        obj.source_base_path = source_base_path

        obj.source_city_path = str(os.path.join(source_base_path, folder_name_city_data))
        obj.source_city_primary_data_path = str(os.path.join(obj.source_city_path, FOLDER_NAME_PRIMARY_DATA))
        obj.source_intermediate_data_path = str(os.path.join(obj.source_city_path, FOLDER_NAME_INTERMEDIATE_DATA))
        obj.source_met_files_path = os.path.join(obj.source_city_primary_data_path, FOLDER_NAME_PRIMARY_MET_FILES)
        obj.city_method_config_path = os.path.join(obj.source_city_path, FILENAME_METHOD_YML_CONFIG)

        # parse config file
        yml_values = read_yaml(obj.city_method_config_path)
        obj.scenario_short_title, obj.scenario_version, obj.scenario_description, obj.scenario_author = (
            parse_scenario_config(yml_values))

        obj.utc_offset, obj.min_lon, obj.min_lat, obj.max_lon, obj.max_lat, obj.tile_side_meters, obj.tile_buffer_meters = \
            parse_processing_areas_config(yml_values)

        obj.met_filenames, obj.has_era_met_download = parse_met_files_config(yml_values)

        (obj.dem_tif_filename, obj.dsm_tif_filename, obj.tree_canopy_tif_filename, obj.lulc_tif_filename,
         obj.custom_primary_feature_list, obj.custom_primary_filenames, obj.cif_primary_feature_list) = (
            parse_primary_filenames_config(yml_values))

        (obj.wall_aspect_filename, obj.wall_height_filename, obj.skyview_factor_filename,
         obj.custom_intermediate_list, obj.ctcm_intermediate_list) =\
            parse_intermediate_filenames_config(yml_values)

        (obj.new_task_method, northern_leaf_start, northern_leaf_end, southern_leaf_start, southern_leaf_end,
         obj.wall_lower_limit_height, obj.light_transmissivity, obj.trunk_zone_height,
         obj.conifer_trees, obj.albedo_walls, obj.albedo_ground, obj.emis_walls, obj.emis_ground, obj.output_tmrt,
         obj.output_sh, obj.sampling_local_hours) = (parse_method_attributes_config(yml_values))

        # set leave start/end for latitude
        obj.leaf_start, obj.leaf_end =_get_latitude_based_leaf_start_end(obj.min_lat, obj.max_lat,
                                                                 northern_leaf_start, northern_leaf_end,
                                                                 southern_leaf_start, southern_leaf_end)

        if obj.folder_name_tile_data:
            obj.source_raster_files_path = os.path.join(obj.source_city_primary_data_path, FOLDER_NAME_PRIMARY_RASTER_FILES)
            obj.source_primary_raster_tile_data_path = os.path.join(obj.source_raster_files_path, obj.folder_name_tile_data)
            obj.source_dem_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dem_tif_filename)
            obj.source_dsm_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dsm_tif_filename)
            obj.source_tree_canopy_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.tree_canopy_tif_filename)
            obj.source_land_cover_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.lulc_tif_filename)

            source_intermediate_tile_data_path = os.path.join(obj.source_intermediate_data_path, obj.folder_name_tile_data)
            obj.source_wallheight_path = os.path.join(source_intermediate_tile_data_path, obj.wall_height_filename)
            obj.source_wallaspect_path = os.path.join(source_intermediate_tile_data_path, obj.wall_aspect_filename)
            obj.source_svfszip_path = os.path.join(source_intermediate_tile_data_path, obj.skyview_factor_filename)


        if target_base_path:
            obj.target_base_path = target_base_path

            cleaned_tile = str(obj.scenario_short_title).strip().replace(" ", "_").replace(".","")
            cleaned_version = str(obj.scenario_version).strip().replace(".", "_")
            filled_title = f'{cleaned_tile}_v{cleaned_version}'

            scenario_sub_folder =  f'{folder_name_city_data}_{filled_title}'
            obj.target_city_parent_path = str(os.path.join(target_base_path, folder_name_city_data))
            obj.target_city_path = str(os.path.join(obj.target_city_parent_path, scenario_sub_folder))

            obj.target_city_primary_data_path = str(os.path.join(obj.target_city_path, FOLDER_NAME_PRIMARY_DATA))
            obj.target_met_files_path = os.path.join(obj.target_city_primary_data_path, FOLDER_NAME_PRIMARY_MET_FILES)

            obj.target_log_path = os.path.join(obj.target_city_path, '.admin')
            obj.target_manager_log_path = os.path.join(obj.target_log_path, 'log_worker_manager.log')
            obj.target_model_log_path = os.path.join(obj.target_log_path, 'log_model_execution.log')

            obj.target_qgis_data_path = os.path.join(obj.target_city_path, '.qgis_data')

            obj.target_intermediate_data_path = os.path.join(obj.target_city_path, FOLDER_NAME_INTERMEDIATE_DATA)

            obj.target_tcm_results_path = os.path.join(obj.target_city_path, FOLDER_NAME_UMEP_TCM_RESULTS)

            if obj.folder_name_tile_data:
                obj.target_raster_files_path = os.path.join(obj.target_city_primary_data_path,
                                                            FOLDER_NAME_PRIMARY_RASTER_FILES)
                obj.target_primary_tile_data_path = os.path.join(obj.target_raster_files_path, obj.folder_name_tile_data)

                obj.target_dem_path = os.path.join(obj.target_primary_tile_data_path, obj.dem_tif_filename)
                obj.target_dsm_path = os.path.join(obj.target_primary_tile_data_path, obj.dsm_tif_filename)
                obj.target_tree_canopy_path = os.path.join(obj.target_primary_tile_data_path,
                                                           obj.tree_canopy_tif_filename)
                obj.target_land_cover_path = os.path.join(obj.target_primary_tile_data_path, obj.lulc_tif_filename)

                obj.target_intermediate_tile_data_path = os.path.join(obj.target_intermediate_data_path, obj.folder_name_tile_data)
                obj.target_wallheight_path = os.path.join(obj.target_intermediate_tile_data_path, obj.wall_height_filename)
                obj.target_wallaspect_path = os.path.join(obj.target_intermediate_tile_data_path, obj.wall_aspect_filename)
                obj.target_svfszip_path = os.path.join(obj.target_intermediate_tile_data_path, obj.skyview_factor_filename)

        return obj

def _get_latitude_based_leaf_start_end(min_lat, max_lat, northern_leaf_start, northern_leaf_end,
                                       southern_leaf_start, southern_leaf_end):
    tropical_latitude = 23.5
    mid_lat = (min_lat + max_lat) / 2
    if abs(mid_lat) <= tropical_latitude:  # tropical zone
        leaf_start = 0
        leaf_end = 365
    elif mid_lat > tropical_latitude:
        leaf_start = northern_leaf_start
        leaf_end = northern_leaf_end
    else:
        leaf_start = southern_leaf_start
        leaf_end = southern_leaf_end

    return leaf_start, leaf_end
