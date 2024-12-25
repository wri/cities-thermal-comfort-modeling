from src.constants import *
from src.workers.config_processor import *


class CityData:

    def __new__(cls, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
        obj = super().__new__(cls)

        obj.folder_name_city_data = folder_name_city_data
        obj.folder_name_tile_data = folder_name_tile_data

        obj.source_base_path = source_base_path

        obj.source_city_path = str(os.path.join(source_base_path, folder_name_city_data))
        obj.source_city_data_path = str(os.path.join(obj.source_city_path, FOLDER_NAME_PRIMARY_DATA))

        obj.city_processing_config_path = os.path.join(obj.source_city_path, FILENAME_PROCESSING_CONFIG)
        obj.city_method_config_path = os.path.join(obj.source_city_path, FILENAME_METHOD_CONFIG)

        (obj.wall_lower_limit_height, obj.light_transmissivity, obj.trunk_zone_height, obj.leaf_start, obj.leaf_end,
         obj.conifer_trees, obj.albedo_walls, obj.albedo_ground, obj.emis_walls, obj.emis_ground, obj.output_tmrt,
         obj.output_sh, obj.sampling_local_hours, obj.met_filenames) = (parse_method_attributes_conf(obj.source_city_path))

        (obj.dem_tif_filename, obj.dsm_tif_filename, obj.tree_canopy_tif_filename, obj.lulc_tif_filename,
         has_custom_features, obj.custom_feature_list, obj.cif_feature_list) = (
            parse_filenames_config(obj.source_city_path))

        obj.utc_offset, obj.min_lon, obj.min_lat, obj.max_lon, obj.max_lat, tile_side_meters, tile_buffer_meters = \
            parse_processing_areas_config(obj.source_city_path)

        if obj.folder_name_tile_data:
            obj.source_raster_files_path = os.path.join(obj.source_city_data_path, FOLDER_NAME_PRIMARY_RASTER_FILES)
            obj.source_primary_raster_tile_data_path = os.path.join(obj.source_raster_files_path, obj.folder_name_tile_data)
            obj.source_dem_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dem_tif_filename)
            obj.source_dsm_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dsm_tif_filename)
            obj.source_tree_canopy_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.tree_canopy_tif_filename)
            obj.source_land_cover_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.lulc_tif_filename)
        else:
            obj.source_primary_raster_tile_data_path = None
            obj.source_dem_path = None
            obj.source_dsm_path = None
            obj.source_tree_canopy_path = None
            obj.source_land_cover_path = None

        obj.source_met_filenames_path = os.path.join(obj.source_city_data_path, FOLDER_NAME_PRIMARY_MET_FILENAMES)

        if target_base_path:
            obj.target_base_path = target_base_path

            obj.target_city_path = str(os.path.join(target_base_path, folder_name_city_data))
            obj.target_city_data_path = str(os.path.join(obj.target_city_path, FOLDER_NAME_PRIMARY_DATA))

            obj.target_path_city_data = str(os.path.join(obj.target_base_path, folder_name_city_data,
                                                         FOLDER_NAME_RESULTS))
            obj.target_manager_log_path = os.path.join(obj.target_city_path, '.logs', 'worker_manager.log')
            obj.target_preprocessed_path = os.path.join(obj.target_path_city_data, FOLDER_NAME_PREPROCESSED_DATA)
            obj.target_tcm_results_path = os.path.join(obj.target_path_city_data, FOLDER_NAME_TCM_RESULTS)

            if obj.folder_name_tile_data:
                # source_primary_raster_tile_data_path
                obj.target_raster_files_path = os.path.join(obj.target_city_data_path,
                                                            FOLDER_NAME_PRIMARY_RASTER_FILES)
                obj.target_primary_tile_data_path = os.path.join(obj.target_raster_files_path, obj.folder_name_tile_data)

                obj.target_dem_path = os.path.join(obj.target_primary_tile_data_path, obj.dem_tif_filename)
                obj.target_dsm_path = os.path.join(obj.target_primary_tile_data_path, obj.dsm_tif_filename)
                obj.target_tree_canopy_path = os.path.join(obj.target_primary_tile_data_path,
                                                           obj.tree_canopy_tif_filename)
                obj.target_land_cover_path = os.path.join(obj.target_primary_tile_data_path, obj.lulc_tif_filename)

                obj.target_preprocessed_tile_data_path = os.path.join(obj.target_preprocessed_path, obj.folder_name_tile_data)
                obj.target_wallheight_path = os.path.join(obj.target_preprocessed_tile_data_path, FILENAME_WALL_HEIGHT)
                obj.target_wallaspect_path = os.path.join(obj.target_preprocessed_tile_data_path, FILENAME_WALL_ASPECT)
                obj.target_svfszip_path = os.path.join(obj.target_preprocessed_tile_data_path, FILENAME_SVFS_ZIP)
            else:
                obj.target_preprocessed_tile_data_path = None
                obj.target_dem_path = None
                obj.target_dsm_path = None
                obj.target_tree_canopy_path = None
                obj.target_land_cover_path = None
                obj.target_wallheight_path = None
                obj.target_wallaspect_path = None
                obj.target_svfszip_path = None

            obj.target_met_filenames_path = os.path.join(obj.target_city_data_path, FOLDER_NAME_PRIMARY_MET_FILENAMES)

        return obj

