import os

from city_metrix import GeoExtent
from city_metrix.constants import ProjectionType, GeoType
from city_metrix.metrix_tools import get_utm_zone_from_latlon_point, get_projection_type
from shapely import Point

from src.constants import FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_INTERMEDIATE_DATA, FOLDER_NAME_PRIMARY_MET_FILES, \
    FOLDER_NAME_PRIMARY_RASTER_FILES, FOLDER_NAME_UMEP_TCM_RESULTS, WGS_CRS, FOLDER_NAME_ADMIN_DATA, \
    FOLDER_NAME_QGIS_DATA
from src.workers.config_processor import *


class CityData:

    def __new__(cls, city_geoextent, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
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
        yml_values = get_yml_content(source_base_path, folder_name_city_data)
        (obj.scenario_scenario_id, obj.infra_id, obj.scenario_description, obj.scenario_author,
         obj.publishing_target) = parse_scenario_config(yml_values)

        (obj.seasonal_utc_offset, obj.city_json_str, obj.source_aoi_crs, obj.min_lon, obj.min_lat, obj.max_lon, obj.max_lat,
         obj.tile_side_meters, obj.tile_buffer_meters, obj.remove_mrt_buffer_for_final_output) = \
            parse_processing_areas_config(yml_values)

        if city_geoextent is not None:
            geo_bbox = city_geoextent.as_utm_bbox()
            obj.utm_grid_west = geo_bbox.min_x
            obj.utm_grid_south = geo_bbox.min_y
            obj.utm_grid_east = geo_bbox.max_x
            obj.utm_grid_north = geo_bbox.max_y
            obj.grid_crs = geo_bbox.crs
            if obj.min_lon is None:
                obj.min_lon = geo_bbox.min_x
                obj.min_lat = geo_bbox.min_y
                obj.max_lon = geo_bbox.max_x
                obj.max_lat = geo_bbox.max_y
                obj.source_aoi_crs = geo_bbox.crs
        else:
            if obj.min_lon is None:
                obj.utm_grid_west = None
                obj.utm_grid_south = None
                obj.utm_grid_east = None
                obj.utm_grid_north = None
                obj.grid_crs = None
            else:
                bbox = [obj.min_lon, obj.min_lat, obj.max_lon, obj.max_lat]
                geo_bbox = GeoExtent(bbox, obj.source_aoi_crs).as_utm_bbox()
                obj.utm_grid_west = geo_bbox.min_x
                obj.utm_grid_south = geo_bbox.min_y
                obj.utm_grid_east = geo_bbox.max_x
                obj.utm_grid_north = geo_bbox.max_y
                obj.grid_crs = geo_bbox.crs

        (obj.dem_tif_filename, obj.dsm_tif_filename, obj.lulc_tif_filename, obj.open_urban_tif_filename, obj.tree_canopy_tif_filename,
         obj.albedo_cloud_masked_tif_filename, obj.custom_primary_feature_list, obj.custom_primary_filenames,
         obj.cif_primary_feature_list) = parse_primary_filenames_config(yml_values)

        (obj.processing_method, northern_leaf_start, northern_leaf_end, southern_leaf_start, southern_leaf_end,
         obj.wall_lower_limit_height, obj.light_transmissivity, obj.trunk_zone_height,
         obj.conifer_trees, obj.albedo_walls, obj.albedo_ground, obj.emis_walls, obj.emis_ground, obj.output_tmrt,
         obj.output_sh, obj.sampling_local_hours) = (parse_method_attributes_config(yml_values))

        (obj.wall_aspect_filename, obj.wall_height_filename, obj.skyview_factor_filename,
         obj.custom_intermediate_list, obj.ctcm_intermediate_list) = parse_intermediate_filenames_config(yml_values, obj.processing_method)

        # set leave start/end for latitude
        if obj.min_lat is not None:
            center_lat = (float(obj.min_lat) + float(obj.max_lat))/2
        else:
            center_lat = 45 # dummy value to avoid failure at this point
        obj.leaf_start, obj.leaf_end =_get_latitude_based_leaf_start_end(center_lat,
                                                                 northern_leaf_start, northern_leaf_end,
                                                                 southern_leaf_start, southern_leaf_end)

        # Adjust seasonal_utc_offset for time zones that do not have whole number offsets
        obj.seasonal_utc_offset = _time_shift_utc_offset(obj.seasonal_utc_offset)

        obj.met_filenames, obj.has_era_met_download, obj.era5_date_range = parse_met_files_config(yml_values, obj.processing_method)

        if obj.folder_name_tile_data:
            obj.source_raster_files_path = os.path.join(obj.source_city_primary_data_path, FOLDER_NAME_PRIMARY_RASTER_FILES)
            obj.source_primary_raster_tile_data_path = os.path.join(obj.source_raster_files_path, obj.folder_name_tile_data)
            obj.source_albedo_cloud_masked = os.path.join(obj.source_primary_raster_tile_data_path, obj.albedo_cloud_masked_tif_filename)
            obj.source_dem_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dem_tif_filename)
            obj.source_dsm_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dsm_tif_filename)
            obj.source_lulc_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.lulc_tif_filename)
            obj.source_open_urban_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.open_urban_tif_filename)
            obj.source_tree_canopy_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.tree_canopy_tif_filename)

            source_intermediate_tile_data_path = os.path.join(obj.source_intermediate_data_path, obj.folder_name_tile_data)
            obj.source_wallheight_path = os.path.join(source_intermediate_tile_data_path, obj.wall_height_filename)
            obj.source_wallaspect_path = os.path.join(source_intermediate_tile_data_path, obj.wall_aspect_filename)
            obj.source_svfszip_path = os.path.join(source_intermediate_tile_data_path, obj.skyview_factor_filename)


        if target_base_path:
            obj.target_base_path = target_base_path
            obj.scenario_title = str(obj.scenario_scenario_id).strip().replace(" ", "_").replace(".","")

            scenario_sub_folder =  f'{folder_name_city_data}_{obj.scenario_title}'
            obj.target_city_parent_path = str(os.path.join(target_base_path, folder_name_city_data))
            obj.target_city_path = str(os.path.join(obj.target_city_parent_path, scenario_sub_folder))

            obj.target_city_primary_data_path = str(os.path.join(obj.target_city_path, FOLDER_NAME_PRIMARY_DATA))
            obj.target_met_files_path = os.path.join(obj.target_city_primary_data_path, FOLDER_NAME_PRIMARY_MET_FILES)

            obj.target_log_path = os.path.join(obj.target_city_path, FOLDER_NAME_ADMIN_DATA)
            obj.target_manager_log_path = os.path.join(obj.target_log_path, 'log_worker_manager.log')
            obj.target_model_log_path = os.path.join(obj.target_log_path, 'log_model_execution.log')

            obj.target_qgis_data_path = os.path.join(obj.target_city_path, FOLDER_NAME_QGIS_DATA)

            obj.target_intermediate_data_path = os.path.join(obj.target_city_path, FOLDER_NAME_INTERMEDIATE_DATA)

            obj.target_tcm_results_path = os.path.join(obj.target_city_path, FOLDER_NAME_UMEP_TCM_RESULTS)

            if obj.folder_name_tile_data:
                metadata_file_name = f'{obj.folder_name_tile_data}_metadata.log'
                obj.target_umep_metadata_log_path = os.path.join(obj.target_log_path, 'model_metadata', metadata_file_name)

                obj.target_raster_files_path = os.path.join(obj.target_city_primary_data_path,
                                                            FOLDER_NAME_PRIMARY_RASTER_FILES)
                obj.target_primary_tile_data_path = os.path.join(obj.target_raster_files_path, obj.folder_name_tile_data)

                obj.target_albedo_cloud_masked_path = os.path.join(obj.target_primary_tile_data_path,
                                                           obj.albedo_cloud_masked_tif_filename)
                obj.target_dem_path = os.path.join(obj.target_primary_tile_data_path, obj.dem_tif_filename)
                obj.target_dsm_path = os.path.join(obj.target_primary_tile_data_path, obj.dsm_tif_filename)
                obj.target_lulc_path = os.path.join(obj.target_primary_tile_data_path, obj.lulc_tif_filename)
                obj.target_open_urban_path = os.path.join(obj.target_primary_tile_data_path, obj.open_urban_tif_filename)
                obj.target_tree_canopy_path = os.path.join(obj.target_primary_tile_data_path,
                                                           obj.tree_canopy_tif_filename)

                obj.target_intermediate_tile_data_path = os.path.join(obj.target_intermediate_data_path, obj.folder_name_tile_data)
                obj.target_wallheight_path = os.path.join(obj.target_intermediate_tile_data_path, obj.wall_height_filename)
                obj.target_wallaspect_path = os.path.join(obj.target_intermediate_tile_data_path, obj.wall_aspect_filename)
                obj.target_svfszip_path = os.path.join(obj.target_intermediate_tile_data_path, obj.skyview_factor_filename)

        return obj

def _get_grid_crs(city_geoextent, min_lon, min_lat, max_lon, max_lat, source_aoi_crs):
    grid_crs = None
    if city_geoextent is not None and city_geoextent.geo_type == GeoType.CITY:
        grid_crs = city_geoextent.crs
    elif min_lon is not None:
        if source_aoi_crs is not None and get_projection_type(source_aoi_crs) == ProjectionType.UTM:
            grid_crs = source_aoi_crs
        elif source_aoi_crs is not None and min_lon is not None:
            center_lon = (min_lon + max_lon) / 2
            center_lat = (min_lat + max_lat) / 2
            centroid = Point(center_lon, center_lat)
            grid_crs = get_utm_zone_from_latlon_point(centroid)

    return grid_crs

def get_yml_content(source_base_path, folder_name_city_data):
    source_city_path = str(os.path.join(source_base_path, folder_name_city_data))
    city_method_config_path = os.path.join(source_city_path, FILENAME_METHOD_YML_CONFIG)

    # parse config file
    yml_values = read_yaml(city_method_config_path)
    return yml_values

# Logic for function: https://gfw.atlassian.net/browse/CIF-317 and https://gfw.atlassian.net/browse/CDB-300
def _get_latitude_based_leaf_start_end(center_lat, northern_leaf_start, northern_leaf_end,
                                       southern_leaf_start, southern_leaf_end):
    tropical_latitude = 23.5
    if abs(center_lat) <= tropical_latitude:  # tropical zone
        leaf_start = 0
        leaf_end = 366
    elif center_lat > tropical_latitude:
        leaf_start = northern_leaf_start
        leaf_end = northern_leaf_end
    else:
        leaf_start = southern_leaf_start
        leaf_end = southern_leaf_end

    return leaf_start, leaf_end


def _time_shift_utc_offset(seasonal_utc_offset):
    # Adjust seasonal_utc_offset that are not on a whole hour. This adjustment uses the same time shifting used
    # in CIF era_5_hottest_day layer class
    if seasonal_utc_offset - int(seasonal_utc_offset) != 0:
        hour_fraction = seasonal_utc_offset - int(seasonal_utc_offset)
        if hour_fraction <= 0.5:
            seasonal_utc_offset = int(seasonal_utc_offset)
        else:
            seasonal_utc_offset = int(seasonal_utc_offset) + 1

    return seasonal_utc_offset
