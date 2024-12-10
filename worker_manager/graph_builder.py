import math
import os
import pandas as pd

from src.src_tools import coordinates_to_bbox
from worker_manager.tools import construct_polygon_from_bounds
from workers.city_data import parse_processing_areas_config, CityData, parse_filenames_config

def _build_source_dataframes(source_base_path, city_folder_name):
    config_processing_file_path = str(os.path.join(source_base_path, city_folder_name, CityData.filename_umep_city_processing_config))
    processing_config_df = pd.read_csv(config_processing_file_path)

    return processing_config_df


def get_aoi(source_base_path, city_folder_name):
    from city_metrix.layers.layer import get_utm_zone_epsg

    source_city_path = str(os.path.join(source_base_path, city_folder_name))

    utc_offset, min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters = \
        parse_processing_areas_config(source_city_path, CityData.filename_method_parameters_config)

    aoi_boundary = coordinates_to_bbox(min_lon, min_lat, max_lon, max_lat)

    crs_str = get_utm_zone_epsg(aoi_boundary.bounds)

    return aoi_boundary, tile_side_meters, tile_buffer_meters, utc_offset, crs_str


def get_aoi_fishnet(aoi_boundary, tile_side_meters, tile_buffer_meters):
    bounds = aoi_boundary.bounds

    min_lon = bounds[0]
    min_lat = bounds[1]
    max_lon = bounds[2]
    max_lat = bounds[3]

    if tile_buffer_meters is None:
        tile_buffer_meters = 0

    if tile_side_meters is None:
        import geopandas as gpd
        bbox_poly = construct_polygon_from_bounds(min_lon, min_lat, max_lon, max_lat)
        geom_gpd = gpd.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[bbox_poly])

    else:
        from city_metrix.layers.layer import create_fishnet_grid
        geom_gpd = create_fishnet_grid(min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters,
                                      tile_units_in_degrees=False)

    return geom_gpd


# def _get_distance_between_points(lon1, lat1, lon2, lat2):
#     # Convert decimal degrees to radians
#     lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
#
#     # Haversine formula
#     dlon = lon2 - lon1
#     dlat = lat2 - lat1
#     a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
#     c = 2 * math.asin(math.sqrt(a))
#
#     # Global average radius of Earth in kilometers.
#     r = 6371000
#
#     # Calculate the result
#     return c * r

def get_cif_features(source_city_path):
    (dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_custom_features,
     custom_feature_list, cif_feature_list) = parse_filenames_config(source_city_path, CityData.filename_method_parameters_config)

    custom_file_names = []
    if 'dem' in custom_feature_list:
        custom_file_names.append(dem_tif_filename)
    if 'dsm' in custom_feature_list:
        custom_file_names.append(dsm_tif_filename)
    if 'tree_canopy' in custom_feature_list:
        custom_file_names.append(tree_canopy_tif_filename)
    if 'lulc' in custom_feature_list:
        custom_file_names.append(lulc_tif_filename)

    cif_features = ','.join(cif_feature_list)
    return custom_file_names, has_custom_features, cif_features

