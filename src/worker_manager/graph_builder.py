import os
import pandas as pd

from src.constants import FILENAME_PROCESSING_CONFIG
from src.worker_manager.tools import construct_polygon_from_bounds, coordinates_to_bbox
from src.workers.city_data import CityData
from src.workers.worker_tools import get_utm_zone_epsg


def build_source_dataframes(source_base_path, city_folder_name):
    config_processing_file_path = str(os.path.join(source_base_path, city_folder_name, FILENAME_PROCESSING_CONFIG))
    processing_config_df = pd.read_csv(config_processing_file_path)

    return processing_config_df


def get_aoi(source_base_path, city_folder_name):
    source_city_path = str(os.path.join(source_base_path, city_folder_name))

    non_tiled_city_data = CityData(city_folder_name, None, source_base_path, None)

    # AOI metrics
    utc_offset = non_tiled_city_data.utc_offset
    min_lon = non_tiled_city_data.min_lon
    min_lat = non_tiled_city_data.min_lat
    max_lon = non_tiled_city_data.max_lon
    max_lat = non_tiled_city_data.max_lat
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer_meters = non_tiled_city_data.tile_buffer_meters

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
