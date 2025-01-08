import os

from src.worker_manager.tools import construct_polygon_from_bounds, coordinates_to_bbox
from src.workers.city_data import CityData
from src.workers.worker_tools import get_utm_zone_epsg


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
