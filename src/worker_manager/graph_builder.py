import geopandas as gpd
from city_metrix.constants import ProjectionType
from city_metrix.metrix_model import GeoExtent, create_fishnet_grid
from city_metrix.metrix_tools import get_utm_zone_from_latlon_point, reproject_units
from shapely.geometry.point import Point
from src.constants import WGS_CRS
from src.worker_manager.ancillary_files import add_tile_name_column
from src.worker_manager.tools import construct_polygon_from_bounds, coordinates_to_bbox

def get_grid_dimensions(non_tiled_city_data):
    # AOI metrics
    seasonal_utc_offset = non_tiled_city_data.seasonal_utc_offset
    utm_grid_west = non_tiled_city_data.utm_grid_west
    utm_grid_south = non_tiled_city_data.utm_grid_south
    utm_grid_east = non_tiled_city_data.utm_grid_east
    utm_grid_north = non_tiled_city_data.utm_grid_north
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer_meters = non_tiled_city_data.tile_buffer_meters
    grid_crs = non_tiled_city_data.grid_crs

    aoi_boundary = coordinates_to_bbox(utm_grid_west, utm_grid_south, utm_grid_east, utm_grid_north)

    return aoi_boundary, tile_side_meters, tile_buffer_meters, seasonal_utc_offset, grid_crs


def get_aoi_fishnet(aoi_boundary, tile_side_meters, tile_buffer_meters, utm_grid_crs):
    minx, miny, maxx, maxy = aoi_boundary.bounds

    if tile_side_meters is None:
        bbox_poly = construct_polygon_from_bounds(minx, miny, maxx, maxy)
        unbuffered_tile_gpd = gpd.GeoDataFrame(index=[0], crs=utm_grid_crs, geometry=[bbox_poly])
    else:
        bbox = GeoExtent((minx, miny, maxx, maxy), utm_grid_crs)
        unbuffered_tile_gpd = create_fishnet_grid(bbox, tile_side_meters, 0, length_units="meters",
                                                  output_as=ProjectionType.UTM)

    if tile_side_meters is None:
        buffered_minx = minx - tile_buffer_meters
        buffered_miny = miny - tile_buffer_meters
        buffered_maxx = maxx + tile_buffer_meters
        buffered_maxy = maxy + tile_buffer_meters
        bbox_poly = construct_polygon_from_bounds(buffered_minx, buffered_miny, buffered_maxx, buffered_maxy)
        tile_gpd = gpd.GeoDataFrame(index=[0], crs=utm_grid_crs, geometry=[bbox_poly])
    else:
        bbox = GeoExtent((minx, miny, maxx, maxy), utm_grid_crs)
        tile_gpd = create_fishnet_grid(bbox, tile_side_meters, tile_buffer_meters, length_units="meters",
                                       output_as=ProjectionType.UTM)

    tile_gpd = add_tile_name_column(tile_gpd)
    unbuffered_tile_gpd = add_tile_name_column(unbuffered_tile_gpd)
    return tile_gpd, unbuffered_tile_gpd
