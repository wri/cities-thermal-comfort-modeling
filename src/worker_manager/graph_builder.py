import geopandas as gpd
from city_metrix.constants import ProjectionType
from city_metrix.metrix_model import GeoExtent, create_fishnet_grid
from city_metrix.metrix_tools import get_utm_zone_from_latlon_point, reproject_units
from shapely.geometry.point import Point
from src.constants import WGS_CRS
from src.worker_manager.tools import construct_polygon_from_bounds, coordinates_to_bbox

def get_aoi_from_config(non_tiled_city_data):
    # AOI metrics
    utc_offset = non_tiled_city_data.utc_offset
    min_lon = non_tiled_city_data.min_lon
    min_lat = non_tiled_city_data.min_lat
    max_lon = non_tiled_city_data.max_lon
    max_lat = non_tiled_city_data.max_lat
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer_meters = non_tiled_city_data.tile_buffer_meters

    aoi_boundary = coordinates_to_bbox(min_lon, min_lat, max_lon, max_lat)

    crs = WGS_CRS

    return aoi_boundary, tile_side_meters, tile_buffer_meters, utc_offset, crs


def get_aoi_fishnet(aoi_boundary, tile_side_meters, tile_buffer_meters, in_crs):
    in_minx, in_miny, in_maxx, in_maxy = aoi_boundary.bounds

    if in_crs == WGS_CRS:
        midx = (in_minx + in_maxx) / 2
        midy = (in_miny + in_maxy) / 2
        utm_crs = get_utm_zone_from_latlon_point(Point(midx, midy))

        from shapely.geometry import box, Polygon
        from pyproj import Transformer

        boundary_polygon = box(*aoi_boundary.bounds)
        transformer = Transformer.from_crs(WGS_CRS, utm_crs, always_xy=True)
        utm_coords = [transformer.transform(x, y) for x, y in boundary_polygon.exterior.coords]
        utm_polygon = Polygon(utm_coords)

        minx, miny, maxx, maxy = utm_polygon.bounds
    else:
        minx = in_minx
        miny = in_miny
        maxx = in_maxx
        maxy = in_maxy
        utm_crs = in_crs

    if tile_side_meters is None:
        bbox_poly = construct_polygon_from_bounds(minx, miny, maxx, maxy)
        unbuffered_tile_gpd = gpd.GeoDataFrame(index=[0], crs=utm_crs, geometry=[bbox_poly])
    else:
        bbox = GeoExtent((minx, miny, maxx, maxy), utm_crs)
        unbuffered_tile_gpd = create_fishnet_grid(bbox, tile_side_meters, 0, length_units="meters",
                                                  output_as=ProjectionType.UTM)

    if tile_side_meters is None:
        buffered_minx = minx - tile_buffer_meters
        buffered_miny = miny - tile_buffer_meters
        buffered_maxx = maxx + tile_buffer_meters
        buffered_maxy = maxy + tile_buffer_meters
        bbox_poly = construct_polygon_from_bounds(buffered_minx, buffered_miny, buffered_maxx, buffered_maxy)
        tile_gpd = gpd.GeoDataFrame(index=[0], crs=utm_crs, geometry=[bbox_poly])
    else:
        bbox = GeoExtent((minx, miny, maxx, maxy), utm_crs)
        tile_gpd = create_fishnet_grid(bbox, tile_side_meters, tile_buffer_meters, length_units="meters",
                                       output_as=ProjectionType.UTM)

    return tile_gpd, unbuffered_tile_gpd
