from city_metrix.layers.layer_geometry import get_utm_zone_from_latlon_point, reproject_units, GeoExtent, create_fishnet_grid
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
        reproj_bbox = reproject_units(in_miny, in_minx, in_maxy, in_maxx, WGS_CRS, utm_crs)
        miny, minx, maxy, maxx = reproj_bbox
    else:
        minx = in_minx
        miny = in_miny
        maxx = in_maxx
        maxy = in_maxy
        utm_crs = in_crs

    if tile_buffer_meters is None:
        tile_buffer_meters = 0
        unbuffered_tile_gpd = None
    else:
        bbox = GeoExtent((minx, miny, maxx, maxy), utm_crs)
        unbuffered_tile_gpd = create_fishnet_grid(bbox, tile_side_meters, 0, length_units="meters", output_as='utm')

    if tile_side_meters is None:
        import geopandas as gpd
        bbox_poly = construct_polygon_from_bounds(minx, miny, maxx, maxy)
        tile_gpd = gpd.GeoDataFrame(index=[0], crs=utm_crs, geometry=[bbox_poly])
    else:
        bbox = GeoExtent((minx, miny, maxx, maxy), utm_crs)
        tile_gpd = create_fishnet_grid(bbox, tile_side_meters, tile_buffer_meters, length_units="meters", output_as='utm')

    return tile_gpd, unbuffered_tile_gpd
