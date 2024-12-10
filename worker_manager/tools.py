from shapely.geometry import Polygon
from pyproj import Geod

def get_aoi_area_in_square_meters(min_lon, min_lat, max_lon, max_lat):
    bbox_poly = construct_polygon_from_bounds(min_lon, min_lat, max_lon, max_lat)

    geod = Geod(ellps="WGS84")
    
    # Calculate the area in square meters
    area, _ = geod.geometry_area_perimeter(bbox_poly)

    return abs(area)


def construct_polygon_from_bounds(min_lon, min_lat, max_lon, max_lat):
    bbox = [(min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat), (max_lon, min_lat)]
    bbox_poly = Polygon(bbox)
    return bbox_poly