import geopandas as gpd
import shapely
import numbers
import math

from city_metrix.layers.layer_geometry import reproject_units
from city_metrix.layers.layer_tools import get_haversine_distance
from shapely.geometry import box
from src.constants import FILENAME_METHOD_YML_CONFIG, WGS_CRS


def evaluate_aoi(non_tiled_city_data, existing_tiles_metrics, processing_option):
    combined_invalids = []

    aoi_primary_invalids = evaluate_aoi_primary_configs(non_tiled_city_data)
    combined_invalids.extend(aoi_primary_invalids)

    tiling_invalids = evaluate_aoi_configs_for_tiling(non_tiled_city_data)
    combined_invalids.extend(tiling_invalids)

    aoi_invalids, updated_aoi = evaluate_aoi_discrepancy(non_tiled_city_data, existing_tiles_metrics, processing_option)
    combined_invalids.extend(aoi_invalids)

    return combined_invalids, updated_aoi


def evaluate_aoi_primary_configs(non_tiled_city_data):
    invalids = []

    # AOI metrics
    utc_offset = non_tiled_city_data.utc_offset
    aoi_min_lon, aoi_min_lat, aoi_max_lon, aoi_max_lat = _parse_aoi_dimensions(non_tiled_city_data)

    # Evaluate AOI
    if (not isinstance(aoi_min_lon, numbers.Number) or not isinstance(aoi_min_lat, numbers.Number) or
            not isinstance(aoi_max_lon, numbers.Number) or not isinstance(aoi_max_lat, numbers.Number)):
        msg = f'Parameters in NewProcessingAOI section must be defined in {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    if not (-180 <= aoi_min_lon <= 180) or not (-180 <= aoi_max_lon <= 180):
        msg = f'Min and max longitude values must be between -180 and 180 in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    if not (-90 <= aoi_min_lat <= 90) or not (-90 <= aoi_max_lat <= 90):
        msg = f'Min and max latitude values must be between -90 and 90 in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    if not (aoi_min_lon <= aoi_max_lon):
        msg = f'Min longitude must be less than max longitude in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    if not (aoi_min_lat <= aoi_max_lat):
        msg = f'Min latitude must be less than max latitude in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    # TODO improve this evaluation
    if abs(aoi_max_lon - aoi_min_lon) > 0.3 or abs(aoi_max_lon - aoi_min_lon) > 0.3:
        msg = f'Specified AOI must be less than 30km on a side in ProcessingAOI section of {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    return invalids


def evaluate_aoi_configs_for_tiling(non_tiled_city_data):
    custom_primary_features = non_tiled_city_data.custom_primary_feature_list
    tile_side_meters = non_tiled_city_data.tile_side_meters
    tile_buffer_meters = non_tiled_city_data.tile_buffer_meters
    aoi_min_lon, aoi_min_lat, aoi_max_lon, aoi_max_lat = _parse_aoi_dimensions(non_tiled_city_data)

    invalids = []
    if not custom_primary_features:
        if tile_side_meters is not None:
            is_tile_wider_than_half = _is_tile_wider_than_half_aoi_side(aoi_min_lat, aoi_min_lon, aoi_max_lat,
                                                                        aoi_max_lon, tile_side_meters)
            if is_tile_wider_than_half:
                msg = (f"Requested tile_side_meters cannot be larger than half the AOI side length in {FILENAME_METHOD_YML_CONFIG}."
                       f" Specify None if you don't want to subdivide the aoi.")
                invalids.append(msg)

        if tile_side_meters is not None and tile_side_meters < 150:
            msg = (f"Requested tile_side_meters must be 100 meters or more in {FILENAME_METHOD_YML_CONFIG}. "
                   f"Specify None if you don't want to subdivide the aoi.")
            invalids.append((msg, True))

        if tile_side_meters is not None and int(tile_side_meters) <= 10:
            msg = (f"tile_side_meters must be greater than 10 in {FILENAME_METHOD_YML_CONFIG}. "
                   f"Specify None if you don't want to subdivide the aoi.")
            invalids.append((msg, True))

        if tile_buffer_meters is not None and int(tile_buffer_meters) <= 10:
            msg = (f"tile_buffer_meters must be greater than 10 in {FILENAME_METHOD_YML_CONFIG}. "
                   f"Specify None if you don't want to subdivide the aoi.")
            invalids.append((msg, True))

        if tile_buffer_meters is not None and int(tile_buffer_meters) > 500:
            msg = (f"tile_buffer_meters must be less than 500 in {FILENAME_METHOD_YML_CONFIG}. S"
                   f"pecify None if you don't want to subdivide the aoi.")
            invalids.append((msg, True))

        if tile_side_meters is None and tile_buffer_meters is not None:
            msg = f"tile_buffer_meters must be None if tile_sider_meters is None in {FILENAME_METHOD_YML_CONFIG}."
            invalids.append((msg, True))

    return invalids


def evaluate_aoi_discrepancy(non_tiled_city_data, existing_tiles_metrics, processing_option):
    invalids = []
    updated_aoi = None

    if not non_tiled_city_data.custom_primary_feature_list:
        return invalids, updated_aoi

    max_tolerance_meters = 100

    aoi_min_lon, aoi_min_lat, aoi_max_lon, aoi_max_lat = _parse_aoi_dimensions(non_tiled_city_data)

    tile_grid_total_bounds = gpd.GeoSeries(box(*existing_tiles_metrics.total_bounds))
    tile_grid_min_lon = round(tile_grid_total_bounds.geometry.bounds.minx[0],7)
    tile_grid_min_lat = round(tile_grid_total_bounds.geometry.bounds.miny[0],7)
    tile_grid_max_lon = round(tile_grid_total_bounds.geometry.bounds.maxx[0],7)
    tile_grid_max_lat = round(tile_grid_total_bounds.geometry.bounds.maxy[0],7)

    existing_tiles_crs = existing_tiles_metrics.crs.srs
    if existing_tiles_crs != WGS_CRS:
        reproj_bbox = reproject_units(tile_grid_min_lon, tile_grid_min_lat,
                                      tile_grid_max_lon, tile_grid_max_lat, existing_tiles_crs, WGS_CRS)
        tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat = reproj_bbox

    sw_distance = get_haversine_distance(aoi_min_lon, aoi_min_lat, tile_grid_min_lon, tile_grid_min_lat)
    ne_distance = get_haversine_distance(aoi_max_lon, aoi_max_lat, tile_grid_max_lon, tile_grid_max_lat)

    updated_aoi = None
    if sw_distance > 0 or ne_distance > 0:
        max_offset = sw_distance if sw_distance >= ne_distance else ne_distance
        max_offset = round(max_offset, 2)

        aoi_notice = _construct_aoi_update_notice(tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat)
        if processing_option == 'pre_check':
            msg = (f'WARNING: Tile-grid extent differs from AOI specification in yml file. Investigate the discrepancy '
                   f'and consider updating the yml with: {aoi_notice}'
                   )
            invalids.append([msg, False])
        else:
            if 0 < max_offset < max_tolerance_meters:
                # Notify user of offset and automatically update target yml
                msg = (f'WARNING: Tile-grid extent marginally differs from AOI specification in yml file with '
                       f'corner offset of at least {max_offset} meters. Automatically updating the target yml file to the tile-grid extent. '
                       f'Use the updated coordinates to avoid this warning in the future.')
                invalids.append((msg, False))
                updated_aoi = shapely.box(tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat)
            else:
                # Stop processing and notify user
                msg = (f'Tile-grid extent substantially differs from AOI specification in yml file by maximum '
                       f'corner offset of at least {max_offset} meters. Investigate the discrepancy and consider '
                       f'updating the yml with: {aoi_notice}'
                       )
                invalids.append((msg, True))

    return invalids, updated_aoi

def _construct_aoi_update_notice(tile_grid_min_lon, tile_grid_min_lat, tile_grid_max_lon, tile_grid_max_lat):
    aoi_notice = (f'\n  min_lon: {tile_grid_min_lon}\n  min_lat: {tile_grid_min_lat}'
                  f'\n  max_lon: {tile_grid_max_lon}\n  max_lat: {tile_grid_max_lat}')
    return aoi_notice

def _is_tile_wider_than_half_aoi_side(min_lat, min_lon, max_lat, max_lon, tile_side_meters):
    center_lat = (min_lat + max_lat) / 2
    lon_degree_offset, lat_degree_offset = _offset_meters_to_geographic_degrees(center_lat, tile_side_meters)

    is_tile_wider_than_half = False
    if (lon_degree_offset > (max_lon - min_lon)/2) or (lat_degree_offset > (max_lat - min_lat)/2):
        is_tile_wider_than_half = True

    return is_tile_wider_than_half

def _offset_meters_to_geographic_degrees(decimal_latitude, length_m):
    earth_radius_m = 6378137
    rad = 180/math.pi

    lon_degree_offset = abs((length_m / (earth_radius_m * math.cos(math.pi*decimal_latitude/180))) * rad)
    lat_degree_offset = abs((length_m / earth_radius_m) * rad)

    return lon_degree_offset, lat_degree_offset

def _parse_aoi_dimensions(non_tiled_city_data):
    aoi_min_lon = non_tiled_city_data.min_lon
    aoi_min_lat = non_tiled_city_data.min_lat
    aoi_max_lon = non_tiled_city_data.max_lon
    aoi_max_lat = non_tiled_city_data.max_lat
    return aoi_min_lon, aoi_min_lat, aoi_max_lon, aoi_max_lat

