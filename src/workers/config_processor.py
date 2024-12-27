import os

from attr.converters import to_bool
from src.constants import FILENAME_METHOD_CONFIG
from src.workers.worker_tools import read_yaml, unpack_quoted_value


def parse_method_attributes_conf(source_city_path):
    city_configs = os.path.join(source_city_path, FILENAME_METHOD_CONFIG)
    try:
        values = read_yaml(city_configs)
        method_attributes = values[0]
        wall_lower_limit_height = method_attributes['wall_height_aspect']['lower_limit_for_wall_height']
        light_transmissivity = method_attributes['skyview_factor'][
            'transmissivity_of_light_through_vegetation']
        trunk_zone_height = method_attributes['skyview_factor']['trunk_zone_height']
        leaf_start = method_attributes['solweig']['leaf_start']
        leaf_end = method_attributes['solweig']['leaf_end']
        conifer_trees = to_bool(method_attributes['solweig']['conifer_trees'])
        albedo_walls = method_attributes['solweig']['albedo_walls']
        albedo_ground = method_attributes['solweig']['albedo_ground']
        emis_walls = method_attributes['solweig']['emis_walls']
        emis_ground = method_attributes['solweig']['emis_ground']
        output_tmrt = to_bool(method_attributes['solweig']['output_tmrt'])
        output_sh = to_bool(method_attributes['solweig']['output_sh'])
        sampling_local_hours = method_attributes['solweig']['sampling_local_hours']

        met_filenames = values[1].get('MetFiles')

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_CONFIG} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

    return (wall_lower_limit_height, light_transmissivity, trunk_zone_height, leaf_start, leaf_end, conifer_trees, albedo_walls,
            albedo_ground, emis_walls, emis_ground, output_tmrt, output_sh, sampling_local_hours, met_filenames)


def parse_filenames_config(source_city_path):
    city_configs = os.path.join(source_city_path, FILENAME_METHOD_CONFIG)
    template_name_cif_dem = 'cif_dem.tif'
    template_name_cif_dsm = 'cif_dsm_ground_build.tif'
    template_name_cif_tree_canopy = 'cif_tree_canopy.tif'
    template_name_cif_lulc = 'cif_lulc.tif'
    template_name_cif_era5 = '?????'
    try:
        custom_feature_list = []
        cif_feature_list = []

        values = read_yaml(city_configs)
        filenames = values[2]
        dem_tif_filename = unpack_quoted_value(filenames['dem_tif_filename'])
        if dem_tif_filename is None:
            dem_tif_filename = template_name_cif_dem
            cif_feature_list.append('dem')
        else:
            custom_feature_list.append('dem')

        dsm_tif_filename = unpack_quoted_value(filenames['dsm_tif_filename'])
        if dsm_tif_filename is None:
            dsm_tif_filename = template_name_cif_dsm
            cif_feature_list.append('dsm')
        else:
            custom_feature_list.append('dsm')

        tree_canopy_tif_filename = unpack_quoted_value(filenames['tree_canopy_tif_filename'])
        if tree_canopy_tif_filename is None:
            tree_canopy_tif_filename = template_name_cif_tree_canopy
            cif_feature_list.append('tree_canopy')
        else:
            custom_feature_list.append('tree_canopy')

        lulc_tif_filename = unpack_quoted_value(filenames['lulc_tif_filename'])
        if lulc_tif_filename is None:
            lulc_tif_filename = template_name_cif_lulc
            cif_feature_list.append('lulc')
        else:
            custom_feature_list.append('lulc')

        has_custom_features = True if len(cif_feature_list) < 4 else False
    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_CONFIG} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

    return (dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_custom_features,
            custom_feature_list, cif_feature_list)


def parse_processing_areas_config(source_city_path):
    city_configs = os.path.join(source_city_path, FILENAME_METHOD_CONFIG)
    try:
        values = read_yaml(city_configs)

        processing_area = values[3]
        utc_offset = unpack_quoted_value(processing_area['utc_offset'])
        min_lon = unpack_quoted_value(processing_area['min_lon'])
        min_lat = unpack_quoted_value(processing_area['min_lat'])
        max_lon = unpack_quoted_value(processing_area['max_lon'])
        max_lat = unpack_quoted_value(processing_area['max_lat'])
        tile_side_meters = unpack_quoted_value(processing_area['tile_side_meters'])
        tile_buffer_meters = unpack_quoted_value(processing_area['tile_buffer_meters'])

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_CONFIG} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

    return utc_offset, min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters
