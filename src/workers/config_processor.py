from attr.converters import to_bool
from src.constants import FILENAME_METHOD_YML_CONFIG
from src.workers.worker_tools import read_yaml, unpack_quoted_value


def parse_scenario_config(yml_values):
    try:
        scenario = yml_values[0]
        
        short_title = str(unpack_quoted_value(scenario['short_title'])).lower()
        version = unpack_quoted_value(scenario['version'])
        description = unpack_quoted_value(scenario['description'])
        author = unpack_quoted_value(scenario['author'])

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return short_title, version, description, author


def parse_processing_areas_config(yml_values):
    try:
        processing_area = yml_values[1]
        
        utc_offset = unpack_quoted_value(processing_area['utc_offset'])
        min_lon = unpack_quoted_value(processing_area['min_lon'])
        min_lat = unpack_quoted_value(processing_area['min_lat'])
        max_lon = unpack_quoted_value(processing_area['max_lon'])
        max_lat = unpack_quoted_value(processing_area['max_lat'])
        tile_side_meters = unpack_quoted_value(processing_area['tile_side_meters'])
        tile_buffer_meters = unpack_quoted_value(processing_area['tile_buffer_meters'])

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return utc_offset, min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters


def parse_met_files_config(yml_values):
    try:
        met_filenames = yml_values[2].get('MetFiles')

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return met_filenames

def parse_primary_filenames_config(yml_values):
    template_name_cif_dem = 'cif_dem.tif'
    template_name_cif_dsm = 'cif_dsm_ground_build.tif'
    template_name_cif_tree_canopy = 'cif_tree_canopy.tif'
    template_name_cif_lulc = 'cif_lulc.tif'
    template_name_cif_era5 = '?????'
    try:
        filenames = yml_values[3]
        
        custom_feature_list = []
        custom_primary_filenames = []
        cif_feature_list = []
        dem_tif_filename = unpack_quoted_value(filenames['dem_tif_filename'])
        if dem_tif_filename is None:
            dem_tif_filename = template_name_cif_dem
            cif_feature_list.append('dem')
        else:
            custom_feature_list.append('dem')
            custom_primary_filenames.append(dem_tif_filename)

        dsm_tif_filename = unpack_quoted_value(filenames['dsm_tif_filename'])
        if dsm_tif_filename is None:
            dsm_tif_filename = template_name_cif_dsm
            cif_feature_list.append('dsm')
        else:
            custom_feature_list.append('dsm')
            custom_primary_filenames.append(dsm_tif_filename)

        tree_canopy_tif_filename = unpack_quoted_value(filenames['tree_canopy_tif_filename'])
        if tree_canopy_tif_filename is None:
            tree_canopy_tif_filename = template_name_cif_tree_canopy
            cif_feature_list.append('tree_canopy')
        else:
            custom_feature_list.append('tree_canopy')
            custom_primary_filenames.append(tree_canopy_tif_filename)

        lulc_tif_filename = unpack_quoted_value(filenames['lulc_tif_filename'])
        if lulc_tif_filename is None:
            lulc_tif_filename = template_name_cif_lulc
            cif_feature_list.append('lulc')
        else:
            custom_feature_list.append('lulc')
            custom_primary_filenames.append(lulc_tif_filename)

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return (dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename,
            custom_feature_list, custom_primary_filenames, cif_feature_list)


def parse_intermediate_filenames_config(yml_values):
    template_name_wall_aspect_filename = 'ctcm_wallaspect.tif'
    template_name_wall_height_filename = 'ctcm_wallheight.tif'
    template_name_skyview_factor_zip_filename = 'ctcm_svfs.zip'
    try:
        filenames = yml_values[4]

        custom_intermediate_list = []
        ctcm_intermediate_list = []
        
        wall_aspect_filename = unpack_quoted_value(filenames['wall_aspect_filename'])
        if wall_aspect_filename is None:
            wall_aspect_filename = template_name_wall_aspect_filename
            ctcm_intermediate_list.append('wallaspect')
        else:
            custom_intermediate_list.append('wallaspect')

        wall_height_filename = unpack_quoted_value(filenames['wall_height_filename'])
        if wall_height_filename is None:
            wall_height_filename = template_name_wall_height_filename
            ctcm_intermediate_list.append('wallheight')
        else:
            custom_intermediate_list.append('wallheight')

        skyview_factor_filename = unpack_quoted_value(filenames['skyview_factor_filename'])
        if skyview_factor_filename is None:
            skyview_factor_filename = template_name_skyview_factor_zip_filename
            ctcm_intermediate_list.append('skyview_factor')
        else:
            custom_intermediate_list.append('skyview_factor')

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return (wall_aspect_filename, wall_height_filename, skyview_factor_filename,
            custom_intermediate_list, ctcm_intermediate_list)


def parse_method_attributes_config(yml_values):
    try:
        method_attributes = yml_values[5]

        northern_leaf_start = method_attributes['solweig']['seasonal_leaf_coverage']['north_temperate_leaf_start']
        northern_leaf_end = method_attributes['solweig']['seasonal_leaf_coverage']['north_temperate_leaf_end']
        southern_leaf_start = method_attributes['solweig']['seasonal_leaf_coverage']['south_temperate_leaf_start']
        southern_leaf_end = method_attributes['solweig']['seasonal_leaf_coverage']['south_temperate_leaf_end']
        wall_lower_limit_height = method_attributes['wall_height_aspect']['lower_limit_for_wall_height']
        light_transmissivity = method_attributes['skyview_factor'][
            'transmissivity_of_light_through_vegetation']
        trunk_zone_height = method_attributes['skyview_factor']['trunk_zone_height']
        conifer_trees = to_bool(method_attributes['solweig']['conifer_trees'])
        albedo_walls = method_attributes['solweig']['albedo_walls']
        albedo_ground = method_attributes['solweig']['albedo_ground']
        emis_walls = method_attributes['solweig']['emis_walls']
        emis_ground = method_attributes['solweig']['emis_ground']
        output_tmrt = to_bool(method_attributes['solweig']['output_tmrt'])
        output_sh = to_bool(method_attributes['solweig']['output_sh'])
        sampling_local_hours = method_attributes['solweig']['sampling_local_hours']

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return (northern_leaf_start, northern_leaf_end, southern_leaf_start, southern_leaf_end, wall_lower_limit_height,
            light_transmissivity, trunk_zone_height, conifer_trees, albedo_walls,
            albedo_ground, emis_walls, emis_ground, output_tmrt, output_sh, sampling_local_hours)
