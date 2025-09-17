import json

from attr.converters import to_bool
from src.constants import FILENAME_METHOD_YML_CONFIG, VALID_PRIMARY_TYPES, METHOD_TRIGGER_ERA5_DOWNLOAD, \
    FILENAME_ERA5_UMEP, FILENAME_ERA5_UPENN
from src.workers.worker_tools import read_yaml, unpack_quoted_value, any_value_matches_in_dict_list


def parse_scenario_config(yml_values):
    try:
        scenario = yml_values[0]
        
        scenario_name = str(unpack_quoted_value(scenario['scenario_name'])).lower()
        infra_id = str(unpack_quoted_value(scenario['infra_id'])).lower()
        description = unpack_quoted_value(scenario['description'])
        author = unpack_quoted_value(scenario['author'])
        publishing_target = unpack_quoted_value(scenario['publishing_target']).lower()
    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return scenario_name, infra_id, description, author, publishing_target


def parse_processing_areas_config(yml_values):
    try:
        processing_area = yml_values[1]
        
        seasonal_utc_offset = unpack_quoted_value(processing_area['seasonal_utc_offset'])
        city = processing_area['city']
        city_json_str = None if city == 'None' else json.dumps(city)
        aoi_bounds = unpack_quoted_value(processing_area['aoi_bounds'])
        aoi_epsg = unpack_quoted_value(aoi_bounds['epsg_code'])
        source_aoi_crs = f"EPSG:{aoi_epsg}" if aoi_epsg is not None else None
        min_lon = unpack_quoted_value(aoi_bounds['west'])
        min_lat = unpack_quoted_value(aoi_bounds['south'])
        max_lon = unpack_quoted_value(aoi_bounds['east'])
        max_lat = unpack_quoted_value(aoi_bounds['north'])
        tile_side_meters = unpack_quoted_value(processing_area['tile_side_meters'])
        tile_buffer_meters = unpack_quoted_value(processing_area['tile_buffer_meters'])
        remove_mrt_buffer_for_final_output = unpack_quoted_value(processing_area['remove_mrt_buffer_for_final_output'])

        if not isinstance(remove_mrt_buffer_for_final_output, bool):
            raise Exception("Wrong data type for remove_mrt_buffer_for_final_output parameter.")

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return (seasonal_utc_offset, city_json_str, source_aoi_crs, min_lon, min_lat, max_lon, max_lat,
            tile_side_meters, tile_buffer_meters, remove_mrt_buffer_for_final_output)

def parse_met_files_config(yml_values, new_task_method):
    import fnmatch
    try:
        met_filenames = yml_values[2].get('MetFiles')

        result_met_filenames = []
        has_era_met_download = False
        era5_date_range = None
        for item in met_filenames:
            filename = list(item.values())[0]
            if fnmatch.fnmatch(filename, METHOD_TRIGGER_ERA5_DOWNLOAD):
                has_era_met_download = True
                era5_date_range = _extract_wildcard_match(filename, METHOD_TRIGGER_ERA5_DOWNLOAD)
                if new_task_method == 'upenn_model':
                    result_met_filenames.append(FILENAME_ERA5_UPENN)
                else:
                    result_met_filenames.append(FILENAME_ERA5_UMEP)
            else:
                result_met_filenames.append(filename)

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return result_met_filenames, has_era_met_download, era5_date_range

def _extract_wildcard_match(text, pattern):
    import re
    # Escape all regex characters except the wildcard
    regex_pattern = re.escape(pattern).replace(r'\*', '(.*?)')
    match = re.match(regex_pattern, text)
    if match:
        return match.group(1)  # Group 1 corresponds to the wildcard
    return None

def parse_primary_filenames_config(yml_values):
    try:
        custom_features = []
        custom_primary_filenames = []
        cif_features = []

        dem_tif_filename, this_cif_feature_list, this_custom_feature_list, this_custom_primary_filenames =\
            _assign_primary_type_variables('dem', yml_values)
        cif_features.extend(this_cif_feature_list)
        custom_features.extend(this_custom_feature_list)
        custom_primary_filenames.extend(this_custom_primary_filenames)

        dsm_tif_filename, this_cif_feature_list, this_custom_feature_list, this_custom_primary_filenames =\
            _assign_primary_type_variables('dsm', yml_values)
        cif_features.extend(this_cif_feature_list)
        custom_features.extend(this_custom_feature_list)
        custom_primary_filenames.extend(this_custom_primary_filenames)

        lulc_tif_filename, this_cif_feature_list, this_custom_feature_list, this_custom_primary_filenames =\
            _assign_primary_type_variables('lulc', yml_values)
        cif_features.extend(this_cif_feature_list)
        custom_features.extend(this_custom_feature_list)
        custom_primary_filenames.extend(this_custom_primary_filenames)

        open_urban_tif_filename, this_cif_feature_list, this_custom_feature_list, this_custom_primary_filenames =\
            _assign_primary_type_variables('open_urban', yml_values)
        cif_features.extend(this_cif_feature_list)
        custom_features.extend(this_custom_feature_list)
        custom_primary_filenames.extend(this_custom_primary_filenames)

        tree_canopy_tif_filename, this_cif_feature_list, this_custom_feature_list, this_custom_primary_filenames =\
            _assign_primary_type_variables('tree_canopy', yml_values)
        cif_features.extend(this_cif_feature_list)
        custom_features.extend(this_custom_feature_list)
        custom_primary_filenames.extend(this_custom_primary_filenames)

        albedo_cloud_masked_tif_filename, this_cif_feature_list, this_custom_feature_list, this_custom_primary_filenames =\
            _assign_primary_type_variables('albedo_cloud_masked', yml_values)
        cif_features.extend(this_cif_feature_list)
        custom_features.extend(this_custom_feature_list)
        custom_primary_filenames.extend(this_custom_primary_filenames)

    except Exception as e_msg:
        raise Exception(
            f'The {FILENAME_METHOD_YML_CONFIG} file not found or improperly defined in {FILENAME_METHOD_YML_CONFIG} file. (Error: {e_msg})')

    return (dem_tif_filename, dsm_tif_filename, lulc_tif_filename, open_urban_tif_filename, tree_canopy_tif_filename,
            albedo_cloud_masked_tif_filename, custom_features, custom_primary_filenames, cif_features)


def _assign_primary_type_variables(primary_type_short_name, yml_values):
    this_custom_features = []
    this_custom_primary_filenames = []
    this_cif_features = []
    filenames = yml_values[3]

    type_dict = _find_dict_in_list(VALID_PRIMARY_TYPES, 'short_name', primary_type_short_name)
    yml_tif_filename = unpack_quoted_value(filenames[type_dict['yml_tag']])
    if yml_tif_filename is None:
        yml_tif_filename = type_dict['cif_template_name']
        this_cif_features.append(primary_type_short_name)
    else:
        this_custom_features.append(primary_type_short_name)
        this_custom_primary_filenames.append(yml_tif_filename)

    return yml_tif_filename, this_cif_features, this_custom_features, this_custom_primary_filenames


def _find_dict_in_list(dict_list, key, value):
    for dictionary in dict_list:
        if dictionary.get(key) == value:
            return dictionary
    return None


def parse_intermediate_filenames_config(yml_values, method):
    template_name_wall_aspect_filename = 'ctcm_wallaspect.tif'
    template_name_wall_height_filename = 'ctcm_wallheight.tif'
    if method == 'upenn_model':
        template_name_skyview_factor_zip_filename = 'ctcm_svfs'
    else:
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

        new_task_method = method_attributes['method']
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

    return (new_task_method, northern_leaf_start, northern_leaf_end, southern_leaf_start, southern_leaf_end, wall_lower_limit_height,
            light_transmissivity, trunk_zone_height, conifer_trees, albedo_walls,
            albedo_ground, emis_walls, emis_ground, output_tmrt, output_sh, sampling_local_hours)
