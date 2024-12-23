import os
import yaml
from attr.converters import to_bool

class CityData:
    filename_method_parameters_config = '.config_method_parameters.yml'
    filename_umep_city_processing_config = '.config_umep_city_processing.csv'

    folder_name_primary_data = 'primary_data'
    folder_name_primary_raster_files = 'raster_files'
    folder_name_primary_met_filenames = 'met_files'
    folder_name_results = 'results_data'
    folder_name_preprocessed_data = 'preprocessed_data'
    folder_name_tcm_results = 'tcm_results'

    filename_wall_height = 'wallheight.tif'
    filename_wall_aspect = 'wallaspect.tif'
    filename_svfs_zip = 'svfs.zip'

    filename_era5 = 'met_era5_hottest_days.txt'
    method_trigger_era5_download = '<download_era5>'

    processing_methods = ['cif_download_only', 'wall_height_aspect', 'skyview_factor', 'solweig_only', 'solweig_full']

    def __new__(cls, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
        obj = super().__new__(cls)

        obj.folder_name_city_data = folder_name_city_data
        obj.folder_name_tile_data = folder_name_tile_data

        obj.source_base_path = source_base_path

        obj.source_city_path = str(os.path.join(source_base_path, folder_name_city_data))
        obj.source_city_data_path = str(os.path.join(obj.source_city_path, cls.folder_name_primary_data))

        city_configs = os.path.join(obj.source_city_path, cls.filename_method_parameters_config)
        try:
            with open(city_configs, 'r') as stream:
                values = list(yaml.safe_load_all(stream))[0]

                method_attributes = values[0]
                obj.wall_lower_limit_height = method_attributes['wall_height_aspect']['lower_limit_for_wall_height']
                obj.light_transmissivity = method_attributes['skyview_factor'][
                    'transmissivity_of_light_through_vegetation']
                obj.trunk_zone_height = method_attributes['skyview_factor']['trunk_zone_height']
                obj.leaf_start = method_attributes['solweig']['leaf_start']
                obj.leaf_end = method_attributes['solweig']['leaf_end']
                obj.conifer_trees = to_bool(method_attributes['solweig']['conifer_trees'])
                obj.albedo_walls = method_attributes['solweig']['albedo_walls']
                obj.albedo_ground = method_attributes['solweig']['albedo_ground']
                obj.emis_walls = method_attributes['solweig']['emis_walls']
                obj.emis_ground = method_attributes['solweig']['emis_ground']
                obj.output_tmrt = to_bool(method_attributes['solweig']['output_tmrt'])
                obj.output_sh = to_bool(method_attributes['solweig']['output_sh'])
                obj.sampling_local_hours = method_attributes['solweig']['sampling_local_hours']

                obj.met_filenames = values[1].get('MetFiles')

        except Exception as e_msg:
            raise Exception(f'The {cls.filename_method_parameters_config} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

        (obj.dem_tif_filename, obj.dsm_tif_filename, obj.tree_canopy_tif_filename, obj.lulc_tif_filename,
         has_custom_features, obj.custom_feature_list, obj.cif_feature_list) = (
            parse_filenames_config(obj.source_city_path, cls.filename_method_parameters_config))

        obj.utc_offset, obj.min_lon, obj.min_lat, obj.max_lon, obj.max_lat, tile_side_meters, tile_buffer_meters = \
            parse_processing_areas_config(obj.source_city_path, cls.filename_method_parameters_config)

        if obj.folder_name_tile_data:
            obj.source_raster_files_path = os.path.join(obj.source_city_data_path, obj.folder_name_primary_raster_files)
            obj.source_primary_raster_tile_data_path = os.path.join(obj.source_raster_files_path, obj.folder_name_tile_data)
            obj.source_dem_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dem_tif_filename)
            obj.source_dsm_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.dsm_tif_filename)
            obj.source_tree_canopy_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.tree_canopy_tif_filename)
            obj.source_land_cover_path = os.path.join(obj.source_primary_raster_tile_data_path, obj.lulc_tif_filename)
        else:
            obj.source_primary_raster_tile_data_path = None
            obj.source_dem_path = None
            obj.source_dsm_path = None
            obj.source_tree_canopy_path = None
            obj.source_land_cover_path = None

        obj.source_met_filenames_path = os.path.join(obj.source_city_data_path, obj.folder_name_primary_met_filenames)

        if target_base_path:
            obj.target_base_path = target_base_path

            obj.target_city_path = str(os.path.join(target_base_path, folder_name_city_data))
            obj.target_city_data_path = str(os.path.join(obj.target_city_path, cls.folder_name_primary_data))

            obj.target_path_city_data = str(os.path.join(obj.target_base_path, folder_name_city_data,
                                                         cls.folder_name_results))
            obj.target_manager_log_path = os.path.join(obj.target_city_path, '.logs', 'worker_manager.log')
            obj.target_preprocessed_path = os.path.join(obj.target_path_city_data, obj.folder_name_preprocessed_data)
            obj.target_tcm_results_path = os.path.join(obj.target_path_city_data, obj.folder_name_tcm_results)

            if obj.folder_name_tile_data:
                # source_primary_raster_tile_data_path
                obj.target_raster_files_path = os.path.join(obj.target_city_data_path,
                                                            obj.folder_name_primary_raster_files)
                obj.target_primary_tile_data_path = os.path.join(obj.target_raster_files_path, obj.folder_name_tile_data)

                obj.target_dem_path = os.path.join(obj.target_primary_tile_data_path, obj.dem_tif_filename)
                obj.target_dsm_path = os.path.join(obj.target_primary_tile_data_path, obj.dsm_tif_filename)
                obj.target_tree_canopy_path = os.path.join(obj.target_primary_tile_data_path,
                                                           obj.tree_canopy_tif_filename)
                obj.target_land_cover_path = os.path.join(obj.target_primary_tile_data_path, obj.lulc_tif_filename)

                obj.target_preprocessed_tile_data_path = os.path.join(obj.target_preprocessed_path, obj.folder_name_tile_data)
                obj.target_wallheight_path = os.path.join(obj.target_preprocessed_tile_data_path, obj.filename_wall_height)
                obj.target_wallaspect_path = os.path.join(obj.target_preprocessed_tile_data_path, obj.filename_wall_aspect)
                obj.target_svfszip_path = os.path.join(obj.target_preprocessed_tile_data_path, obj.filename_svfs_zip)
            else:
                obj.target_preprocessed_tile_data_path = None
                obj.target_dem_path = None
                obj.target_dsm_path = None
                obj.target_tree_canopy_path = None
                obj.target_land_cover_path = None
                obj.target_wallheight_path = None
                obj.target_wallaspect_path = None
                obj.target_svfszip_path = None

            obj.target_met_filenames_path = os.path.join(obj.target_city_data_path, obj.folder_name_primary_met_filenames)

        return obj

def parse_filenames_config(source_city_path, filename_method_parameters_config):
    city_configs = os.path.join(source_city_path, filename_method_parameters_config)
    template_name_cif_dem = 'cif_dem.tif'
    template_name_cif_dsm = 'cif_dsm_ground_build.tif'
    template_name_cif_tree_canopy = 'cif_tree_canopy.tif'
    template_name_cif_lulc = 'cif_lulc.tif'
    template_name_cif_era5 = '?????'
    try:
        with open(city_configs, 'r') as stream:
            values = list(yaml.safe_load_all(stream))[0]

            custom_feature_list = []
            cif_feature_list = []
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
            f'The {filename_method_parameters_config} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

    return (dem_tif_filename, dsm_tif_filename, tree_canopy_tif_filename, lulc_tif_filename, has_custom_features,
            custom_feature_list, cif_feature_list)


def parse_processing_areas_config(source_city_path, filename_method_parameters_config):
    city_configs = os.path.join(source_city_path, filename_method_parameters_config)
    try:
        with open(city_configs, 'r') as stream:
            values = list(yaml.safe_load_all(stream))[0]

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
            f'The {filename_method_parameters_config} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

    return utc_offset, min_lon, min_lat, max_lon, max_lat, tile_side_meters, tile_buffer_meters

def unpack_quoted_value(value):
    return_value = value
    if type(value).__name__ == 'str':
        if value.lower() == 'none':
            return_value = None
        elif value.lower() == 'true':
            return_value = True
        elif value.lower() == 'false':
            return_value = False
        elif value.isnumeric():
            if is_float_or_integer(value) == 'Integer':
                return_value = int(value)
            elif is_float_or_integer(value) == 'Float':
                return_value = float(value)

    return return_value

def is_float_or_integer(s):
    try:
        int(s)
        return "Integer"
    except ValueError:
        try:
            float(s)
            return "Float"
        except ValueError:
            return "Neither"

