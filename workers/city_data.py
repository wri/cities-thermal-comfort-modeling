import os
import yaml
from attr.converters import to_bool

class CityData:
    file_name_city_processing_config = '.config_city_processing.yml'
    file_name_met_time_series_config = '.config_met_time_series.csv'
    file_name_umep_city_processing_config = '.config_umep_city_processing.csv'
    
    folder_name_primary_source_data = 'primary_source_data'
    folder_name_met_files = 'met_files'
    folder_name_preprocessed_data = 'preprocessed_data'
    folder_name_tcm_results = 'tcm_results'
    
    file_name_wall_height = 'wallheight.tif'
    file_name_wall_aspect = 'wallaspect.tif'
    file_name_svfs_zip = 'svfs.zip'

    plugin_methods = ['all', 'wall_height_aspect', 'skyview_factor', 'solweig_only', 'solweig_full' ]


    def __new__(cls,  folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
        obj = super().__new__(cls)

        obj.folder_name_city_data = folder_name_city_data
        obj.folder_name_tile_data = folder_name_tile_data
        obj.source_base_path = source_base_path
        obj.target_base_path = target_base_path

        obj.source_city_data_path = str(os.path.join(source_base_path, folder_name_city_data, 'source_data'))
        city_configs = os.path.join(obj.source_city_data_path, cls.file_name_city_processing_config)
        with open(city_configs, 'r') as stream:
            try:
                values = list(yaml.safe_load_all(stream))[0]

                method_attributes = values[0]
                obj.wall_lower_limit_height = method_attributes['wall_height_aspect']['lower_limit_for_wall_height']
                obj.light_transmissivity = method_attributes['skyview_factor']['transmissivity_of_light_through_vegetation']
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

                file_names = values[1]
                obj.dem_file = file_names['dem_tif_filename']
                obj.dsm_file = file_names['dsm_ground_build_tif_filename']
                obj.veg_canopy_file = file_names['veg_canopy_tif_filename']
                obj.landcover_file = file_names['landcover_tif_filename']

            except yaml.YAMLError as e_msg:
                raise Exception(f'The {cls.file_name_city_processing_config} file not found or improperly defined in {city_configs}. ({e_msg})')

        obj.target_path_city_data = str(os.path.join(obj.target_base_path, folder_name_city_data, 'results_data'))

        obj.source_tile_data_path = os.path.join(obj.source_city_data_path, obj.folder_name_primary_source_data, obj.folder_name_tile_data)
        obj.source_met_files_path = os.path.join(obj.source_city_data_path, obj.folder_name_met_files)
        obj.target_preprocessed_data_path = os.path.join(obj.target_path_city_data, obj.folder_name_preprocessed_data, obj.folder_name_tile_data)
        obj.target_tcm_results_path = os.path.join(obj.target_path_city_data, obj.folder_name_tcm_results)

        obj.source_dem_path = os.path.join(obj.source_tile_data_path, obj.dem_file)
        obj.source_dsm_path = os.path.join(obj.source_tile_data_path, obj.dsm_file)
        obj.source_veg_canopy_path = os.path.join(obj.source_tile_data_path, obj.veg_canopy_file)
        obj.source_land_cover_path = os.path.join(obj.source_tile_data_path, obj.landcover_file)

        obj.target_wallheight_path = os.path.join(obj.target_preprocessed_data_path, obj.file_name_wall_height)
        obj.target_wallaspect_path = os.path.join(obj.target_preprocessed_data_path, obj.file_name_wall_aspect)
        obj.target_svfszip_path = os.path.join(obj.target_preprocessed_data_path, obj.file_name_svfs_zip)

        return obj