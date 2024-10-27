import os
import yaml
from attr.converters import to_bool


class CityData:
    filename_method_parameters_config = '.config_method_parameters.yml'
    filename_met_parameters_config = '.config_meteorological_parameters.csv'
    filename_umep_city_processing_config = '.config_umep_city_processing.csv'

    folder_name_primary_source_data = 'primary_source_data'
    folder_name_met_files = 'met_files'
    folder_name_results = 'results_data'
    folder_name_preprocessed_data = 'preprocessed_data'
    folder_name_tcm_results = 'tcm_results'

    filename_wall_height = 'wallheight.tif'
    filename_wall_aspect = 'wallaspect.tif'
    filename_svfs_zip = 'svfs.zip'

    filename_cif_dem = 'cif_dem.tif'
    filename_cif_dsm_ground_build = 'cif_dsm_ground_build.tif'
    filename_cif_tree_canopy = 'cif_tree_canopy.tif'
    filename_cif_lulc = 'cif_lulc.tif'
    filename_cif_era5 = '?????'

    plugin_methods = ['all', 'wall_height_aspect', 'skyview_factor', 'solweig_only', 'solweig_full']

    def __new__(cls, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path):
        obj = super().__new__(cls)

        obj.folder_name_city_data = folder_name_city_data
        obj.folder_name_tile_data = folder_name_tile_data
        obj.source_base_path = source_base_path
        obj.target_base_path = target_base_path

        obj.source_city_path = str(os.path.join(source_base_path, folder_name_city_data))
        obj.source_city_data_path = str(os.path.join(obj.source_city_path, 'source_data'))

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

                obj.met_files = values[1].get('MetFiles')

                filenames = values[2]
                obj.dem_tif_filename = filenames['dem_tif_filename']
                obj.dsm_tif_filename = filenames['dsm_tif_filename']
                obj.tree_canopy_tif_filename = filenames['tree_canopy_tif_filename']
                obj.lulc_tif_filename = filenames['lulc_tif_filename']

                cif_processing = values[3]
                obj.retrieve_cif_dem_file = to_bool(cif_processing['retrieve_cif_dem_file'])
                obj.retrieve_cif_dsm_file = to_bool(cif_processing['retrieve_cif_dsm_file'])
                obj.retrieve_cif_tree_canopy_file = to_bool(cif_processing['retrieve_cif_tree_canopy_file'])
                obj.retrieve_cif_lulc_file = to_bool(cif_processing['retrieve_cif_lulc_file'])

        except Exception as e_msg:
            raise Exception(f'The {cls.filename_method_parameters_config} file not found or improperly defined in {city_configs}. (Error: {e_msg})')

        obj.source_tile_data_path = os.path.join(obj.source_city_data_path, obj.folder_name_primary_source_data,
                                                 obj.folder_name_tile_data)
        obj.source_met_files_path = os.path.join(obj.source_city_data_path, obj.folder_name_met_files)
        obj.source_dem_path = os.path.join(obj.source_tile_data_path, obj.dem_tif_filename)
        obj.source_dsm_path = os.path.join(obj.source_tile_data_path, obj.dsm_tif_filename)
        obj.source_tree_canopy_path = os.path.join(obj.source_tile_data_path, obj.tree_canopy_tif_filename)
        obj.source_land_cover_path = os.path.join(obj.source_tile_data_path, obj.lulc_tif_filename)

        if target_base_path:
            obj.target_path_city_data = str(os.path.join(obj.target_base_path, folder_name_city_data, cls.folder_name_results))
            obj.target_tile_data_path = os.path.join(obj.target_path_city_data, obj.folder_name_preprocessed_data,
                                                             obj.folder_name_tile_data)
            obj.target_tcm_results_path = os.path.join(obj.target_path_city_data, obj.folder_name_tcm_results)
            obj.target_wallheight_path = os.path.join(obj.target_tile_data_path, obj.filename_wall_height)
            obj.target_wallaspect_path = os.path.join(obj.target_tile_data_path, obj.filename_wall_aspect)
            obj.target_svfszip_path = os.path.join(obj.target_tile_data_path, obj.filename_svfs_zip)

        return obj