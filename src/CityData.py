import os
import yaml


SOURCE_DATA_SUBFOLDER = "primary_source_data"
SOURCE_MET_SUBFOLDER = "met_files"
SOURCE_DATA_MAP = "primary_source_data.yml"
GENERATED_DATA_ATTRIBUTES = 'data_processing_attributes.yml'
GENERATED_PREPROCESSED_SUBFOLDER = 'preprocessed_data'
GENERATED_WALL_HEIGHT_FILE = 'wallheight.tif'
GENERATED_WALL_ASPECT_FILE = 'wallaspect.tif'
GENERATED_SVF_ZIP_FILE = 'svfs.zip'
GENERATED_TCM_SUBFOLDER = 'tcm_results'

class CityData:
    def __init__(self, source_base_path, target_base_path, city_folder_name, tile_folder_name,
                 dem_file, dsm_file, veg_canopy_file, landcover_file,
                 wall_height_file, wall_aspect_file, wall_lower_limit_height,
                 svfs_zip_file, light_transmissivity, trunk_zone_height):

        self.source_base_path = source_base_path
        self.target_base_path = target_base_path
        self.city_folder_name = city_folder_name
        self.tile_folder_name = tile_folder_name

        self.dem_file = dem_file
        self.dsm_file = dsm_file
        self.veg_canopy_file = veg_canopy_file
        self.landcover_file = landcover_file

        self.wall_height_file = wall_height_file
        self.wall_aspect_file = wall_aspect_file
        self.wall_lower_limit_height = wall_lower_limit_height
        self.svfs_zip_file = svfs_zip_file
        self.light_transmissivity = light_transmissivity
        self.trunk_zone_height = trunk_zone_height

        self.city_source_path = str(os.path.join(self.source_base_path, city_folder_name))
        self.city_target_path = str(os.path.join(self.target_base_path, city_folder_name))

        self.data_source_path = os.path.join(self.city_source_path, SOURCE_DATA_SUBFOLDER, self.tile_folder_name)
        self.met_files_path = os.path.join(self.city_source_path, SOURCE_MET_SUBFOLDER)
        self.preprocessed_data_path = os.path.join(self.city_target_path, GENERATED_PREPROCESSED_SUBFOLDER, self.tile_folder_name)
        self.tcm_results_path = os.path.join(self.city_target_path, GENERATED_TCM_SUBFOLDER, self.tile_folder_name)

        self.dem_path = os.path.join(self.data_source_path, self.dem_file)
        self.dsm_path = os.path.join(self.data_source_path, self.dsm_file)
        self.vegcanopy_path = os.path.join(self.data_source_path, self.veg_canopy_file)
        self.landcover_path = os.path.join(self.data_source_path, self.landcover_file)

        self.wallheight_path = os.path.join(self.preprocessed_data_path, self.wall_height_file)
        self.wallaspect_path = os.path.join(self.preprocessed_data_path, self.wall_aspect_file)
        self.svfszip_path = os.path.join(self.preprocessed_data_path, self.svfs_zip_file)

def instantiate_city_data(city_folder_name, tile_folder_name, source_base_path, target_base_path):
    city_source_path = str(os.path.join(source_base_path, city_folder_name))
    source_data_map = os.path.join(city_source_path, SOURCE_DATA_MAP)
    with open(source_data_map) as source_data_file_map:
        try:
            source_data_dict = yaml.safe_load(source_data_file_map)
            dem_file = source_data_dict['dem_tif_filename']
            dsm_file= source_data_dict['dsm_tif_filename']
            veg_canopy_file = source_data_dict['veg_canopy_tif_filename']
            landcover_file = source_data_dict['landcover_tif_filename']
        except yaml.YAMLError as exc:
            raise Exception('The %s file not found or improperly defined in %s' %
                            (SOURCE_DATA_MAP, city_source_path))

    generated_data_atts = os.path.join(city_source_path, GENERATED_DATA_ATTRIBUTES)
    with open(generated_data_atts) as generated_attributes:
        try:
            generated_atts_dict = yaml.safe_load(generated_attributes)
            wall_lower_limit_height = generated_atts_dict['wall_height_aspect']['lower_limit_for_wall_height']
            light_transmissivity = generated_atts_dict['skyview_factor']['transmissivity_of_light_through_vegetation']
            trunk_zone_height = generated_atts_dict['skyview_factor']['trunk_zone_height']

        except yaml.YAMLError as exc:
            raise Exception('The %s file not found or improperly defined in %s' %
                            (GENERATED_DATA_ATTRIBUTES, generated_data_atts))

    city_data = CityData(source_base_path, target_base_path, city_folder_name, tile_folder_name,
                         dem_file, dsm_file, veg_canopy_file, landcover_file,
                         GENERATED_WALL_HEIGHT_FILE, GENERATED_WALL_ASPECT_FILE, wall_lower_limit_height,
                         GENERATED_SVF_ZIP_FILE, light_transmissivity, trunk_zone_height)

    return city_data