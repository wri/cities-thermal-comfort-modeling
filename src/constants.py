import os
from pathlib import Path

WGS_CRS = 'EPSG:4326'

ROOT_DIR = str(Path(os.path.dirname(os.path.abspath(__file__))).parent)
SRC_DIR = os.path.join(ROOT_DIR, 'src')
DATA_DIR = os.path.join(ROOT_DIR, 'data')

FILENAME_METHOD_YML_CONFIG = '.config_method_parameters.yml'

FOLDER_NAME_PRIMARY_DATA = 'primary_data'
FOLDER_NAME_PRIMARY_RASTER_FILES = 'raster_files'
FOLDER_NAME_PRIMARY_MET_FILES = 'met_files'
FOLDER_NAME_INTERMEDIATE_DATA = 'processed_data'
FOLDER_NAME_UMEP_TCM_RESULTS = 'tcm_results'

FILENAME_WALL_HEIGHT = 'wallheight.tif'
FILENAME_WALL_ASPECT = 'wallaspect.tif'
FILENAME_SVFS_ZIP = 'svfs.zip'
FILENAME_ERA5_UMEP = 'met_era5_hottest_days.txt'
FILENAME_ERA5_UPENN = 'met_era5_hottest_days.csv'

METHOD_TRIGGER_ERA5_DOWNLOAD = '<download_era5>'
PROCESSING_METHODS = ['download_only', 'umep_solweig', 'upenn_model']

VALID_PRIMARY_TYPES = [
    {
        "short_name": "dem",
        "yml_tag": "dem_tif_filename",
        "cif_template_name": 'cif_dem.tif',
    },
    {
        "short_name": "dsm",
        "yml_tag": "dsm_tif_filename",
        "cif_template_name": 'cif_dsm_ground_build.tif'
    },
    {
        "short_name": "lulc",
        "yml_tag": "lulc_tif_filename",
        "cif_template_name": 'cif_lulc.tif'
    },
    {
        "short_name": "open_urban",
        "yml_tag": "open_urban_tif_filename",
        "cif_template_name": 'cif_open_urban.tif'
    },
    {
        "short_name": "tree_canopy",
        "yml_tag": "tree_canopy_tif_filename",
        "cif_template_name": 'cif_tree_canopy.tif'
    },
    {
        "short_name": "albedo",
        "yml_tag": "albedo_tif_filename",
        "cif_template_name": 'cif_albedo.tif'
    },

]

