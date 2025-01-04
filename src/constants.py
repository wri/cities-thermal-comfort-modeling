import os
from pathlib import Path

ROOT_DIR = str(Path(os.path.dirname(os.path.abspath(__file__))).parent)
SRC_DIR = os.path.join(ROOT_DIR, 'src')
DATA_DIR = os.path.join(ROOT_DIR, 'data')

FILENAME_METHOD_YML_CONFIG = '.config_method_parameters.yml'

FOLDER_NAME_PRIMARY_DATA = 'primary_data'
FOLDER_NAME_PRIMARY_RASTER_FILES = 'raster_files'
FOLDER_NAME_PRIMARY_MET_FILENAMES = 'met_files'
FOLDER_NAME_INTERMEDIATE_DATA = 'processed_data'
FOLDER_NAME_RESULTS = 'results_data'
FOLDER_NAME_UMEP_TCM_RESULTS = 'umep_tcm_results'

FILENAME_WALL_HEIGHT = 'wallheight.tif'
FILENAME_WALL_ASPECT = 'wallaspect.tif'
FILENAME_SVFS_ZIP = 'svfs.zip'
FILENAME_ERA5 = 'met_era5_hottest_days.txt'

METHOD_TRIGGER_ERA5_DOWNLOAD = '<download_era5>'
PROCESSING_METHODS = ['cif_download_only', 'solweig_full']

