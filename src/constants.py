import os
from pathlib import Path

from city_metrix.constants import CTCM_CACHE_S3_BUCKET_URI, DEFAULT_DEVELOPMENT_ENV, DEFAULT_PRODUCTION_ENV, \
    CIF_TESTING_S3_BUCKET_URI

S3_PUBLICATION_BUCKET = CTCM_CACHE_S3_BUCKET_URI # for production  # CIF_TESTING_S3_BUCKET_URI # for testing
# S3_PUBLICATION_BUCKET = CIF_TESTING_S3_BUCKET_URI # for production  # CIF_TESTING_S3_BUCKET_URI # for testing
S3_PUBLICATION_ENV = DEFAULT_DEVELOPMENT_ENV # DEFAULT_PRODUCTION_ENV # DEFAULT_DEVELOPMENT_ENV

WGS_CRS = 'EPSG:4326'
TILE_NUMBER_PADCOUNT = 5

ROOT_DIR = str(Path(os.path.dirname(os.path.abspath(__file__))).parent)
SRC_DIR = os.path.join(ROOT_DIR, 'src')
DATA_DIR = os.path.join(ROOT_DIR, 'data')

FOLDER_NAME_ADMIN_DATA = '.admin'
FOLDER_NAME_QGIS_DATA = '.qgis_data'
FOLDER_NAME_PRIMARY_DATA = 'primary_data'
FOLDER_NAME_PRIMARY_RASTER_FILES = 'raster_files'
FOLDER_NAME_PRIMARY_MET_FILES = 'met_files'
FOLDER_NAME_INTERMEDIATE_DATA = 'processed_data'
FOLDER_NAME_UMEP_TCM_RESULTS = 'tcm_results'

FILENAME_METHOD_YML_CONFIG = '.config_method_parameters.yml'
FILENAME_TILE_GRID = 'tile_grid.geojson'
FILENAME_UNBUFFERED_TILE_GRID = 'unbuffered_tile_grid.geojson'
FILENAME_WALL_HEIGHT = 'wallheight.tif'
FILENAME_WALL_ASPECT = 'wallaspect.tif'
FILENAME_SVFS_ZIP = 'svfs.zip'
FILENAME_ERA5_UMEP = 'met_era5_hottest_days.txt'
FILENAME_ERA5_UPENN = 'met_era5_hottest_days.csv'

METHOD_TRIGGER_ERA5_DOWNLOAD = 'ERA5:<*>'
PRIOR_5_YEAR_KEYWORD = "prior_5_years"

PROCESSING_METHODS = ['download_only', 'umep_solweig', 'upenn_model']

MET_HEADER_UMEP = ['%iy', 'id', 'it', 'imin', 'qn', 'qh', 'qe', 'qs', 'qf', 'U', 'RH', 'Tair', 'press', 'rain', 'kdown', 'snow', 'ldown', 'fcld', 'wuh', 'xsmd', 'lai', 'kdiff', 'kdir', 'wdir']
MET_HEADER_0_UPENN = ['', 'Source', 'Latitude', 'Longitude', 'Local Time Zone', 'Clearsky DHI Units', 'Clearsky DNI Units', 'Clearsky GHI Units', 'DHI Units', 'DNI Units', 'GHI Units', 'Temperature Units', 'Pressure Units', 'Relative Humidity Units', 'Wind Speed Units']
MET_HEADER_1_UPENN = ['0', 'CDS-ERA5', '<lat>', '<lon>', '<seasonal_utc_offset>', 'w/m2', 'w/m2', 'w/m2', 'w/m2', 'w/m2', 'w/m2', 'c', 'mbar', '%', 'm/s']
MET_HEADER_2_UPENN = ['Index', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'DHI', 'DNI', 'GHI', 'Clearsky DHI', 'Clearsky DNI', 'Clearsky GHI', 'Wind Speed', 'Relative Humidity', 'Temperature', 'Pressure']

VALID_PRIMARY_TYPES = [
    {
        "short_name": "albedo_cloud_masked",
        "yml_tag": "albedo_cloud_masked_tif_filename",
        "cif_template_name": 'cif_albedo_cloud_masked.tif'
    },
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
]

