# see https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/PythonProcessing1.html

import logging
import os

from src.CityData import CityData

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
log_file_path = os.path.join(os.path.dirname(os.getcwd()), 'execution.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s\t%(message)s',
                    datefmt='%a_%Y_%b_%d_%H:%M:%S',
                    filename=log_file_path
                    )

import sys
import os
import tempfile
import warnings
import processing
import shutil
from pathlib import Path
from datetime import datetime
from osgeo import gdal
from osgeo.gdalconst import *
from src.tools import remove_file, clean_folder, create_folder
from processing.core.Processing import Processing


class UmepProcessingQgisPlugins:
    def __init__(self, qgis_app):
        self.qgis_app = qgis_app
        from processing_umep.processing_umep import ProcessingUMEPProvider
        umep_provider = ProcessingUMEPProvider()
        self.qgis_app.processingRegistry().addProvider(umep_provider)

    def generate_wall_height_aspect(self, runID, city_data):
        print('RunID:%s Generating wall-height/aspect results for tile:%s' % (runID, city_data.tile_folder_name))
        start_time = _log_method_start('Wall Height/Aspect', runID, None, city_data.source_base_path)
        warnings.filterwarnings("ignore")

        create_folder(city_data.target_preprocessed_data_path)
        remove_file(city_data.wallheight_path)
        remove_file(city_data.wallaspect_path)

        parin ={
            'INPUT': city_data.dsm_path,
            'INPUT_LIMIT': city_data.wall_lower_limit_height,
            'OUTPUT_HEIGHT': city_data.wallheight_path,
            'OUTPUT_ASPECT': city_data.wallaspect_path,
            }

        try:
            Processing.initialize()
            processing.run("umep:Urban Geometry: Wall Height and Aspect", parin)

            _log_method_completion(start_time, 'Wall Height/Aspect', runID, None, city_data.source_base_path)
            return 0
        except Exception as e_msg:
            log_method_failure(start_time, 'Wall Height/Aspect', runID, None, city_data.source_base_path, e_msg)
            return -1


    def generate_skyview_factor_files(self, runID, city_data):
        print('RunID:%s Generating skyview-factor results for tile:%s' % (runID, city_data.tile_folder_name))
        start_time = _log_method_start('Skyview-factor', runID, None, city_data.source_base_path)
        warnings.filterwarnings("ignore")

        create_folder(city_data.target_preprocessed_data_path)
        remove_file(city_data.svfszip_path)

        try:
            with tempfile.TemporaryDirectory() as tmpdirname:
                temp_svfs_file_no_extension = os.path.join(tmpdirname, Path(city_data.svfszip_path).stem)
                temp_svfs_file_with_extension = os.path.join(tmpdirname, os.path.basename(city_data.svfszip_path))

                # ANISO Specifies to generate 153 shadow images for use by SOLWEIG
                parin = {
                    'INPUT_DSM': city_data.dsm_path,
                    'INPUT_CDSM': city_data.vegcanopy_path,
                    'TRANS_VEG': 3,
                    'INPUT_TDSM': None,
                    'INPUT_THEIGHT': 25,
                    'ANISO': True,
                    'OUTPUT_DIR': tmpdirname,
                    'OUTPUT_FILE': temp_svfs_file_no_extension
                    }

                Processing.initialize()
                processing.run("umep:Urban Geometry: Sky View Factor", parin)

                shutil.move(temp_svfs_file_with_extension, city_data.target_preprocessed_data_path)

            _log_method_completion(start_time, 'Skyview-factor', runID, None, city_data.source_base_path)
            return 0
        except Exception as e_msg:
            log_method_failure(start_time, 'Skyview-factor', runID, None, city_data.source_base_path, e_msg)
            return -2


    def generate_solweig(self, runID, step, city_data: CityData, met_file_name, utc_offset):
        print('RunID:%s Generating SOLWEIG results for met_file:%s tile:%s' % (runID, met_file_name, city_data.tile_folder_name))
        start_time = _log_method_start('SOLWEIG', runID, step, city_data.source_base_path)
        warnings.filterwarnings("ignore")

        source_met_file_path = os.path.join(city_data.source_met_files_path, met_file_name)
        target_met_folder = os.path.join(city_data.target_tcm_results_path, Path(met_file_name).stem)

        create_folder(target_met_folder)
        clean_folder(target_met_folder)

        parin ={
               "INPUT_DSM":city_data.dsm_path,
               "INPUT_SVF":city_data.svfszip_path,
               "INPUT_HEIGHT":city_data.wallheight_path,
               "INPUT_ASPECT":city_data.wallaspect_path,
               "INPUT_CDSM":city_data.vegcanopy_path,
               "TRANS_VEG":3,
               "LEAF_START":city_data.leaf_start,
               "LEAF_END":city_data.leaf_end,
               "CONIFER_TREES":city_data.conifer_trees,
               "INPUT_TDSM":None,
               "INPUT_THEIGHT":25,
               "INPUT_LC":city_data.landcover_path,
               "USE_LC_BUILD":False,
               "INPUT_DEM":city_data.dem_path,
               "SAVE_BUILD":False,
               "INPUT_ANISO":"",
               "ALBEDO_WALLS":city_data.albedo_walls,
               "ALBEDO_GROUND":city_data.albedo_ground,
               "EMIS_WALLS":city_data.emis_walls,
               "EMIS_GROUND":city_data.emis_ground,
               "ABS_S":0.7,
               "ABS_L":0.95,
               "POSTURE":0,
               "CYL":True,
               "INPUTMET":source_met_file_path,
               "ONLYGLOBAL":True,
               "UTC":utc_offset,
               "POI_FILE":None,
               "POI_FIELD":'',
               "AGE":35,
               "ACTIVITY":80,
               "CLO":0.9,
               "WEIGHT":75,
               "HEIGHT":180,
               "SEX":0,
               "SENSOR_HEIGHT":10,
               "OUTPUT_TMRT":city_data.output_tmrt,
               "OUTPUT_KDOWN":False,
               "OUTPUT_KUP":False,
               "OUTPUT_LDOWN":False,
               "OUTPUT_LUP":False,
               "OUTPUT_SH":city_data.output_sh,
               "OUTPUT_TREEPLANTER":False,
               "OUTPUT_DIR":target_met_folder
            }

        try:
            Processing.initialize()
            processing.run("umep:Outdoor Thermal Comfort: SOLWEIG", parin)

            _log_method_completion(start_time, 'SOLWEIG', runID, step, city_data.source_base_path)
            return 0
        except Exception as e_msg:
            log_method_failure(start_time, 'SOLWEIG', runID, None, city_data.source_base_path, e_msg)
            return -3

    # def generate_aggregated_shadows(self, runID, step, dsm_tif, cdsm_tif, start_year, start_month, start_day, number_of_days_in_run, out_file_path):
    #     start_time = _start_log_entry(self, 'Shadow-generation', runID, step)
    #     warnings.filterwarnings("ignore")
    #
    #     baseraster = gdal.Open(dsm_tif)
    #     # initialize accumulation dataset with same dimensions as base and zeroed-out values
    #     aggregation_raster = baseraster.ReadAsArray().astype(float)
    #     aggregation_raster = aggregation_raster * 0.0
    #
    #     Processing.initialize()
    #     index = 0
    #     for i in range(0, number_of_days_in_run):
    #         date = datetime.date(start_year, start_month, start_day) + datetime.timedelta(days=i)
    #         date_str = date.strftime("%d-%m-%Y")
    #         print(date_str)
    #
    #         datetorun = QDate.fromString(date_str, "d-M-yyyy")
    #
    #         if (datetorun > QDate(start_year, 4, 15)) & (datetorun < QDate(start_year, 10, 1)):
    #             transVeg = 3
    #         else:
    #             transVeg = 49
    #
    #         with tempfile.TemporaryDirectory() as temp_dir:
    #             parin = {
    #                 'DATEINI' : datetorun,
    #                 'DST' : False,
    #                 'INPUT_ASPECT' : None,
    #                 'INPUT_CDSM' : cdsm_tif,
    #                 'INPUT_DSM' : dsm_tif,
    #                 'INPUT_HEIGHT' : None,
    #                 'INPUT_TDSM' : None,
    #                 'INPUT_THEIGHT' : 25,
    #                 'ITERTIME' : 30,
    #                 'ONE_SHADOW' : False,
    #                 'OUTPUT_DIR' : temp_dir,
    #                 'TIMEINI' : QTime(16, 32, 58),
    #                 'TRANS_VEG' : transVeg,
    #                 'UTC' : 1 }
    #
    #             processing.run("umep:Solar Radiation: Shadow Generator", parin)
    #
    #             # Aggregate raster values
    #             aggregation_raster = _aggregate_rasters_in_folder(temp_dir, aggregation_raster)
    #
    #         index += 1 #A counter that specifies total numer of shadows in a year (30 minute resolution)
    #
    #
    #     aggregation_raster = aggregation_raster / index
    #
    #     remove_file(out_file_path)
    #     _save_raster(baseraster, aggregation_raster, out_file_path)
    #     _end_log_entry(self, start_time, 'Shadow-generation', runID, step)
    #
    #     return

def _log_method_start(method, runId, step, source_base_path):
    if step is None:
        logging.info("run:%s\tStarting '%s'\tconfig:'%s')" % (runId, method, source_base_path))
    else:
        logging.info("run:%s\tStarting '%s' for met_series:%s\tconfig:'%s')" % (runId, method, step, source_base_path))
    return datetime.now()

def _log_method_completion(start_time, method, runId, step, source_base_path):
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.info("run:%s\tFinished '%s', runtime:%s mins\tconfig:'%s'" % (runId, method, runtime, source_base_path))
    else:
        logging.info("run:%s\tFinished '%s' for met_series:%s, runtime:%s mins\tconfig:'%s'" % (runId, method, step, runtime, source_base_path))

def log_method_failure(start_time, feature, runId, step, source_base_path, e_msg):
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.error("run:%s\t**** FAILED execution of '%s' after runtime:%s mins\tconfig:'%s'(%s)" % (runId, feature, runtime, source_base_path, e_msg))
    else:
        logging.error("run:%s\t**** FAILED execution of '%s' fpr met_series:%s after runtime:%s mins\tconfig:'%s'(%s)" % (runId, feature, step, runtime, source_base_path, e_msg))

def _aggregate_rasters_in_folder(folder_path, aggregation_raster):
    for file in os.listdir(folder_path):
        temp_file_path = os.path.join(folder_path, file)
        this_gdal_dataset = gdal.Open(temp_file_path)
        this_raster = this_gdal_dataset.ReadAsArray().astype(float)

        aggregation_raster = aggregation_raster + this_raster

        this_gdal_dataset = None
        os.remove(temp_file_path)
        return aggregation_raster

def _save_raster(gdal_data, raster, file_path):
    create_folder(os.path.dirname(os.path.abspath(file_path)))

    rows = gdal_data.RasterYSize
    cols = gdal_data.RasterXSize

    outDs = gdal.GetDriverByName("GTiff").Create(file_path, cols, rows, int(1), GDT_Float32)
    outBand = outDs.GetRasterBand(1)

    # write the data
    outBand.WriteArray(raster, 0, 0)
    # flush data to disk, set the NoData value and calculate stats
    outBand.FlushCache()
    outBand.SetNoDataValue(-9999)

    # georeference the image and set the projection
    outDs.SetGeoTransform(gdal_data.GetGeoTransform())
    outDs.SetProjection(gdal_data.GetProjection())
