# see https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/PythonProcessing1.html

import os
import tempfile
import warnings
import shutil
from pathlib import Path
from datetime import datetime
from osgeo import gdal
from osgeo.gdalconst import *
# import dask

from src_old.CityData import CityData
from src_old.logger_tools import log_method_start, log_method_completion, log_method_failure, log_other_failure
from src_old.qgis_initializer import create_new_instance
# from src_old.plugin_runner import run_wall_height_aspect, create_new_instance
from src_old.tools import remove_file, clean_folder, create_folder

MAX_RETRY_COUNT = 3


def generate_wall_height_aspect(task_id, city_data):
    start_time = datetime.now()
    # print('task_id:%s Generating wall-height/aspect results for tile:%s' % (task_id, city_data.tile_folder_name))
    log_method_start('Wall Height/Aspect', task_id, None, city_data.source_base_path)
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

    retry_count = 1
    return_code = -999
    while retry_count <= MAX_RETRY_COUNT and return_code != 0:
        try:
            from src_old.plugin_runner import run_wall_height_aspect
            run_wall_height_aspect(parin)

            # import processing
            # from processing.core.Processing import Processing
            # from processing_umep.processing_umep import ProcessingUMEPProvider
            # from src_old.plugin_runner import UmepProcessingQgisPlugins
            # UmepProcessingQgisPlugins = create_new_instance()
            # upp = UmepProcessingQgisPlugins()
            # run_wall_height_aspect(parin)
            # qgis_app = create_new_instance().qgis_app
            # qgis_app = UmepProcessingQgisPlugins.qgis_app
            # pp = UmepProcessingQgisPlugins.proc()
            # umep_provider = ProcessingUMEPProvider()
            # qgis_app = create_new_instance().qgis_app
            # qgis_app.processingRegistry().addProvider(umep_provider)

            # processing.Processing.initialize()
            # # processing.Processing.activateProvider(ProcessingUMEPProvider)
            # processing.run("umep:Urban Geometry: Wall Height and Aspect", parin)


            log_method_completion(start_time, 'Wall Height/Aspect', task_id, None, city_data.source_base_path)
            return_code = 0
        except Exception as e_msg:
            print(e_msg)
            # print('task_id:%s Height/Aspect failure. Retrying' % (task_id))
            log_method_failure(start_time, 'Wall Height/Aspect for try %s of %s retries' % (retry_count, MAX_RETRY_COUNT),
                               task_id, None, city_data.source_base_path, e_msg)
            return_code = -1

        retry_count += 1
    if retry_count >= MAX_RETRY_COUNT:
        # print('task_id:%s Height/Aspect failure. Maximum retries exceeded. See log for details.' % (task_id))
        log_other_failure('Wall Height/Aspect  failure exceeded maximum retry.', '')

    return return_code


def generate_skyview_factor_files(task_id, city_data):
    start_time = datetime.now()
    # print('task_id:%s Generating skyview-factor results for tile:%s' % (task_id, city_data.tile_folder_name))
    log_method_start('Skyview-factor', task_id, None, city_data.source_base_path)
    warnings.filterwarnings("ignore")

    create_folder(city_data.target_preprocessed_data_path)
    remove_file(city_data.svfszip_path)

    retry_count = 1
    return_code = -999
    while retry_count <= MAX_RETRY_COUNT and return_code != 0:
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

                run_skyview_factor(parin)

                shutil.move(temp_svfs_file_with_extension, city_data.target_preprocessed_data_path)

            log_method_completion(start_time, 'Skyview-factor', task_id, None, city_data.source_base_path)
            return_code = 0
        except Exception as e_msg:
            # print('task_id:%s Skyview-factor  failure. Retrying' % (task_id))
            log_method_failure(start_time, 'Skyview-factor for try %s of %s retries' % (retry_count, MAX_RETRY_COUNT),
                               task_id, None, city_data.source_base_path, e_msg)
            return_code = -2

        retry_count += 1
    if retry_count >= MAX_RETRY_COUNT:
        # print('task_id:%s Skyview-factor failure. Maximum retries exceeded. See log for details.' % (task_id))
        log_other_failure('Skyview-factor failure exceeded maximum retry.', '')

    return return_code


def generate_solweig(task_id, step, city_data: CityData, met_file_name, utc_offset):
    start_time = datetime.now()
    # print('task_id:%s Generating SOLWEIG results for met_file:%s tile:%s' % (task_id, met_file_name, city_data.tile_folder_name))
    log_method_start('SOLWEIG', task_id, step, city_data.source_base_path)
    warnings.filterwarnings("ignore")

    source_met_file_path = os.path.join(city_data.source_met_files_path, met_file_name)
    target_met_folder = os.path.join(city_data.target_tcm_results_path, Path(met_file_name).stem, city_data.tile_folder_name)

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

    retry_count = 1
    return_code = -999
    while retry_count <= MAX_RETRY_COUNT and return_code != 0:
        try:
            run_solweig(parin)

            log_method_completion(start_time, 'SOLWEIG', task_id, step, city_data.source_base_path)
            return_code = 0
        except Exception as e_msg:
            # print('task_id:%s SOLWEIG failure. Retrying' % (task_id))
            log_method_failure(start_time, 'SOLWEIG for try %s of %s retries' % (retry_count, MAX_RETRY_COUNT),
                               task_id, None, city_data.source_base_path, e_msg)
            return_code = -3

        retry_count += 1
    if retry_count >= MAX_RETRY_COUNT:
        # print('task_id:%s SOLWEIG failure. Maximum retries exceeded. See log for details.' % (task_id))
        log_other_failure('SOLWEIG failure exceeded maximum retry.', '')

    return return_code

    # def generate_aggregated_shadows(self, task_id, step, dsm_tif, cdsm_tif, start_year, start_month, start_day, number_of_days_in_run, out_file_path):
    #     start_time = _start_log_entry(self, 'Shadow-generation', task_id, step)
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
    #     _end_log_entry(self, start_time, 'Shadow-generation', task_id, step)
    #
    #     return



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
