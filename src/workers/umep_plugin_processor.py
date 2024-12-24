import os
import tempfile
import time
import shutil
import warnings
import sys
import processing
from processing.core.Processing import Processing
from processing_umep.processing_umep import ProcessingUMEPProvider

from src.constants import FILENAME_WALL_HEIGHT, FILENAME_WALL_ASPECT

warnings.filterwarnings("ignore")

from pathlib import Path
from datetime import datetime
from qgis.core import QgsApplication
from worker_tools import remove_file, remove_folder, compute_time_diff_mins, create_folder, log_method_failure, \
    log_method_start, log_method_completion, start_model_logging, get_configurations
from city_data import CityData

import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

MAX_RETRY_COUNT = 3
RETRY_PAUSE_TIME_SEC = 10


def run_plugin(task_index, step_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path,
               target_base_path, met_filename, utc_offset):
    start_time = datetime.now()
    start_model_logging(target_base_path, folder_name_city_data)

    city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
    method_title = _assign_method_title(step_method)
    log_method_start(method_title, task_index, None, city_data.target_base_path)

    # Initiate QGIS and UMEP processing
    try:
        qgis_app = qgis_app_init()
        umep_provider = ProcessingUMEPProvider()
        qgis_app.processingRegistry().addProvider(umep_provider)
        Processing.initialize()
    except Exception as e_msg:
        msg = 'Processing could not initialize UMEP processing'
        log_method_failure(datetime.now(), msg, None, None, None, e_msg)
        # return

    e_msg = ''
    return_code = -999
    retry_count = 0
    with (tempfile.TemporaryDirectory() as tmpdirname):
        # Get the UMEP processing parameters and prepare for the method
        input_params, umep_method_title, keepers = _prepare_method_execution(step_method, city_data, tmpdirname, met_filename, utc_offset)

        while retry_count < MAX_RETRY_COUNT and return_code != 0:
            try:
                # Run the UMEP plugin!!
                processing.run(umep_method_title, input_params)

                # Prepare target folder and transfer over the temporary results
                try:
                    if step_method == 'solweig_only':
                        for temp_result_path, target_base_path in keepers.items():
                            remove_folder(target_base_path)
                            shutil.move(str(temp_result_path), str(target_base_path))
                    else:
                        for temp_result_path, target_base_path in keepers.items():
                            remove_file(target_base_path)
                            shutil.move(str(temp_result_path), str(target_base_path))
                except Exception as e_msg:
                    msg = (f'{method_title} processing succeeded but could not create target folder or move files: {city_data.target_preprocessed_tile_data_path}.')
                    log_method_failure(start_time, msg, task_index, None, city_data.target_base_path, e_msg)
                    return 1

                return_code = 0
            except Exception as e_msg:
                msg = f'task:{task_index} {method_title} failure. Retrying. ({e_msg})'
                log_method_failure(start_time, msg, task_index, None, city_data.target_base_path, e_msg)
                if retry_count < MAX_RETRY_COUNT:
                    time.sleep(RETRY_PAUSE_TIME_SEC)
                return_code = 3
            retry_count += 1

    if return_code == 0:
        log_method_completion(start_time, method_title, task_index, None, city_data.target_base_path)
    else:
        msg = f'{method_title} processing cancelled after {MAX_RETRY_COUNT} attempts.'
        log_method_failure(start_time, msg, task_index, None, city_data.target_base_path, '')

    run_duration_min = compute_time_diff_mins(start_time)

    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    return_stdout = (f'{{"task_index": {task_index}, "tile": "{folder_name_tile_data}", "step_index": {step_index}, '
                     f'"step_method": "{step_method}", "met_filename": "{met_filename}", "return_code": {return_code}, '
                     f'"start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}')

    return return_stdout


def _prepare_method_execution(method, city_data, tmpdirname, met_filename=None, utc_offset=None):
    keepers = {}

    if method == 'wall_height_aspect':
        create_folder(city_data.target_preprocessed_tile_data_path)

        temp_target_wallheight_path = os.path.join(tmpdirname, FILENAME_WALL_HEIGHT)
        temp_target_wallaspect_path = os.path.join(tmpdirname, FILENAME_WALL_ASPECT)
        input_params = {
            'INPUT': city_data.target_dsm_path,
            'INPUT_LIMIT': city_data.wall_lower_limit_height,
            'OUTPUT_HEIGHT': temp_target_wallheight_path,
            'OUTPUT_ASPECT': temp_target_wallaspect_path,
        }
        umep_method_title = "umep:Urban Geometry: Wall Height and Aspect"
        keepers[temp_target_wallheight_path] = city_data.target_wallheight_path
        keepers[temp_target_wallaspect_path] = city_data.target_wallaspect_path

    elif method == 'skyview_factor':
        create_folder(city_data.target_preprocessed_tile_data_path)

        temp_svfs_file_no_extension = os.path.join(tmpdirname, Path(city_data.target_svfszip_path).stem)
        temp_svfs_file_with_extension = os.path.join(tmpdirname, os.path.basename(city_data.target_svfszip_path))
        input_params = {
            'INPUT_DSM': city_data.target_dsm_path,
            'INPUT_CDSM': city_data.target_tree_canopy_path,
            'TRANS_VEG': 3,
            'INPUT_TDSM': None,
            'INPUT_THEIGHT': 25,
            'ANISO': True,
            'OUTPUT_DIR': tmpdirname,
            'OUTPUT_FILE': temp_svfs_file_no_extension
        }
        umep_method_title = 'umep:Urban Geometry: Sky View Factor'
        keepers[temp_svfs_file_with_extension] = city_data.target_svfszip_path
    else:
        target_met_file_path = os.path.join(city_data.target_met_filenames_path, met_filename)
        temp_met_folder = os.path.join(tmpdirname, Path(met_filename).stem, city_data.folder_name_tile_data)
        create_folder(temp_met_folder)
        target_met_folder = os.path.join(city_data.target_tcm_results_path, Path(met_filename).stem, city_data.folder_name_tile_data)
        input_params = {
            "INPUT_DSM": city_data.target_dsm_path,
            "INPUT_SVF": city_data.target_svfszip_path,
            "INPUT_HEIGHT": city_data.target_wallheight_path,
            "INPUT_ASPECT": city_data.target_wallaspect_path,
            "INPUT_CDSM": city_data.target_tree_canopy_path,
            "TRANS_VEG": 3,
            "LEAF_START": city_data.leaf_start,
            "LEAF_END": city_data.leaf_end,
            "CONIFER_TREES": city_data.conifer_trees,
            "INPUT_TDSM": None,
            "INPUT_THEIGHT": 25,
            "INPUT_LC": city_data.target_land_cover_path,
            "USE_LC_BUILD": False,
            "INPUT_DEM": city_data.target_dem_path,
            "SAVE_BUILD": False,
            "INPUT_ANISO": "",
            "ALBEDO_WALLS": city_data.albedo_walls,
            "ALBEDO_GROUND": city_data.albedo_ground,
            "EMIS_WALLS": city_data.emis_walls,
            "EMIS_GROUND": city_data.emis_ground,
            "ABS_S": 0.7,
            "ABS_L": 0.95,
            "POSTURE": 0,
            "CYL": True,
            "INPUTMET": target_met_file_path,
            "ONLYGLOBAL": True,
            "UTC": utc_offset,
            "POI_FILE": None,
            "POI_FIELD": '',
            "AGE": 35,
            "ACTIVITY": 80,
            "CLO": 0.9,
            "WEIGHT": 75,
            "HEIGHT": 180,
            "SEX": 0,
            "SENSOR_HEIGHT": 10,
            "OUTPUT_TMRT": city_data.output_tmrt,
            "OUTPUT_KDOWN": False,
            "OUTPUT_KUP": False,
            "OUTPUT_LDOWN": False,
            "OUTPUT_LUP": False,
            "OUTPUT_SH": city_data.output_sh,
            "OUTPUT_TREEPLANTER": False,
            "OUTPUT_DIR": temp_met_folder
        }
        umep_method_title = 'umep:Outdoor Thermal Comfort: SOLWEIG'

        keepers[temp_met_folder] = target_met_folder

    return input_params, umep_method_title, keepers


def _assign_method_title(method):
    if method == 'wall_height_aspect':
        method_title = 'Wall Height/Aspect'
    elif method == 'skyview_factor':
        method_title = 'Skyview Factor'
    else:
        method_title = 'SOLWEIG'
    return method_title


def qgis_app_init():
    qgis_home_path, qgis_plugin_path = get_configurations()
    sys.path.append(qgis_plugin_path)

    QgsApplication.setPrefixPath(qgis_home_path, True)
    qgis_app = QgsApplication([], False)
    qgis_app.initQgis()

    return qgis_app
