import os
import tempfile
import time
import shutil
import warnings
import random
import multiprocessing as mp
from math import floor

from pathlib import Path
from datetime import datetime

from src.constants import FILENAME_SVFS_ZIP
from src.workers.model_upenn.svfsCalculator_veg import run_skyview_calculations
from src.workers.model_upenn.weather_meanRadiantT import run_mrt_calculations
from src.workers.worker_tools import remove_file, remove_folder, compute_time_diff_mins, create_folder
from src.workers.city_data import CityData
from src.workers.logger_tools import setup_logger, log_method_start, log_method_failure, log_method_completion, \
    log_model_metadata_message, setup_metadata_logger
from src.workers.model_upenn.aspect_height import run_wall_calculations

warnings.filterwarnings("ignore")

MAX_RETRY_COUNT = 10
RETRY_PAUSE_TIME_SEC = 1

MAX_TILE_PAUSE_MINS = 5


def run_upenn_module(step_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path,
                     target_base_path, met_filename, seasonal_utc_offset):
    # Pause for a random time interval to offset retrievals between tiles
    # For small box, use smaller delay
    max_tile_pause_mins = 1 if mp.cpu_count() <= 8 else MAX_TILE_PAUSE_MINS
    wait_secs = int(random.uniform(0, max_tile_pause_mins * 60))
    time.sleep(wait_secs)

    start_time = datetime.now()

    tiled_city_data = CityData(None, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
    processing_logger = setup_logger(tiled_city_data.target_model_log_path)
    metadata_logger = setup_metadata_logger(tiled_city_data.target_umep_metadata_log_path)
    method_title = _assign_method_title(step_method)
    method_title_with_tile = f'{method_title} for {folder_name_tile_data}'
    log_method_start(method_title_with_tile, None, tiled_city_data.target_base_path, processing_logger)

    e_msg = ''
    return_code = -999
    retry_count = 0
    with (tempfile.TemporaryDirectory() as tmpdirname):
        # Get the UMEP processing parameters and prepare for the method
        method_params, keepers = _prepare_method_execution(step_method, tiled_city_data, tmpdirname, metadata_logger,
                                                           met_filename, seasonal_utc_offset)
        while retry_count < MAX_RETRY_COUNT and return_code != 0:
            try:
                if step_method == 'wall_height_aspect':
                    print(f"tile: {folder_name_tile_data} - building wall height/aspect files..")
                    run_wall_calculations(method_params)
                elif step_method == 'skyview_factor':
                    print(f"tile: {folder_name_tile_data} - building skyview-factor files..")
                    run_skyview_calculations(method_params, folder_name_tile_data)
                else:
                    from datetime import date
                    sampling_hours = tiled_city_data.sampling_local_hours
                    run_mrt_calculations(method_params, sampling_hours)

                # Prepare target folder and transfer over the temporary results
                try:
                    if step_method == 'skyview_factor':
                        for temp_result_path, target_base_path in keepers.items():
                            remove_folder(target_base_path)
                            shutil.move(str(temp_result_path), str(target_base_path))
                    else:
                        for temp_result_path, target_base_path in keepers.items():
                            remove_file(target_base_path)
                            shutil.move(str(temp_result_path), str(target_base_path))
                except Exception as e_msg:
                    msg = (f'{method_title} processing succeeded but could not create target folder or move files: '
                           f'{tiled_city_data.target_intermediate_tile_data_path} due to {e_msg}')
                    log_method_failure(datetime.now(), method_title, tiled_city_data.source_base_path, msg, processing_logger)
                    return 1

                return_code = 0
            except Exception as e_msg:
                msg = f'task:{method_title} failure. Retrying. ({e_msg})'
                log_method_failure(datetime.now(), method_title, tiled_city_data.source_base_path, msg, processing_logger)
                if retry_count < MAX_RETRY_COUNT:
                    time.sleep(RETRY_PAUSE_TIME_SEC)
                return_code = 3
            retry_count += 1

    if return_code == 0:
        log_method_completion(start_time, method_title,  None, tiled_city_data.target_base_path, processing_logger)
    else:
        msg = f'{method_title} processing cancelled after {MAX_RETRY_COUNT} attempts.'
        log_method_failure(datetime.now(), method_title, tiled_city_data.source_base_path, msg, processing_logger)

    run_duration_min = compute_time_diff_mins(start_time)

    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    return_stdout = (f'{{"tile": "{folder_name_tile_data}", "step_index": {step_index}, '
                     f'"step_method": "{step_method}", "met_filename": "{met_filename}", "return_code": {return_code}, '
                     f'"start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}')

    return return_stdout

def _write_metadata(method, met_filename, input_params, exclusion_keys, metadata_logger):
    for key, value in input_params.items():
        if key not in exclusion_keys:
            log_model_metadata_message(method, met_filename, key, value, metadata_logger)

def _prepare_method_execution(method, tiled_city_data, tmpdirname, metadata_logger, met_filename, seasonal_utc_offset):
    keepers = {}

    if method == 'wall_height_aspect':
        create_folder(tiled_city_data.target_intermediate_tile_data_path)

        temp_target_wallheight_path = os.path.join(tmpdirname, tiled_city_data.wall_height_filename)
        temp_target_wallaspect_path = os.path.join(tmpdirname, tiled_city_data.wall_aspect_filename)
        method_params = {
            'INPUT': tiled_city_data.target_dsm_path,
            'INPUT_LIMIT': tiled_city_data.wall_lower_limit_height,
            'OUTPUT_HEIGHT': temp_target_wallheight_path,
            'OUTPUT_ASPECT': temp_target_wallaspect_path,
        }
        keepers[temp_target_wallheight_path] = tiled_city_data.target_wallheight_path
        keepers[temp_target_wallaspect_path] = tiled_city_data.target_wallaspect_path

        # exclude output paths and write remainder to log file
        exclusion_keys = ['OUTPUT_HEIGHT', 'OUTPUT_ASPECT']
        _write_metadata(method, None, method_params, exclusion_keys, metadata_logger)

    elif method == 'skyview_factor':
        create_folder(tiled_city_data.target_intermediate_tile_data_path)
        temp_svfs_file_no_extension = Path(tiled_city_data.target_svfszip_path).stem
        temp_svfs_file_path = os.path.join(tmpdirname, temp_svfs_file_no_extension)
        create_folder(temp_svfs_file_path)
        method_params = {
            'INPUT_DSM': tiled_city_data.target_dsm_path,
            'INPUT_CDSM': tiled_city_data.target_tree_canopy_path,
            'TRANS_VEG': tiled_city_data.light_transmissivity,
            'INPUT_TDSM': None,
            'INPUT_THEIGHT': tiled_city_data.trunk_zone_height,
            'ANISO': True,
            'OUTPUT_DIR': tmpdirname,
            'OUTPUT_FILE': temp_svfs_file_path
        }
        keepers[temp_svfs_file_path] = tiled_city_data.target_svfszip_path

        # exclude output paths and write remainder to log file
        exclusion_keys = ['OUTPUT_DIR', 'OUTPUT_FILE']
        _write_metadata(method, None, method_params, exclusion_keys, metadata_logger)

    else:
        target_met_file_path = os.path.join(tiled_city_data.target_met_files_path, met_filename)
        temp_met_folder = os.path.join(tmpdirname, Path(met_filename).stem, tiled_city_data.folder_name_tile_data)
        create_folder(temp_met_folder)
        target_met_folder = os.path.join(tiled_city_data.target_tcm_results_path, Path(met_filename).stem, tiled_city_data.folder_name_tile_data)
        method_params = {
            "INPUT_DSM": tiled_city_data.target_dsm_path,
            "INPUT_SVF": tiled_city_data.target_svfszip_path,
            "INPUT_HEIGHT": tiled_city_data.target_wallheight_path,
            "INPUT_ASPECT": tiled_city_data.target_wallaspect_path,
            "INPUT_CDSM": tiled_city_data.target_tree_canopy_path,
            "INPUT_ALBEDO": tiled_city_data.target_albedo_cloud_masked_path,
            "TRANS_VEG": tiled_city_data.light_transmissivity,
            "LEAF_START": tiled_city_data.leaf_start,
            "LEAF_END": tiled_city_data.leaf_end,
            "CONIFER_TREES": tiled_city_data.conifer_trees,
            "INPUT_TDSM": None,
            "INPUT_THEIGHT": tiled_city_data.trunk_zone_height,
            "INPUT_LC": tiled_city_data.target_lulc_path,
            "USE_LC_BUILD": False,
            "INPUT_DEM": tiled_city_data.target_dem_path,
            "SAVE_BUILD": False,
            "INPUT_ANISO": "",
            "ALBEDO_WALLS": tiled_city_data.albedo_walls,
            "ALBEDO_GROUND": tiled_city_data.albedo_ground,
            "EMIS_WALLS": tiled_city_data.emis_walls,
            "EMIS_GROUND": tiled_city_data.emis_ground,
            "ABS_S": 0.7,
            "ABS_L": 0.95,
            "POSTURE": 0,
            "CYL": True,
            "INPUTMET": target_met_file_path,
            "ONLYGLOBAL": True,
            "UTC": seasonal_utc_offset,
            "POI_FILE": None,
            "POI_FIELD": '',
            "AGE": 35,
            "ACTIVITY": 80,
            "CLO": 0.9,
            "WEIGHT": 75,
            "HEIGHT": 180,
            "SEX": 0,
            "SENSOR_HEIGHT": 10,
            "OUTPUT_TMRT": tiled_city_data.output_tmrt,
            "OUTPUT_KDOWN": False,
            "OUTPUT_KUP": False,
            "OUTPUT_LDOWN": False,
            "OUTPUT_LUP": False,
            "OUTPUT_SH": tiled_city_data.output_sh,
            "OUTPUT_TREEPLANTER": False,
            "OUTPUT_DIR": temp_met_folder,
        }
        keepers[temp_met_folder] = target_met_folder

        # exclude output paths and write remainder to log file
        exclusion_keys = ['OUTPUT_DIR']
        _write_metadata(method, met_filename, method_params, exclusion_keys, metadata_logger)

    return method_params, keepers


def _assign_method_title(method):
    if method == 'wall_height_aspect':
        method_title = 'Wall Height/Aspect'
    elif method == 'skyview_factor':
        method_title = 'Skyview Factor'
    else:
        method_title = 'SOLWEIG'
    return method_title