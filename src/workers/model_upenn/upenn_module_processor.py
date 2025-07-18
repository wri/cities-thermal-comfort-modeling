import os
import tempfile
import time
import shutil
import warnings
import sys


from pathlib import Path
from datetime import datetime

from src.constants import FILENAME_SVFS_ZIP
from src.workers.model_upenn.svfsCalculator_veg import run_skyview_calculations
from src.workers.worker_tools import remove_file, remove_folder, compute_time_diff_mins, create_folder, get_configurations
from src.workers.city_data import CityData
from src.workers.logger_tools import setup_logger, log_method_start, log_method_failure, log_method_completion, \
    log_model_metadata_message, setup_metadata_logger
from src.workers.model_upenn.aspect_height import run_wall_calculations

warnings.filterwarnings("ignore")

MAX_RETRY_COUNT = 3
RETRY_PAUSE_TIME_SEC = 10


def run_upenn_module(step_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path,
                     target_base_path, met_filename, utc_offset):
    start_time = datetime.now()

    tiled_city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
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
        method_params, keepers = _prepare_method_execution(step_method, tiled_city_data, tmpdirname,
                                                                             metadata_logger, met_filename, utc_offset)

        while retry_count < MAX_RETRY_COUNT and return_code != 0:
            try:
                if step_method == 'wall_height_aspect':
                    dsm_source_file = method_params['INPUT']
                    input_wall_limit = method_params['INPUT_LIMIT']
                    output_height_file = method_params['OUTPUT_HEIGHT']
                    output_aspect_file = method_params['OUTPUT_ASPECT']
                    run_wall_calculations(dsm_source_file, input_wall_limit, output_height_file, output_aspect_file)
                elif step_method == 'skyview_factor':
                    dsm_source_file = method_params['INPUT_DSM']
                    cdsm_source_file = method_params['INPUT_CDSM']
                    TRANS_VEG = method_params['TRANS_VEG']
                    aa = method_params['INPUT_TDSM']
                    INPUT_THEIGHT = method_params['INPUT_THEIGHT']
                    aa = method_params['ANISO']
                    svffolder = method_params['OUTPUT_DIR']
                    OUTPUT_FILE = method_params['OUTPUT_FILE']

                    run_skyview_calculations(dsm_source_file, cdsm_source_file, OUTPUT_FILE, TRANS_VEG, INPUT_THEIGHT)

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

def _prepare_method_execution(method, tiled_city_data, tmpdirname, metadata_logger, met_filename=None, utc_offset=None):
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

    return method_params, keepers


def _assign_method_title(method):
    if method == 'wall_height_aspect':
        method_title = 'Wall Height/Aspect'
    elif method == 'skyview_factor':
        method_title = 'Skyview Factor'
    else:
        method_title = 'SOLWEIG'
    return method_title