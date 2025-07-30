import os
import tempfile
import time
import shutil
import warnings
import sys

from pathlib import Path
from datetime import datetime

from src.constants import FILENAME_SVFS_ZIP
from src.workers.worker_tools import remove_file, remove_folder, compute_time_diff_mins, create_folder, get_configurations
from src.workers.city_data import CityData
from src.workers.logger_tools import setup_logger, log_method_start, log_method_failure, log_method_completion, \
    log_model_metadata_message, setup_metadata_logger

warnings.filterwarnings("ignore")

MAX_RETRY_COUNT = 3
RETRY_PAUSE_TIME_SEC = 10


def run_umep_plugin(step_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path,
                    target_base_path, met_filename, utc_offset):
    start_time = datetime.now()

    tiled_city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
    processing_logger = setup_logger(tiled_city_data.target_model_log_path)
    metadata_logger = setup_metadata_logger(tiled_city_data.target_umep_metadata_log_path)
    method_title = _assign_method_title(step_method)
    method_title_with_tile = f'{method_title} for {folder_name_tile_data}'
    log_method_start(method_title_with_tile, None, tiled_city_data.target_base_path, processing_logger)

    # Initiate QGIS and UMEP processing
    try:
        import processing
        from processing.core.Processing import Processing
        from processing_umep.processing_umep import ProcessingUMEPProvider
        qgis_app = qgis_app_init()
        umep_provider = ProcessingUMEPProvider()
        qgis_app.processingRegistry().addProvider(umep_provider)
        Processing.initialize()
    except Exception as e_msg:
        msg = f'Processing could not initialize UMEP processing {e_msg}'
        log_method_failure(datetime.now(), 'UMEP', tiled_city_data.source_base_path, msg, processing_logger)


    e_msg = ''
    return_code = -999
    retry_count = 0
    with (tempfile.TemporaryDirectory() as tmpdirname):
        # Get the UMEP processing parameters and prepare for the method
        method_params, umep_method_title, keepers = _prepare_method_execution(step_method, tiled_city_data, tmpdirname,
                                                                             metadata_logger, met_filename, utc_offset)

        while retry_count < MAX_RETRY_COUNT and return_code != 0:
            try:
                # Run the UMEP plugin!!
                processing.run(umep_method_title, method_params)

                # Prepare target folder and transfer over the temporary results
                try:
                    if step_method == 'umep_solweig_only':
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
        umep_method_title = "umep:Urban Geometry: Wall Height and Aspect"
        keepers[temp_target_wallheight_path] = tiled_city_data.target_wallheight_path
        keepers[temp_target_wallaspect_path] = tiled_city_data.target_wallaspect_path

        # exclude output paths and write remainder to log file
        exclusion_keys = ['OUTPUT_HEIGHT', 'OUTPUT_ASPECT']
        _write_metadata(method, None, method_params, exclusion_keys, metadata_logger)

    elif method == 'skyview_factor':
        create_folder(tiled_city_data.target_intermediate_tile_data_path)

        temp_svfs_file_no_extension = os.path.join(tmpdirname, Path(tiled_city_data.target_svfszip_path).stem)
        skview_factor_plugin_output_default_filename = FILENAME_SVFS_ZIP
        temp_svfs_file_with_extension = os.path.join(tmpdirname, skview_factor_plugin_output_default_filename)
        method_params = {
            'INPUT_DSM': tiled_city_data.target_dsm_path,
            'INPUT_CDSM': tiled_city_data.target_tree_canopy_path,
            'TRANS_VEG': tiled_city_data.light_transmissivity,
            'INPUT_TDSM': None,
            'INPUT_THEIGHT': tiled_city_data.trunk_zone_height,
            'ANISO': True,
            'OUTPUT_DIR': tmpdirname,
            'OUTPUT_FILE': temp_svfs_file_no_extension
        }
        umep_method_title = 'umep:Urban Geometry: Sky View Factor'
        keepers[temp_svfs_file_with_extension] = tiled_city_data.target_svfszip_path

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
            "OUTPUT_TMRT": tiled_city_data.output_tmrt,
            "OUTPUT_KDOWN": False,
            "OUTPUT_KUP": False,
            "OUTPUT_LDOWN": False,
            "OUTPUT_LUP": False,
            "OUTPUT_SH": tiled_city_data.output_sh,
            "OUTPUT_TREEPLANTER": False,
            "OUTPUT_DIR": temp_met_folder
        }
        umep_method_title = 'umep:Outdoor Thermal Comfort: SOLWEIG'

        keepers[temp_met_folder] = target_met_folder

        # exclude output paths and write remainder to log file
        exclusion_keys = ['OUTPUT_DIR']
        _write_metadata(method, met_filename, method_params, exclusion_keys, metadata_logger)

    return method_params, umep_method_title, keepers


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

    from qgis.core import QgsApplication
    QgsApplication.setPrefixPath(qgis_home_path, True)
    qgis_app = QgsApplication([], False)
    qgis_app.initQgis()

    return qgis_app
