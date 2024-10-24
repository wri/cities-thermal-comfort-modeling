import os
import tempfile
import time
import shutil
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
from datetime import datetime
from tools import remove_file, create_folder, remove_folder, get_application_path
from qgis_initializer import qgis_app_init
from city_data import CityData

import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

MAX_RETRY_COUNT = 3
RETRY_PAUSE_TIME_SEC = 10

def run_plugin(task_index, step_method, folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path, met_filename=None, utc_offset=None):
    start_time = datetime.now()
    _start_logging(target_base_path, folder_name_city_data)

    city_data = CityData(folder_name_city_data, folder_name_tile_data, source_base_path, target_base_path)
    method_title = _assign_method_title(step_method)
    _log_method_start(method_title, task_index, None, city_data.source_base_path)

    # Initiate QGIS and UMEP processing
    try:
        qgis_app = qgis_app_init()
        from processing_umep.processing_umep import ProcessingUMEPProvider
        umep_provider = ProcessingUMEPProvider()
        qgis_app.processingRegistry().addProvider(umep_provider)
        from processing.core.Processing import Processing
        Processing.initialize()
        import processing
    except Exception as e_msg:
        msg = f'{method_title} processing could not initialize UMEP processing'
        _log_method_failure(start_time, msg, task_index, None, city_data.source_base_path, e_msg)
        return 2

    e_msg = ''
    return_code = -999
    retry_count = 0
    with (tempfile.TemporaryDirectory() as tmpdirname):
        # Get the UMEP processing parameters and prepare for the method
        input_params, umep_method_title, keepers = _prepare_method_execution(step_method, city_data, tmpdirname, met_filename, utc_offset)

        b = 2
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
                    msg = (f'{method_title} processing succeeded but could not create target folder or move files: {city_data.target_preprocessed_data_path}.')
                    _log_method_failure(start_time, msg, task_index, None, city_data.source_base_path, e_msg)
                    return 1

                return_code = 0
            except Exception as e_msg:
                msg = f'task:{task_index} {method_title} failure. Retrying. ({e_msg})'
                _log_method_failure(start_time, msg, task_index, None, city_data.source_base_path, e_msg)
                if retry_count < MAX_RETRY_COUNT:
                    time.sleep(RETRY_PAUSE_TIME_SEC)
                return_code = 3
            retry_count += 1

    if return_code == 0:
        _log_method_completion(start_time, method_title, task_index, None, city_data.source_base_path)
    else:
        msg = f'{method_title} processing cancelled after {MAX_RETRY_COUNT} attempts.'
        _log_method_failure(start_time, msg, task_index, None, city_data.source_base_path, '')

    run_duration_min = _compute_time_diff_mins(start_time)
    return return_code, start_time, run_duration_min

def _start_logging(target_base_path, city_folder_name):
    results_subfolder = CityData.folder_name_results
    log_folder_path = str(os.path.join(target_base_path, city_folder_name, results_subfolder, '.logs'))
    create_folder(log_folder_path)
    log_file_path = os.path.join(log_folder_path, 'execution.log')
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s\t%(levelname)s\t%(message)s',
                        datefmt='%a_%Y_%b_%d_%H:%M:%S',
                        filename=log_file_path
                        )

def _prepare_method_execution(method, city_data, tmpdirname, met_filename=None, utc_offset=None):
    keepers = {}

    if method == 'wall_height_aspect':
        create_folder(city_data.target_preprocessed_data_path)

        temp_target_wallheight_path = os.path.join(tmpdirname, city_data.filename_wall_height)
        temp_target_wallaspect_path = os.path.join(tmpdirname, city_data.filename_wall_aspect)
        input_params = {
            'INPUT': city_data.source_dsm_path,
            'INPUT_LIMIT': city_data.wall_lower_limit_height,
            'OUTPUT_HEIGHT': temp_target_wallheight_path,
            'OUTPUT_ASPECT': temp_target_wallaspect_path,
        }
        umep_method_title = "umep:Urban Geometry: Wall Height and Aspect"
        keepers[temp_target_wallheight_path] = city_data.target_wallheight_path
        keepers[temp_target_wallaspect_path] = city_data.target_wallaspect_path

    elif method == 'skyview_factor':
        create_folder(city_data.target_preprocessed_data_path)

        temp_svfs_file_no_extension = os.path.join(tmpdirname, Path(city_data.target_svfszip_path).stem)
        temp_svfs_file_with_extension = os.path.join(tmpdirname, os.path.basename(city_data.target_svfszip_path))
        input_params = {
            'INPUT_DSM': city_data.source_dsm_path,
            'INPUT_CDSM': city_data.source_veg_canopy_path,
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
        source_met_file_path = os.path.join(city_data.source_met_files_path, met_filename)
        temp_met_folder = os.path.join(tmpdirname, Path(met_filename).stem, city_data.folder_name_tile_data)
        create_folder(temp_met_folder)
        target_met_folder = os.path.join(city_data.target_tcm_results_path, Path(met_filename).stem, city_data.folder_name_tile_data)
        input_params = {
            "INPUT_DSM": city_data.source_dsm_path,
            "INPUT_SVF": city_data.target_svfszip_path,
            "INPUT_HEIGHT": city_data.target_wallheight_path,
            "INPUT_ASPECT": city_data.target_wallaspect_path,
            "INPUT_CDSM": city_data.source_veg_canopy_path,
            "TRANS_VEG": 3,
            "LEAF_START": city_data.leaf_start,
            "LEAF_END": city_data.leaf_end,
            "CONIFER_TREES": city_data.conifer_trees,
            "INPUT_TDSM": None,
            "INPUT_THEIGHT": 25,
            "INPUT_LC": city_data.source_land_cover_path,
            "USE_LC_BUILD": False,
            "INPUT_DEM": city_data.source_dem_path,
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
            "INPUTMET": source_met_file_path,
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


def _log_method_start(method, task_index, step, source_base_path):
    if step is None:
        logging.info(f"task:{task_index}\tStarting '{method}'\tconfig:'{source_base_path}')")
    else:
        logging.info(f"task:{task_index}\tStarting '{method}' for met_series:{step}\tconfig:'{source_base_path}'")


def _log_method_completion(start_time, method, task_index, step, source_base_path):
    runtime = _compute_time_diff_mins(start_time)
    if step is None:
        logging.info(f"task:{task_index}\tFinished '{method}', runtime:{runtime} mins\tconfig:'{source_base_path}'")
    else:
        logging.info(f"task:{task_index}\tFinished '{method}' for met_series:{step}, runtime:{runtime} mins\tconfig:'{source_base_path}'")


def _log_method_failure(start_time, feature, task_index, step, source_base_path, e_msg):
    print('Method failure. See log file.')
    runtime = _compute_time_diff_mins(start_time)
    if step is None:
        logging.error(f"task:{task_index}\t**** FAILED execution of '{feature}' after runtime:{runtime} mins\tconfig:'{source_base_path}'({e_msg})")
    else:
        logging.error(f"task:{task_index}\t**** FAILED execution of '{feature}' fpr met_series:{step} after runtime:{runtime} mins\tconfig:'{source_base_path}'({e_msg})")


def _compute_time_diff_mins(start_time):
    return round(((datetime.now() - start_time).seconds)/60, 1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run specified method in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--task_index', metavar='str', required=True, help='index from the processor config file')
    parser.add_argument('--step_index', metavar='str', required=True, help='index for multi-part runs. only used for reporting')
    valid_methods = ['wall_height_aspect', 'skyview_factor', 'solweig_only']
    parser.add_argument('--step_method', metavar='str', choices=valid_methods, required=True, help='plugin method name')
    parser.add_argument('--folder_name_city_data', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--folder_name_tile_data', metavar='str', required=True, help='name of tile folder')
    parser.add_argument('--source_data_path', metavar='path', required=True, help='folder with source data')
    parser.add_argument('--target_path', metavar='path', required=True, help='folder that is to be populated')

    parser.add_argument('--met_filename', metavar='str', required=False, help='name of the meteorological file')
    parser.add_argument('--utc_offset', metavar='int', required=False, help='local hour offset from utc')
    args = parser.parse_args()

    return_code, start_time, run_duration_min = run_plugin(args.task_index, args.step_method, args.folder_name_city_data, args.folder_name_tile_data,
                             args.source_data_path, args.target_path, args.met_filename, args.utc_offset)

    met_filename_str = args.met_filename if args.met_filename != 'None' else 'N/A'
    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    return_stdout = (f'{{"Return_package": {{"task_index": {args.task_index}, "step_index": {args.step_index}, \
                     "step_method": "{args.step_method}", "met_filename": "{met_filename_str}", "return_code": {return_code}, \
                     "start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}}}')
    print(return_stdout)