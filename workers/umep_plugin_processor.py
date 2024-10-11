# from conda_build.utils import prepend_bin_path
import os
import tempfile
import shutil
import warnings
from pathlib import Path
from datetime import datetime
from tools import remove_file, create_folder, remove_folder
from qgis_initializer import qgis_app_init
from city_data import instantiate_city_data

import logging

from workers.tools import clean_folder

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
log_file_path = os.path.join(os.path.dirname(os.getcwd()), 'logs', 'execution.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s\t%(levelname)s\t%(message)s',
                    datefmt='%a_%Y_%b_%d_%H:%M:%S',
                    filename=log_file_path
                    )

MAX_RETRY_COUNT = 3


def run_plugin(task_id, method, city_folder_name, tile_folder_name, source_data_path, target_path, met_file_name=None, utc_offset=None):
    city_data = instantiate_city_data(city_folder_name, tile_folder_name, source_data_path, target_path)
    method_title = _get_method_title(method)

    # TODO - Add checks for prerequite data!!
    start_time = datetime.now()
    log_method_start(method_title, task_id, None, city_data.source_base_path)

    # Initiate QGIS and UMEP processing
    try:
        qgis_app = qgis_app_init()
        from processing_umep.processing_umep import ProcessingUMEPProvider
        umep_provider = ProcessingUMEPProvider()
        qgis_app.processingRegistry().addProvider(umep_provider)
        from processing.core.Processing import Processing
        Processing.initialize()
        import processing
    except:
        msg = '%s processing could not initalize UMEP processing' % method_title
        log_method_failure(start_time, msg, task_id, None, city_data.source_base_path, '')
        return 2

    e_msg = ''
    return_code = -999
    retry_count = 0
    warnings.filterwarnings("ignore")
    with (tempfile.TemporaryDirectory() as tmpdirname):
        # Get the UMEP processing parameters and prepare for the method
        input_params, umep_method_title, keepers = _prepare_method_execution(method, city_data, tmpdirname, met_file_name, utc_offset)

        while retry_count < MAX_RETRY_COUNT and return_code != 0:
            try:
                # Run the UMEP plugin!!
                processing.run(umep_method_title, input_params)

                # Prepare target folder and transfer over the temporary results
                try:
                    if method == 'solweig':
                        create_folder(city_data.target_tcm_results_path)
                        for temp_result_path, target_path in keepers.items():
                            remove_file(target_path)
                            shutil.move(str(temp_result_path), str(target_path))
                    else:
                        create_folder(city_data.target_preprocessed_data_path)
                        for temp_result_path, target_path in keepers.items():
                            clean_folder(target_path)
                            shutil.move(str(temp_result_path), str(target_path))
                except Exception as e_msg:
                    msg = ('%s processing succeeded but could not create target folder or move files: %s.' %
                           (method_title, city_data.target_preprocessed_data_path))
                    log_method_failure(start_time, msg, task_id, None, city_data.source_base_path, e_msg)
                    return 1

                return_code = 0
            except Exception as e_msg:
                msg = 'task_id:%s %s failure. Retrying. %s' % (task_id, method_title, e_msg)
                log_method_failure(start_time, msg, task_id, None, city_data.source_base_path, e_msg)
                return_code = 3
            retry_count += 1

    if return_code == 0:
        log_method_completion(start_time, method_title, task_id, None, city_data.source_base_path)
    else:
        msg = '%s processing cancelled after %s attempts.' % (method_title, MAX_RETRY_COUNT)
        log_method_failure(start_time, msg, task_id, None, city_data.source_base_path, '')

    return return_code


def log_method_start(method, task_id, step, source_base_path):
    if step is None:
        logging.info("task_id:%s\tStarting '%s'\tconfig:'%s')" % (task_id, method, source_base_path))
    else:
        logging.info("task_id:%s\tStarting '%s' for met_series:%s\tconfig:'%s'" % (task_id, method, step, source_base_path))

def log_method_completion(start_time, method, task_id, step, source_base_path):
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.info("task_id:%s\tFinished '%s', runtime:%s mins\tconfig:'%s'" % (task_id, method, runtime, source_base_path))
    else:
        logging.info("task_id:%s\tFinished '%s' for met_series:%s, runtime:%s mins\tconfig:'%s'" % (task_id, method, step, runtime, source_base_path))


def log_method_failure(start_time, feature, task_id, step, source_base_path, e_msg):
    print('Method failure. See log file.')
    runtime = round((datetime.now() - start_time).seconds/60,2)
    if step is None:
        logging.error("task_id:%s\t**** FAILED execution of '%s' after runtime:%s mins\tconfig:'%s'(%s)" % (task_id, feature, runtime, source_base_path, e_msg))
    else:
        logging.error("task_id:%s\t**** FAILED execution of '%s' fpr met_series:%s after runtime:%s mins\tconfig:'%s'(%s)" % (task_id, feature, step, runtime, source_base_path, e_msg))


def _get_method_title(method):
    if method == 'wall_height_aspect':
        method_title = 'Wall Height/Aspect'
    elif method == 'skyview_factor':
        method_title = 'Skyview Factor'
    else:
        method_title = 'SOLWEIG'
    return method_title


def _prepare_method_execution(method, city_data, tmpdirname, met_file_name=None, utc_offset=None):
    keepers = {}

    if method == 'wall_height_aspect':
        temp_wallheight_path = os.path.join(tmpdirname, city_data.wall_height_file)
        temp_wallaspect_path = os.path.join(tmpdirname, city_data.wall_aspect_file)
        input_params = {
            'INPUT': city_data.dsm_path,
            'INPUT_LIMIT': city_data.wall_lower_limit_height,
            'OUTPUT_HEIGHT': temp_wallheight_path,
            'OUTPUT_ASPECT': temp_wallaspect_path,
        }
        umep_method_title = "umep:Urban Geometry: Wall Height and Aspect"
        keepers[temp_wallheight_path] = city_data.wallheight_path
        keepers[temp_wallaspect_path] = city_data.wallaspect_path

    elif method == 'skyview_factor':
        temp_svfs_file_no_extension = os.path.join(tmpdirname, Path(city_data.svfszip_path).stem)
        temp_svfs_file_with_extension = os.path.join(tmpdirname, os.path.basename(city_data.svfszip_path))
        input_params = {
            'INPUT_DSM': city_data.dsm_path,
            'INPUT_CDSM': city_data.vegcanopy_path,
            'TRANS_VEG': 3,
            'INPUT_TDSM': None,
            'INPUT_THEIGHT': 25,
            'ANISO': True,
            'OUTPUT_DIR': tmpdirname,
            'OUTPUT_FILE': temp_svfs_file_no_extension
        }
        umep_method_title = 'umep:Urban Geometry: Sky View Factor'
        keepers[temp_svfs_file_with_extension] = city_data.svfszip_path
    else:
        source_met_file_path = os.path.join(city_data.source_met_files_path, met_file_name)
        temp_met_folder = os.path.join(tmpdirname, Path(met_file_name).stem,
                                         city_data.tile_folder_name)
        create_folder(temp_met_folder)
        input_params = {
            "INPUT_DSM": city_data.dsm_path,
            "INPUT_SVF": city_data.svfszip_path,
            "INPUT_HEIGHT": city_data.wallheight_path,
            "INPUT_ASPECT": city_data.wallaspect_path,
            "INPUT_CDSM": city_data.vegcanopy_path,
            "TRANS_VEG": 3,
            "LEAF_START": city_data.leaf_start,
            "LEAF_END": city_data.leaf_end,
            "CONIFER_TREES": city_data.conifer_trees,
            "INPUT_TDSM": None,
            "INPUT_THEIGHT": 25,
            "INPUT_LC": city_data.landcover_path,
            "USE_LC_BUILD": False,
            "INPUT_DEM": city_data.dem_path,
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
        target_met_folder = os.path.join(city_data.target_tcm_results_path, Path(met_file_name).stem,
                                         city_data.tile_folder_name)

        keepers[temp_met_folder] = target_met_folder

    return input_params, umep_method_title, keepers

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run specified method in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--task_id', metavar='str', required=True, help='blah')
    valid_methods = ['wall_height_aspect', 'skyview_factor', 'solweig']
    parser.add_argument('--method', metavar='str', choices=valid_methods, required=True, help='blah')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='the city name')
    parser.add_argument('--tile_folder_name', metavar='str', required=True, help='the tile name')
    parser.add_argument('--source_data_path', metavar='str', required=True, help='blah')
    parser.add_argument('--target_path', metavar='str', required=True, help='blah')

    parser.add_argument('--met_file_name', metavar='str', required=False, help='blah')
    parser.add_argument('--utc_offset', metavar='str', required=False, help='blah')
    args = parser.parse_args()



    return_code = run_plugin(args.task_id, args.method, args.city_folder_name, args.tile_folder_name,
                             args.source_data_path, args.target_path, args.met_file_name, args.utc_offset)
    return_code2 = 555
    print(return_code2)
