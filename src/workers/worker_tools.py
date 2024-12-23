import shutil
import os
import logging
from datetime import datetime
from pathlib import Path

from src.constants import ROOT_DIR


def create_folder(folder_path):
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

def remove_folder(folder_path):
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)

def remove_file(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)

def get_configurations():
    import configparser
    config_file = os.path.join(ROOT_DIR, '.config.ini')

    config = configparser.ConfigParser()
    config.read(config_file)
    qgis_home_path = config['Resources']['qgis_home_path']
    qgis_plugin_path = config['Resources']['qgis_plugin_path']

    return qgis_home_path, qgis_plugin_path

toBool = {'true': True, 'false': False}


def save_tiff_file(raster_data_array, tile_data_path, tiff_filename):
    create_folder(tile_data_path)
    file_path = os.path.join(tile_data_path, tiff_filename)
    remove_file(file_path)
    try:
        raster_data_array.rio.to_raster(raster_path=file_path, driver="COG")
    except Exception as e_msg:
        raise Exception(f'GeoTiff file {tiff_filename} not written to {tile_data_path}.')


def save_geojson_file(vector_geodataframe, tile_data_path, tiff_data_FILENAME):
    create_folder(tile_data_path)
    file_path = os.path.join(tile_data_path, tiff_data_FILENAME)
    remove_file(file_path)
    vector_geodataframe.to_file(file_path, driver='GeoJSON')


def compute_time_diff_mins(start_time):
    return round(((datetime.now() - start_time).seconds)/60, 1)


def reverse_y_dimension_as_needed(dataarray):
    was_reversed= False
    y_dimensions = dataarray.shape[0]
    if dataarray.y.data[0] < dataarray.y.data[y_dimensions - 1]:
        dataarray = dataarray.isel({dataarray.rio.y_dim: slice(None, None, -1)})
        was_reversed = True
    return was_reversed, dataarray


def log_method_start(method, task_index, step, source_base_path):
    if step is None:
        logging.info(f"task:{task_index}\tStarting '{method}'\tconfig:'{source_base_path}')")
    else:
        logging.info(f"task:{task_index}\tStarting '{method}' for met_series:{step}\tconfig:'{source_base_path}'")


def log_method_completion(start_time, method, task_index, step, source_base_path):
    runtime = compute_time_diff_mins(start_time)
    if step is None:
        logging.info(f"task:{task_index}\tFinished '{method}', runtime:{runtime} mins\tconfig:'{source_base_path}'")
    else:
        logging.info(f"task:{task_index}\tFinished '{method}' for met_series:{step}, runtime:{runtime} mins\tconfig:'{source_base_path}'")


def log_method_failure(start_time, feature, task_index, step, source_base_path, e_msg):
    print('Method failure. See log file.')
    runtime = compute_time_diff_mins(start_time)
    if step is None:
        logging.error(f"task:{task_index}\t**** FAILED execution of '{feature}' after runtime:{runtime} mins\tconfig:'{source_base_path}'({e_msg})")
    else:
        logging.error(f"task:{task_index}\t**** FAILED execution of '{feature}' fpr met_series:{step} after runtime:{runtime} mins\tconfig:'{source_base_path}'({e_msg})")


def _start_logging(target_base_path, city_folder_name):
    log_folder_path = str(os.path.join(target_base_path, city_folder_name, '.logs'))
    create_folder(log_folder_path)
    log_file_path = os.path.join(log_folder_path, 'model_execution.log')
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s\t%(levelname)s\t%(message)s',
                        datefmt='%a_%Y_%b_%d_%H:%M:%S',
                        filename=log_file_path
                        )
