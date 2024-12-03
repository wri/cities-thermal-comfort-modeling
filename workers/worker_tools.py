import shutil
import os
import logging
from datetime import datetime
from pathlib import Path


def initialize_scratch_folder(folder_path):
    if os.path.isdir(folder_path):
        clean_folder(folder_path)
    else:
        create_folder(folder_path)

def create_folder(folder_path):
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

def clean_folder(folder_path):
    if os.path.isdir(folder_path):
        # Iterate over all the files and subdirectories in the directory
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                # Check if it is a file or a directory
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)  # Remove the file or link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Remove the directory and its contents
            except Exception as e_msg:
                print(f'Failed to delete {file_path}. Reason: {e_msg}')

def remove_folder(folder_path):
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)

def remove_file(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)

def get_configurations():
    import configparser

    application_path = get_application_path()
    config_file = os.path.join(application_path, '.config.ini')

    config = configparser.ConfigParser()
    config.read(config_file)
    qgis_home_path = config['Resources']['qgis_home_path']
    qgis_plugin_path = config['Resources']['qgis_plugin_path']

    return qgis_home_path, qgis_plugin_path

def get_application_path():
    return str(Path(os.path.dirname(os.path.abspath(__file__))).parent)

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


def list_files_with_extension(directory, extension):
    return [f for f in os.listdir(directory) if f.endswith(extension)]


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

