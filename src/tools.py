import shutil
import os
import configparser
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
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

def verify_path(path):
    is_valid = os.path.exists(path)
    return is_valid

def remove_folder(folder_path):
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)

def remove_file(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)

def get_configurations():
    application_path = get_application_path()
    config_file = os.path.join(application_path, 'config.ini')

    config = configparser.ConfigParser()
    config.read(config_file)
    qgis_home_path = config['Resources']['qgis_home_path']
    qgis_plugin_path = config['Resources']['qgis_plugin_path']

    return qgis_home_path, qgis_plugin_path

def get_application_path():
    return Path(os.path.dirname(os.path.abspath(__file__))).parent

toBool = {'true': True, 'false': False}

