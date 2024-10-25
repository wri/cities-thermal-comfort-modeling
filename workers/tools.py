import shutil
import os
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

def save_raster_file(raster_data_array, tile_data_path, tiff_data_FILENAME):
    create_folder(tile_data_path)
    file_path = os.path.join(tile_data_path, f'{tiff_data_FILENAME}.tif')
    remove_file(file_path)
    raster_data_array.rio.to_raster(raster_path=file_path, driver="COG")


def save_vector_file(vector_geodataframe, tile_data_path, tiff_data_FILENAME):
    create_folder(tile_data_path)
    file_path = os.path.join(tile_data_path, f'{tiff_data_FILENAME}.geojson')
    remove_file(file_path)
    vector_geodataframe.to_file(file_path, driver='GeoJSON')


def read_tiff_file(tile_data_path, filename):
    import rioxarray
    file_path = os.path.join(tile_data_path, f'{filename}.tif')
    raster_data = rioxarray.open_rasterio(file_path)
    return raster_data

def read_vector_file(tile_data_path, filename):
    import geopandas as gpd
    file_path = os.path.join(tile_data_path, f'{filename}.geojson')
    vector_data = gpd.read_file(file_path)
    return vector_data