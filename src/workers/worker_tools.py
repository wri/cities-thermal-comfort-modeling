import shutil
import os

import yaml
import utm
from datetime import datetime
from shapely.geometry import box
from src.constants import ROOT_DIR


toBool = {'true': True, 'false': False}

def get_utm_zone_epsg(bbox) -> str:
    """
    Get the UTM zone projection for given a bounding box.

    :param bbox: tuple of (min x, min y, max x, max y)
    :return: the EPSG code for the UTM zone of the centroid of the bbox
    """
    centroid = box(*bbox).centroid
    utm_x, utm_y, band, zone = utm.from_latlon(centroid.y, centroid.x)

    if centroid.y > 0:  # Northern zone
        epsg = 32600 + band
    else:
        epsg = 32700 + band

    return f"EPSG:{epsg}"

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


def read_yaml(config_path):
    with open(config_path, 'r') as stream:
        data = yaml.safe_load_all(stream)
        values = list(data)[0]
        b=2
    return values


def write_yaml(data, config_path):
    dir = os.path.dirname(config_path)
    create_folder(dir)
    with open(config_path, 'w') as file:
        yaml.safe_dump(data, file, sort_keys=False)


def read_commented_yaml(config_path):
    from ruamel.yaml import YAML
    yaml = YAML()
    with open(config_path) as stream:
        data = yaml.load(stream)
    return data


def write_commented_yaml(data, config_path):
    from ruamel.yaml import YAML
    yaml = YAML()
    dir = os.path.dirname(config_path)
    create_folder(dir)
    with open(config_path, 'w') as file:
        yaml.dump(data, file)


def get_substring_after(s, delim):
    return s.partition(delim)[2]


def unpack_quoted_value(value):
    return_value = value
    if type(value).__name__ == 'str':
        if value.lower() == 'none':
            return_value = None
        elif value.lower() == 'true':
            return_value = True
        elif value.lower() == 'false':
            return_value = False
        elif value.isnumeric():
            if _is_float_or_integer(value) == 'Integer':
                return_value = int(value)
            elif _is_float_or_integer(value) == 'Float':
                return_value = float(value)

    return return_value


def _is_float_or_integer(s):
    try:
        int(s)
        return "Integer"
    except ValueError:
        try:
            float(s)
            return "Float"
        except ValueError:
            return "Neither"


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

