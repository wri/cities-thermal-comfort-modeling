import shutil
import os
import configparser
from pathlib import Path
import rasterio
import shapely
from pyproj import Transformer

from workers.city_data import CityData


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


def get_existing_tiles(source_city_path, custom_file_names, start_tile_id, end_tile_id):
    tiles_folders = str(os.path.join(source_city_path, CityData.folder_name_source_data, CityData.folder_name_primary_source_data))

    tile_sizes = {}
    for dir_obj in Path(tiles_folders).iterdir():
        if dir_obj.is_dir() and os.path.basename(dir_obj).startswith('tile_'):
            tile_path = os.path.join(tiles_folders, dir_obj)
            tile_name = os.path.basename(tile_path)

            if start_tile_id == '*' or end_tile_id == '*':
                tile_range = []
            else:
                tile_range = _get_tile_range(start_tile_id, end_tile_id)

            if not tile_range or tile_name in tile_range:
                for file_obj in Path(tile_path).iterdir():
                    if file_obj.name in custom_file_names and file_obj.is_file() and Path(file_obj).suffix == '.tif':
                        # get bounds for first tiff file found in folder, assuming all other geotiffs have same bounds
                        tile_boundary, avg_res = _get_geobounds_of_geotiff_file(file_obj)
                        tile_sizes[tile_name] = [tile_boundary, avg_res]
                        break
        continue

    return tile_sizes


def _get_tile_range(start_tile_id, end_tile_id):
    t_len = len('tile_')
    start_id = int(start_tile_id[t_len:])
    end_id = int(end_tile_id[t_len:])
    tile_range = []
    for x in range(start_id, end_id+1):
        pad_x = str(x).zfill(3)
        tile_name = f'tile_{pad_x}'
        tile_range.append(tile_name)
    return tile_range


def _get_geobounds_of_geotiff_file(file_path):
    with rasterio.open(file_path) as dataset:
        bounds = dataset.bounds
        min_x = bounds.left
        min_y = bounds.bottom
        max_x = bounds.right
        max_y = bounds.top

        source_crs = dataset.crs.data.get('init')
        if source_crs != 'epsg:4326':
            transformer = Transformer.from_crs(source_crs, "EPSG:4326")
            sw_coord = transformer.transform(min_x, min_y)
            ne_coord = transformer.transform(max_x, max_y)
            tile_boundary = coordinates_to_bbox(sw_coord[1], sw_coord[0], ne_coord[1], ne_coord[0])
        else:
            tile_boundary = coordinates_to_bbox(min_x, min_y, max_x, max_y)

        avg_res = int(round((dataset.res[0] + dataset.res[1])/2, 0))
        return tile_boundary, avg_res


def coordinates_to_bbox(min_x, min_y, max_x, max_y):
    tile_boundary = shapely.box(min_x, min_y, max_x, max_y)
    return tile_boundary


def get_substring_between_chars(string, start_char, end_char):
    start_index = string.find(start_char) + 1
    end_index = string.find(end_char, start_index)

    if start_index == 0 or end_index == -1:
        return ""

    return string[start_index:end_index]