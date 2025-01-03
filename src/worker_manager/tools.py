import os
import shutil
import rasterio
import shapely
import hashlib
import pandas as pd

from pyproj import Transformer
from shapely.geometry import Polygon
from pyproj import Geod
from pathlib import Path

from src.constants import FOLDER_NAME_PRIMARY_RASTER_FILES, FOLDER_NAME_PRIMARY_DATA


def get_existing_tiles(source_city_path, custom_file_names, start_tile_id, end_tile_id, include_extended_metrics = False):
    tiles_folders = str(os.path.join(source_city_path, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES))

    if include_extended_metrics:
        columns = ['tile_name', 'primary_file', 'checksum']
    else:
        columns = ['tile_name', 'primary_file', 'boundary', 'avg_res', 'source_crs']

    tile_metrics_df = pd.DataFrame(columns=columns)

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
                        file_stem = Path(file_obj.name).stem

                        if include_extended_metrics:
                            chksum = calculate_checksum(file_obj)
                            new_row = {'tile_name': tile_name, 'primary_file': file_stem,
                                       'checksum': chksum}
                        else:
                            tile_boundary, avg_res, source_crs = _get_spatial_dimensions_of_geotiff_file(file_obj)
                            new_row = {'tile_name': tile_name, 'primary_file': file_stem,
                                       'boundary': tile_boundary, 'avg_res': avg_res, 'source_crs': source_crs}

                        tile_metrics_df = tile_metrics_df._append(new_row, ignore_index=True)

    return tile_metrics_df


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


def _get_spatial_dimensions_of_geotiff_file(file_path):
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

            # TODO Use below code after fixing CIF-321
            # tile_boundary = coordinates_to_bbox(min_x, min_y, max_x, max_y)
            # TODO USe above code after fixing CIF-321
        else:
            tile_boundary = bounds

        avg_res = int(round((dataset.res[0] + dataset.res[1])/2, 0))

    return tile_boundary, avg_res, source_crs


def coordinates_to_bbox(min_x, min_y, max_x, max_y):
    tile_boundary = shapely.box(min_x, min_y, max_x, max_y)
    return tile_boundary


def get_aoi_area_in_square_meters(min_lon, min_lat, max_lon, max_lat):
    bbox_poly = construct_polygon_from_bounds(min_lon, min_lat, max_lon, max_lat)

    geod = Geod(ellps="WGS84")
    
    # Calculate the area in square meters
    area, _ = geod.geometry_area_perimeter(bbox_poly)

    return abs(area)


def construct_polygon_from_bounds(min_lon, min_lat, max_lon, max_lat):
    bbox = [(min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat), (max_lon, min_lat)]
    bbox_poly = Polygon(bbox)
    return bbox_poly

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

def delete_files_with_extension(root_folder, extension):
    for foldername, subfolders, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith(extension):
                file_path = os.path.join(foldername, filename)
                try:
                    os.remove(file_path)
                except Exception as e_msg:
                    print(f"Error deleting {file_path}: {e_msg}")


def list_files_with_extension(directory, extension):
    return [f for f in os.listdir(directory) if f.endswith(extension)]


def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
