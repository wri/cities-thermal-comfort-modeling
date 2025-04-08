import os
import shutil
import rasterio
import shapely
import hashlib
import pandas as pd

from pyproj import Transformer, CRS
from shapely.geometry import Polygon
from pyproj import Geod
from pathlib import Path

from src.constants import FOLDER_NAME_PRIMARY_RASTER_FILES, FOLDER_NAME_PRIMARY_DATA, WGS_CRS


def get_existing_tile_metrics(source_city_path, custom_file_names, include_extended_metrics = False):
    tiles_folders = str(os.path.join(source_city_path, FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_RASTER_FILES))

    if include_extended_metrics:
        columns = ['tile_name', 'primary_file', 'checksum']
    else:
        columns = ['tile_name', 'primary_file', 'boundary', 'avg_res', 'cell_count', 'source_crs']

    tile_metrics_df = pd.DataFrame(columns=columns)

    if os.path.isdir(tiles_folders):
        for dir_obj in Path(tiles_folders).iterdir():
            if dir_obj.is_dir() and os.path.basename(dir_obj).startswith('tile_'):
                tile_path = os.path.join(tiles_folders, dir_obj)
                tile_name = os.path.basename(tile_path)

                for file_obj in Path(tile_path).iterdir():
                    if file_obj.name in custom_file_names and file_obj.is_file() and Path(file_obj).suffix == '.tif':
                        # get bounds for first tiff file found in folder, assuming all other geotiffs have same bounds
                        file_stem = Path(file_obj.name).stem

                        if include_extended_metrics:
                            chksum = calculate_checksum(file_obj)
                            new_row = {'tile_name': tile_name, 'primary_file': file_stem,
                                       'checksum': chksum}
                            tile_metrics_df = tile_metrics_df._append(new_row, ignore_index=True)
                        else:
                            tile_boundary, avg_res, resolution_x, resolution_y, cell_count, source_crs = (
                                _get_spatial_dimensions_of_geotiff_file(file_obj))
                            new_row = {'tile_name': tile_name, 'primary_file': file_stem, 'boundary': tile_boundary,
                                       'avg_res': avg_res, 'resolution_x': resolution_x, 'resolution_y': resolution_y,
                                       'cell_count': cell_count, 'source_crs': source_crs}

                            tile_metrics_df = tile_metrics_df._append(new_row, ignore_index=True)
                            import geopandas as gpd
                            tile_metrics_df = gpd.GeoDataFrame(tile_metrics_df, geometry="boundary", crs=source_crs)

    return tile_metrics_df


def _get_spatial_dimensions_of_geotiff_file(file_path):
    with rasterio.open(file_path) as dataset:
        bounds = dataset.bounds
        min_x = bounds.left
        min_y = bounds.bottom
        max_x = bounds.right
        max_y = bounds.top
        height = dataset.height
        width = dataset.width

        source_epsg = CRS.from_wkt(dataset.crs.wkt).to_epsg()
        source_crs = f'EPSG:{source_epsg}'
        tile_boundary = coordinates_to_bbox(min_x, min_y, max_x, max_y)
        # tile_boundary = (min_x, min_y, max_x, max_y)

        resolution_x = dataset.res[0]
        resolution_y = dataset.res[1]

        avg_res = int(round((dataset.res[0] + dataset.res[1])/2, 0))
        cell_count = width * height

    return tile_boundary, avg_res, resolution_x, resolution_y, cell_count, source_crs


def coordinates_to_bbox(min_x, min_y, max_x, max_y):
    tile_boundary = shapely.box(min_x, min_y, max_x, max_y)
    return tile_boundary


def get_aoi_area_in_square_meters(min_lon, min_lat, max_lon, max_lat):
    bbox_poly = construct_polygon_from_bounds(min_lon, min_lat, max_lon, max_lat)

    geod = Geod(ellps="WGS84")
    
    # Calculate the area in square meters
    area, _ = geod.geometry_area_perimeter(bbox_poly)

    return abs(area)


def construct_polygon_from_bounds(minx, miny, maxx, maxy):
    bbox = [(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]
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
