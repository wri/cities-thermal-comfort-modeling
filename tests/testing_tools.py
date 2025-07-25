import os
import subprocess

import numpy as np

from src.constants import DATA_DIR, ROOT_DIR

SAMPLE_CITIES_SOURCE_DIR = str(os.path.join(DATA_DIR, 'sample_cities'))
SCRATCH_TARGET_DIR = str(os.path.join(DATA_DIR, 'scratch_target'))


def is_valid_output_directory(path):
    is_valid = True if os.path.isdir(path) and len(os.listdir(path)) > 0 else False
    return is_valid

def is_valid_output_file(file_path):
    is_valid = True if os.path.isfile(file_path) and os.path.getsize(file_path) > 0 else False
    return is_valid


def run_main(target_base_path:str, source_city_folder_name:str, processing_option:str):
    command = ['python', os.path.join(ROOT_DIR, 'main.py'),
               '--source_base_path', SAMPLE_CITIES_SOURCE_DIR,
               '--target_base_path', target_base_path,
               '--city_folder_name', source_city_folder_name,
               '--processing_option', processing_option]
    proc_results = subprocess.run(command, capture_output=True, text=True)
    return_code = proc_results.returncode
    return return_code


def file_count_in_vrt_directory(non_tiled_city_data):
    vrt_dir = non_tiled_city_data.target_qgis_data_path
    lst = os.listdir(vrt_dir)
    number_files = len(lst)
    return number_files

def compare_raster_data(file1, file2):
    import rasterio
    with rasterio.open(file1) as src1, rasterio.open(file2) as src2:
        if src1.shape != src2.shape:
            return False, None  # Different dimensions
        if src1.count != src2.count:
            return False, None  # Different number of bands
        for band in range(1, src1.count + 1):
            data1 = src1.read(band)
            data2 = src2.read(band)
            if not np.array_equal(data1, data2):
                diff = (data1 - data2)

                count_non_zero = np.count_nonzero(diff)

                rounded_arr = np.round(diff, 2)
                rounded_count_non_zero = np.count_nonzero(rounded_arr)

                return False, count_non_zero, rounded_count_non_zero  # Pixel data differs
    return True, 0, 0


