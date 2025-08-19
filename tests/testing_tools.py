import os
import subprocess
import numpy as np
import rasterio
import hashlib

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

def does_file_signature_match(expected_signature, file):
    file_signature = get_geotiff_signature(file)
    return True if file_signature == expected_signature else False


def get_geotiff_signature(file_path):
    with rasterio.open(file_path) as src:
        # Extract metadata
        meta = {
            'crs': src.crs.to_string() if src.crs else None,
            'transform': tuple(src.transform),
            'width': src.width,
            'height': src.height,
            'count': src.count,
            'dtype': src.dtypes,
            'band_checksums': []
        }

        # Compute checksum for each band
        for i in range(1, src.count + 1):
            band = src.read(i)
            checksum = hashlib.md5(band.tobytes()).hexdigest()
            meta['band_checksums'].append(checksum)

        # Optional: checksum of entire raster stack
        all_data = src.read()
        meta['full_checksum'] = hashlib.md5(all_data.tobytes()).hexdigest()

    return meta

