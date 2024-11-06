import xarray as xr
from rasterio.enums import Resampling
from dask.diagnostics import ProgressBar
import shapely.wkt
import geopandas as gp
import numpy as np
import random
import time
import os

from pathlib import Path
from datetime import datetime, timedelta
from city_data import CityData
from worker_tools import compute_time_diff_mins, save_tiff_file, save_geojson_file, log_method_failure
from workers.worker_tools import remove_file

MET_NULL_VALUE = -999
TARGET_HEADING =  '%iy id it imin qn qh qe qs qf U RH Tair press rain kdown snow ldown fcld wuh xsmd lai kdiff kdir wdir'

# ERA_MET_FILE_NAME = 'met_era5_hottest_days.txt'

def get_met_data(task_index, source_base_path, folder_name_city_data, aoi_boundary):
    start_time = datetime.now()

    d = {'geometry': [shapely.wkt.loads(aoi_boundary)]}
    aoi_gdf = gp.GeoDataFrame(d, crs="EPSG:4326")

    result_flags = []
    _get_era5(aoi_gdf, source_base_path, folder_name_city_data)

    step_method = 'met'
    met_filename = CityData.filename_era5
    return_code = 0 if all(result_flags) else 1
    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    run_duration_min = compute_time_diff_mins(start_time)
    return_stdout = (f'{{"task_index": {task_index}, "tile": "None", "step_index": {0}, '
                     f'"step_method": "{step_method}", "met_filename": "{met_filename}", "return_code": {return_code}, '
                     f'"start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}')

    result_json = f'{{"Return_package": [{return_stdout}]}}'

    return result_json


def _get_era5(aoi_gdf, output_base_path, folder_name_city_data):
    from city_metrix.metrics import era_5_met_preprocessing

    max_count = 3
    count = 0
    aoi_era_5 = None
    while count < max_count:
        try:
            aoi_era_5 = era_5_met_preprocessing(aoi_gdf)
            break
        except:
            count +=1

    if aoi_era_5 is None:
        raise Exception('failed to retrieve era5 data')

    utc_offset = 2
    met_filename = CityData.filename_era5
    target_file_path = os.path.join(output_base_path, folder_name_city_data, CityData.folder_name_source_data, CityData.folder_name_met_files, met_filename)


    reformatted_data = []
    reformatted_data.append(TARGET_HEADING)
    for index, row in aoi_era_5.iterrows():
        reformatted_data.append(_reformat_line(row, utc_offset))


    remove_file(target_file_path)
    with open(target_file_path, 'w') as file:
        for row in reformatted_data:
            file.write('%s\n' % row)

    b=2

def _reformat_line(line, utc_offset):
    local_datetime = line['time']  # , utc_offset)
    lat = line['lat']
    lon = line['lon']
    temp = line['temp']
    rh = line['rh']
    global_rad = line['global_rad']
    direct_rad = line['direct_rad']
    diffuse_rad = line['diffuse_rad']
    water_temp = line['water_temp']
    wind = line['wind']
    vpd = line['vpd']

    # remapped titles
    iy = local_datetime.year
    id = _day_of_year(local_datetime)
    it = local_datetime.hour
    imin = local_datetime.minute
    qn = MET_NULL_VALUE
    qh = MET_NULL_VALUE
    qe = MET_NULL_VALUE
    qs = MET_NULL_VALUE
    qf = MET_NULL_VALUE
    U = _standardize_string(wind)
    RH = _standardize_string(rh)
    Tair = _standardize_string(temp)
    press = MET_NULL_VALUE
    rain = MET_NULL_VALUE
    kdown = _standardize_string(global_rad)
    snow = MET_NULL_VALUE
    ldown = _standardize_string(diffuse_rad)
    fcld = MET_NULL_VALUE
    wuh = MET_NULL_VALUE
    xsmd = MET_NULL_VALUE
    lai = MET_NULL_VALUE
    kdiff = MET_NULL_VALUE
    kdir = _standardize_string(direct_rad)
    wdir = MET_NULL_VALUE

    new_line = ''
    new_line += '%s %s %s %s' % (iy, id, it, imin)
    new_line += ' %s %s %s %s %s' % (qn, qh, qe, qs, qf)
    new_line += ' %s %s %s' % (U, RH, Tair)
    new_line += ' %s %s' % (press, rain)
    new_line += ' %s %s %s' % (kdown, snow, ldown)
    new_line += ' %s %s %s %s %s' % (fcld, wuh, xsmd, lai, kdiff)
    new_line += ' %s %s' % (kdir, wdir)

    return new_line

def _standardize_string(value):
    return MET_NULL_VALUE if value == '' else value

def _day_of_year(date_time):
    return (date_time - datetime(date_time.year, 1, 1)).days + 1

def _string_to_datetime(utc_datetime_string, utc_offset):
    utc_datetime = datetime.strptime(utc_datetime_string, '%Y-%m-%d %H:%M:%S')
    local_time = utc_datetime + timedelta(hours=utc_offset)
    return local_time



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Retrieve meteorological data.')
    parser.add_argument('--task_index', metavar='str', required=True, help='index from the processor config file')
    parser.add_argument('--source_base_path', metavar='path', required=True, help='folder for source data')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--aoi_boundary', metavar='str', required=True, help='geographic boundary of the AOI')

    args = parser.parse_args()

    result_json = get_met_data(args.task_index, args.source_base_path, args.city_folder_name, args.aoi_boundary)

    print(result_json)