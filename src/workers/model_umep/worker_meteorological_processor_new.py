import shapely.wkt
import geopandas as gp
import pandas as pd
import os
import numpy as np

from datetime import datetime

from city_metrix.metrics import Era5MetPreprocessing

from src.constants import FOLDER_NAME_PRIMARY_DATA, FOLDER_NAME_PRIMARY_MET_FILES, FILENAME_ERA5
from src.workers.worker_tools import compute_time_diff_mins, remove_file, create_folder

MET_NULL_VALUE = -999
TARGET_HEADING =  '%iy id it imin qn qh qe qs qf U RH Tair press rain kdown snow ldown fcld wuh xsmd lai kdiff kdir wdir'

def get_met_data(target_met_files_path, aoi_boundary_poly, utc_offset, sampling_local_hours):
    start_time = datetime.now()

    # Create a GeoDataFrame with the polygon
    aoi_gdf = gp.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[aoi_boundary_poly])

    # Retrieve and write ERA5 data
    return_code = _get_era5(aoi_gdf, target_met_files_path, utc_offset, sampling_local_hours)

    return return_code


def _get_era5(aoi_gdf, target_met_files_path, utc_offset, sampling_local_hours):
    # Attempt to download data with up to 3 tries
    count = 0
    aoi_era_5 = None
    era5_failure_msg = ''
    while count < 3:
        try:
            aoi_era_5 = Era5MetPreprocessing().get_data(aoi_gdf)
            break
        except Exception as e_msg:
            era5_failure_msg = e_msg
            count +=1

    if aoi_era_5 is None:
        raise Exception(f'Failed to retrieve era5 data with error: {era5_failure_msg}')

    # round all numbers to two decimal places, which is the precision needed by the umep model
    aoi_era_5 = aoi_era_5.round(2)

    # adjust for utc
    int_utc_offset = int(utc_offset)
    aoi_era_5['local_time'] = aoi_era_5['time'] + pd.Timedelta(hours=int_utc_offset)

    # filter to specific hours of day
    filter_hours = [int(x) for x in sampling_local_hours.split(',')]
    filtered_era_5 = aoi_era_5[aoi_era_5['local_time'].dt.hour.isin(filter_hours)]

    # order by datetime
    filtered_era_5.sort_values(by='time', inplace=True, ascending=True)
    
    # Reformat into target format
    reformatted_data = []
    reformatted_data.append(TARGET_HEADING)
    for index, row in filtered_era_5.iterrows():
        reformatted_data.append(_reformat_line(row))

    # Write results to text file
    target_met_file = os.path.join(target_met_files_path, FILENAME_ERA5)
    create_folder(target_met_files_path)
    remove_file(target_met_file)
    with open(target_met_file, 'w') as file:
        for row in reformatted_data:
            file.write('%s\n' % row)

    return 0


def _reformat_line(line):
    local_datetime = line['local_time']
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
    U = _standardize_string(wind) # wind  (ERA5 10u, 10m_u_component_of_wind)
    RH = _standardize_string(rh) # rh
    Tair = _standardize_string(temp) # temp
    press = MET_NULL_VALUE
    rain = MET_NULL_VALUE
    kdown = _standardize_string(global_rad) # sw rad
    snow = MET_NULL_VALUE
    ldown = _standardize_string(diffuse_rad) # lw rad
    fcld = MET_NULL_VALUE
    wuh = MET_NULL_VALUE
    xsmd = MET_NULL_VALUE
    lai = MET_NULL_VALUE
    kdiff = MET_NULL_VALUE
    kdir = _standardize_string(direct_rad) # sw rad
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
    if value == '' or value == 'nan' or np.isnan(value):
        str_value = MET_NULL_VALUE
    else:
        str_value = f"{value:.2f}"
    return str_value


def _day_of_year(date_time):
    return (date_time - datetime(date_time.year, 1, 1)).days + 1

