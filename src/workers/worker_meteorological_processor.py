import shapely.wkt
import geopandas as gp
import pandas as pd
import os

from datetime import datetime
from city_data import CityData
from worker_tools import compute_time_diff_mins, remove_file

MET_NULL_VALUE = -999
TARGET_HEADING =  '%iy id it imin qn qh qe qs qf U RH Tair press rain kdown snow ldown fcld wuh xsmd lai kdiff kdir wdir'

def get_met_data(task_index, source_base_path, folder_name_city_data, aoi_boundary, utc_offset, sampling_local_hours):
    start_time = datetime.now()

    d = {'geometry': [shapely.wkt.loads(aoi_boundary)]}
    aoi_gdf = gp.GeoDataFrame(d, crs="EPSG:4326")

    # Retrieve and write ERA5 data
    return_code = _get_era5(aoi_gdf, source_base_path, folder_name_city_data, utc_offset, sampling_local_hours)

    # Wrap up results
    step_method = 'ERA5_download'
    met_filename = CityData.filename_era5
    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    run_duration_min = compute_time_diff_mins(start_time)
    return_stdout = (f'{{"task_index": {task_index}, "tile": "None", "step_index": {0}, '
                     f'"step_method": "{step_method}", "met_filename": "{met_filename}", "return_code": {return_code}, '
                     f'"start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}')

    result_json = f'{{"Return_package": [{return_stdout}]}}'

    return result_json


def _get_era5(aoi_gdf, output_base_path, folder_name_city_data, utc_offset, sampling_local_hours):
    from city_metrix.metrics import era_5_met_preprocessing

    # Attempt to download data with up to 3 tries
    count = 0
    aoi_era_5 = None
    while count < 3:
        try:
            aoi_era_5 = era_5_met_preprocessing(aoi_gdf)
            break
        except:
            count +=1

    if aoi_era_5 is None:
        raise Exception('failed to retrieve era5 data')

    # round all numbers to two decimal places, which is the precision needed by the model
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
    met_filename = CityData.filename_era5
    target_file_path = os.path.join(output_base_path, folder_name_city_data, CityData.folder_name_source_data,
                                    CityData.folder_name_met_files, met_filename)
    remove_file(target_file_path)
    with open(target_file_path, 'w') as file:
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
    if value == '':
        str_value = MET_NULL_VALUE
    else:
        str_value = f"{value:.2f}"
    return str_value


def _day_of_year(date_time):
    return (date_time - datetime(date_time.year, 1, 1)).days + 1



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Retrieve meteorological data.')
    parser.add_argument('--task_index', metavar='str', required=True, help='index from the processor config file')
    parser.add_argument('--source_base_path', metavar='path', required=True, help='folder for source data')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--aoi_boundary', metavar='str', required=True, help='geographic boundary of the AOI')
    parser.add_argument('--utc_offset', metavar='str', required=True, help='hour offset from utc')
    parser.add_argument('--sampling_local_hours', metavar='str', required=True, help='comma-delimited string of sampling local hours')

    args = parser.parse_args()

    result_json = get_met_data(args.task_index, args.source_base_path, args.city_folder_name, args.aoi_boundary, args.utc_offset, args.sampling_local_hours)

    print(result_json)