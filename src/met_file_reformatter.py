import os
from pathlib import Path
from datetime import datetime, timedelta


NULL_VALUE = -999
SOURCE_HEADING = ',time,lat,lon,temp,rh,global_rad,direct_rad,diffuse_rad,water_temp,wind,vpd'
TARGET_HEADING =  '%iy id it imin qn qh qe qs qf U RH Tair press rain kdown snow ldown fcld wuh xsmd lai kdiff kdir wdir'

def reformat(source_folder, target_folder, utc_offset):
    files_and_dirs = os.listdir(source_folder)

    # Filter out only the files
    source_met_files = [f for f in files_and_dirs if os.path.isfile(os.path.join(source_folder, f)) and Path(f).suffix == '.txt']

    for file in source_met_files:
        source_file_path = os.path.join(source_folder, file)
        target_file_path = os.path.join(target_folder, file)
        convert_to_output_folder(source_file_path, target_file_path, utc_offset)


def convert_to_output_folder(source_file, target_file, utc_offset):
    i = 0
    reformatted_data = []
    with open(source_file) as fp:
        for line in fp:
            if i == 0:
                header_line = line.rstrip()
                is_valid_header = _is_header_valid(header_line)
                if is_valid_header is False:
                    raise Exception('Met file does not have expected header')
                else:
                    reformatted_data.append(TARGET_HEADING)
            else:
                reformatted_data.append(_reformat_line(line, utc_offset))
            i += 1

    with open(target_file, 'w') as file:
        for row in reformatted_data:
            file.write('%s\n' % row)


def _reformat_line(line, utc_offset):
    values = line.split(',')

    # source titles
    rowid = values[0]
    local_datetime = _string_to_datetime(values[1], utc_offset)
    lat = values[2]
    lon = values[3]
    temp = values[4]
    rh = values[5]
    global_rad = values[6]
    direct_rad = values[7]
    diffuse_rad = values[8]
    water_temp = values[9]
    wind = values[10]
    vpd = values[11].rstrip()

    # remapped titles
    iy = local_datetime.year
    id = _day_of_year(local_datetime)
    it = local_datetime.hour
    imin = local_datetime.minute
    qn = NULL_VALUE
    qh = NULL_VALUE
    qe = NULL_VALUE
    qs = NULL_VALUE
    qf = NULL_VALUE
    U = _standardize_string(wind)
    RH = _standardize_string(rh)
    Tair = _standardize_string(temp)
    press = NULL_VALUE
    rain = NULL_VALUE
    kdown = _standardize_string(global_rad)
    snow = NULL_VALUE
    ldown = _standardize_string(diffuse_rad)
    fcld = NULL_VALUE
    wuh = NULL_VALUE
    xsmd = NULL_VALUE
    lai = NULL_VALUE
    kdiff = NULL_VALUE
    kdir = _standardize_string(direct_rad)
    wdir = NULL_VALUE

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
    return NULL_VALUE if value == '' else value

def _day_of_year(date_time):
    return (date_time - datetime(date_time.year, 1, 1)).days + 1

def _string_to_datetime(utc_datetime_string, utc_offset):
    utc_datetime = datetime.strptime(utc_datetime_string, '%Y-%m-%d %H:%M:%S')
    local_time = utc_datetime + timedelta(hours=utc_offset)
    return local_time

def _is_header_valid(header_line):
    expected_line = SOURCE_HEADING
    return True if header_line == expected_line else False
