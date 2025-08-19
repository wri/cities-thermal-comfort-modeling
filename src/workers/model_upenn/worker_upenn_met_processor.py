import time
import geopandas as gp
import pandas as pd
import os

from city_metrix import Era5MetPreprocessingUPenn
from city_metrix.metrix_model import GeoZone

from src.constants import FILENAME_ERA5_UMEP, FILENAME_ERA5_UPENN, MET_HEADER_0_UPENN, MET_HEADER_1_UPENN, \
    MET_HEADER_2_UPENN
from src.workers.worker_tools import remove_file, create_folder

MET_NULL_VALUE = -999

def get_upenn_met_data(target_met_files_path, aoi_boundary_poly, start_date, end_date,
                       seasonal_utc_offset, sampling_local_hours):
    # Create a GeoDataFrame with the polygon
    aoi_gdf = gp.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[aoi_boundary_poly])

    # Retrieve and write ERA5 data
    return_code = _get_era5_upenn(aoi_gdf, target_met_files_path, start_date, end_date, seasonal_utc_offset, sampling_local_hours)

    return return_code


def _get_era5_upenn(aoi_gdf, target_met_files_path, start_date, end_date, seasonal_utc_offset, sampling_local_hours):
    # Attempt to download data with up to 3 tries
    aoi_era_5 = None
    era5_failure_msg = ''
    count = 1
    geo_zone = GeoZone(aoi_gdf)
    while count <= 5:
        try:
            aoi_era_5 = (Era5MetPreprocessingUPenn(start_date=start_date, end_date=end_date, seasonal_utc_offset=seasonal_utc_offset)
                         .get_metric(geo_zone))
            break
        except Exception as e_msg:
            era5_failure_msg = e_msg
            time.sleep(10)
            count +=1

    if aoi_era_5 is None:
        raise Exception(f'Failed to retrieve era5 data after {count} attempts. Error: {era5_failure_msg}')

    # round all numbers to two decimal places, which is the precision needed by the umep model
    aoi_era_5 = aoi_era_5.round(2)

    # adjust for utc
    int_seasonal_utc_offset = int(seasonal_utc_offset)
    aoi_era_5['time'] = pd.to_datetime(aoi_era_5[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    aoi_era_5['local_time'] = aoi_era_5['time'] + pd.Timedelta(hours=int_seasonal_utc_offset)

    # filter to specific hours of day
    filter_hours = [int(x) for x in sampling_local_hours.split(',')]
    filtered_era_5 = aoi_era_5[aoi_era_5['local_time'].dt.hour.isin(filter_hours)]

    # order by datetime
    filtered_era_5.sort_values(by='time', inplace=True, ascending=True)

    met_header_1_upenn = ','.join(MET_HEADER_0_UPENN)
    met_header_2_upenn = ','.join(MET_HEADER_1_UPENN)
    met_header_3_upenn = ','.join(MET_HEADER_2_UPENN)

    # localize LEAD2_HEADING
    lat = geo_zone.centroid.y
    lon = geo_zone.centroid.x

    local_header_2 = (met_header_2_upenn.replace('<lat>', str(lat)).replace('<lon>', str(lon))
                           .replace('<seasonal_utc_offset>', str(seasonal_utc_offset)))
    
    # Reformat into target format
    reformatted_data = []
    reformatted_data.append(met_header_1_upenn)
    reformatted_data.append(local_header_2)
    reformatted_data.append(met_header_3_upenn)
    row_id = 1
    for index, row in filtered_era_5.iterrows():
        reformatted_data.append(_reformat_line(row_id, row))
        row_id +=1

    # Write results to text file
    target_met_file = os.path.join(target_met_files_path, FILENAME_ERA5_UPENN)
    create_folder(target_met_files_path)
    remove_file(target_met_file)
    with open(target_met_file, 'w') as file:
        for row in reformatted_data:
            file.write('%s\n' % row)

    return 0

def _reformat_line(row_id, line):
    local_datetime = line['local_time']
    year = local_datetime.year
    month = local_datetime.month
    day = local_datetime.day
    hour = local_datetime.hour
    minute = local_datetime.minute

    DHI = line['DHI']
    DNI = line['DNI']
    GHI = line['GHI']
    Clearsky_DHI = line['Clearsky DHI']
    Clearsky_DNI = line['Clearsky DHI']
    Clearsky_GHI = line['Clearsky GHI']
    Wind_Speed = line['Wind Speed']
    Relative_Humidity = line['Relative Humidity']
    Temperature = line['Temperature']
    Pressure = line['Pressure']

    new_line = f'{row_id}'
    new_line += f',{year},{month},{day},{hour},{minute}'
    new_line += f',{DHI},{DNI},{GHI},{Clearsky_DHI},{Clearsky_DNI},{Clearsky_GHI}'
    new_line += f',{Wind_Speed},{Relative_Humidity},{Temperature},{Pressure}'
    return new_line

# def _standardize_string(value):
#     if value == '' or value == 'nan' or np.isnan(value):
#         str_value = MET_NULL_VALUE
#     else:
#         str_value = f"{value:.2f}"
#     return str_value
#
#
# def _day_of_year(date_time):
#     return (date_time - datetime(date_time.year, 1, 1)).days + 1

