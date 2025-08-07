import csv
import os
from datetime import datetime
from pathlib import Path

from city_metrix.metrix_tools import is_date

from src.constants import MET_HEADER_UMEP, MET_HEADER_0_UPENN, MET_HEADER_2_UPENN, PRIOR_5_YEAR_KEYWORD, \
    FILENAME_ERA5_UPENN, FILENAME_ERA5_UMEP


def evaluate_met_files(non_tiled_city_data):
    invalids = []

    # ERA5 date range
    era5_date_range = non_tiled_city_data.era5_date_range
    if era5_date_range is not None:
        parsed_dates = era5_date_range.split(',')
        if not (parsed_dates[0] == PRIOR_5_YEAR_KEYWORD
                or (len(parsed_dates) == 2 and is_date(parsed_dates[0].strip()) and is_date(parsed_dates[1].strip()))):
            msg = 'Specified era5_date_range has invalid values.'
            invalids.append((msg, True))

        if parsed_dates[0] != PRIOR_5_YEAR_KEYWORD:
            start_date = datetime.strptime(parsed_dates[0].strip(), "%Y-%m-%d").date()
            end_date = datetime.strptime(parsed_dates[0].strip(), "%Y-%m-%d").date()
            year_difference = end_date.year - start_date.year
            if year_difference > 5:
                msg = 'Specified era5_date_range must be 5 years or less.'
                invalids.append((msg, True))

            if start_date > end_date:
                msg = f'era5_date_range start_date must be before end_date.'
                invalids.append((msg, True))

            record_start_year = 1940  # For ERA5 https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview
            latest_complete_record_year = datetime.now().year - 1
            if not (record_start_year <= start_date.year <= latest_complete_record_year
                    and record_start_year <= end_date.year <= latest_complete_record_year):
                msg = f'era5_date_range must be between {record_start_year} and {latest_complete_record_year}.'
                invalids.append((msg, True))

    return invalids

def evaluate_meteorological_umep_data(non_tiled_city_data, in_target_folder:bool=True):
    invalids = []
    non_nullable_columns = [0,1,2,3,9,10,11,14,16,22]
    for met_filename in non_tiled_city_data.met_filenames:
        if not (non_tiled_city_data.has_era_met_download and met_filename == FILENAME_ERA5_UMEP):
            if in_target_folder:
                met_folder_path = non_tiled_city_data.target_met_files_path
            else:
                met_folder_path = non_tiled_city_data.source_met_files_path
            met_file_path = os.path.join(met_folder_path, met_filename)

            if Path(str(met_file_path)).suffix != '.txt':
                msg = "Meteorological data for UMEP model must have extension of '.txt'"
                invalids.append((msg, True))
                break

            if os.path.exists(met_file_path):
                with open(met_file_path, mode='r') as file:
                    reader = csv.reader(file, delimiter=' ')
                    row_index = 0
                    for row in reader:
                        # remove trailing blank element
                        row = _remove_blank_element(row)

                        if row_index == 0 and row != MET_HEADER_UMEP:
                            location_str = 'target folder' if in_target_folder is True else "source folder"
                            msg = f'Header row in {met_filename} is invalid in {location_str} at {met_folder_path}'
                            invalids.append((msg, True))

                        if row_index > 0:
                            invalid_values = _evaluate_for_invalid_null_values(row, row_index, met_filename, met_folder_path, MET_HEADER_UMEP, non_nullable_columns)
                            invalids.extend(invalid_values)

                        row_index +=1

    return invalids

def evaluate_meteorological_upenn_data(non_tiled_city_data, in_target_folder:bool=True):
    invalids = []
    non_nullable_columns = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    for met_filename in non_tiled_city_data.met_filenames:
        if not (non_tiled_city_data.has_era_met_download and met_filename == FILENAME_ERA5_UPENN):
            if in_target_folder:
                met_folder_path = non_tiled_city_data.target_met_files_path
            else:
                met_folder_path = non_tiled_city_data.source_met_files_path
            met_file_path = os.path.join(met_folder_path, met_filename)

            if Path(str(met_file_path)).suffix != '.csv':
                msg = "Meteorological data for UPENN model must have extension of '.csv'"
                invalids.append((msg, True))
                break

            if os.path.exists(met_file_path):
                with (open(met_file_path, mode='r') as file):
                    reader = csv.reader(file)
                    row_index = 0
                    for row in reader:
                        # remove trailing blank element
                        row = _remove_blank_element(row)

                        # Note header row 1 is too complex to have simple evaluation
                        if (row_index == 0 and row != MET_HEADER_0_UPENN) or (row_index == 2 and row != MET_HEADER_2_UPENN):
                            location_str = 'target folder' if in_target_folder is True else "source folder"
                            msg = f'Header row in {met_filename} is invalid in {location_str} at {met_folder_path}'
                            invalids.append((msg, True))

                        if row_index > 2:
                            invalid_values = _evaluate_for_invalid_null_values(row, row_index, met_filename, met_folder_path, MET_HEADER_2_UPENN, non_nullable_columns)
                            invalids.extend(invalid_values)

                        row_index +=1

    return invalids

def _remove_blank_element(row):
    # remove trailing blank element
    has_blank = True
    while has_blank == True:
        if row and row[-1].strip() == '':
            row.pop()
            has_blank = True
        else:
            has_blank = False
    return row

def _evaluate_for_invalid_null_values(row, row_index, met_filename, met_folder_path, data_header, non_nullable_columns):
    invalid_values = []
    for c in non_nullable_columns:
        cell_value = row[c]
        if cell_value == '-999':
            field = data_header[c]
            msg = f'Invalid value for {field} field in row {row_index} of met file {met_filename} in {met_folder_path}'
            invalid_values.append((msg, True))

    return invalid_values

