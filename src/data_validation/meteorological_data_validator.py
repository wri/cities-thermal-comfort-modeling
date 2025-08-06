import csv
import os
from pathlib import Path
from src.constants import MET_HEADER_UMEP, MET_HEADER_0_UPENN, MET_HEADER_2_UPENN


def evaluate_meteorological_umep_data(non_tiled_city_data, in_target_folder:bool=True):
    invalids = []
    non_nullable_columns = [0,1,2,3,9,10,11,14,16,22]
    for met_file in non_tiled_city_data.met_filenames:
        met_filename = met_file['filename']

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
    for met_file in non_tiled_city_data.met_filenames:
        met_filename = met_file['filename']

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


def _evaluate_for_invalid_null_values(row, row_index, met_filename, met_folder_path, data_header, non_nullable_columns):
    invalid_values = []
    for c in non_nullable_columns:
        cell_value = row[c]
        if cell_value == '-999':
            field = data_header[c]
            msg = f'Invalid value for {field} field in row {row_index} of met file {met_filename} in {met_folder_path}'
            invalid_values.append((msg, True))

    return invalid_values

