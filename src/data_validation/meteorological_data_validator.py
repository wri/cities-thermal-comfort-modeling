import csv
import os

valid_header = ['%iy', 'id', 'it', 'imin', 'qn', 'qh', 'qe', 'qs', 'qf', 'U', 'RH', 'Tair', 'press', 'rain', 'kdown', 'snow', 'ldown', 'fcld', 'wuh', 'xsmd', 'lai', 'kdiff', 'kdir', 'wdir']

def evaluate_meteorological_data(non_tiled_city_data, in_target_folder:bool=True):
    invalids = []
    for met_file in non_tiled_city_data.met_filenames:
        met_filename = met_file['filename']

        if in_target_folder:
            met_folder_path = non_tiled_city_data.target_met_files_path
        else:
            met_folder_path = non_tiled_city_data.source_met_files_path
        met_file_path = os.path.join(met_folder_path, met_filename)

        if os.path.exists(met_file_path):
            with open(met_file_path, mode='r') as file:
                reader = csv.reader(file, delimiter=' ')
                row_index = 0
                for row in reader:
                    if row_index == 0 and row != valid_header:
                        location_str = 'target folder' if in_target_folder is True else "source folder"
                        msg = f'Header row in {met_filename} is invalid in {location_str} at {met_folder_path}'
                        invalids.append((msg, True))

                    if row_index > 0:
                        invalid_values = _evaluate_for_invalid_null_values(row, row_index, met_filename, met_folder_path)
                        invalids.extend(invalid_values)

                    row_index +=1


    return invalids

def _evaluate_for_invalid_null_values(row, row_index, met_filename, met_folder_path):
    invalid_values = []
    non_nullable_columns = [0,1,2,3,9,10,11,14,16,22]
    for c in non_nullable_columns:
        cell_value = row[c]
        if cell_value == '-999':
            field = valid_header[c]
            msg = f'Invalid value for {field} field in row {row_index} of met file {met_filename} in {met_folder_path}'
            invalid_values.append((msg, True))

    return invalid_values

