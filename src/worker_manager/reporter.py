import os
from collections.abc import Iterable
import json
import pandas as pd

from datetime import datetime
from src.workers.worker_tools import create_folder, get_substring_after


def parse_row_results(dc):
    results = []
    # serialize the return information - one way or another
    for obj in dc:
        if isinstance(obj, Iterable):
            for row in obj:
                results.append(row)
        else:
            results.append(obj)

    # extract content from the return package and determine if there was a failure
    results_df = pd.DataFrame(columns=['status', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code', 'start_time', 'run_duration_min'])
    all_passed = True
    failed_task_ids = []
    failed_task_details = []
    for tile_row in results:
        if hasattr(tile_row, 'stderr'):
            error_msg = get_substring_after(tile_row.stderr, 'error:')
            if error_msg:
                all_passed = False
                failed_task_details.append(error_msg)

        if hasattr(tile_row, 'stdout'):
            return_info = _get_inclusive_between_string(tile_row.stdout, '{"Return_package":', "]}")
            if return_info:
                return_package = json.loads(return_info)['Return_package']
                for result in return_package:
                    tile = result['tile']
                    step_index = result['step_index']
                    step_method = result['step_method']
                    met_filename = result['met_filename']
                    return_code = result['return_code']
                    start_time = result['start_time']
                    run_duration_min = result['run_duration_min']

                    status = 'success' if return_code == 0 else 'FAILURE'
                    new_row = [status, tile, step_index, step_method, met_filename, return_code, start_time, run_duration_min]
                    results_df.loc[len(results_df.index)] = new_row

                    if return_code != 0:
                        failed_task_details.append(tile_row)
                        all_passed = False

    return all_passed, results_df, failed_task_ids, failed_task_details


def _get_inclusive_between_string(text, start_substring, end_substring):
    try:
        start_index = text.index(start_substring)
        end_index = text.index(end_substring, start_index) + len(end_substring)
        return text[start_index:end_index]
    except ValueError:
        return None

def report_results(task_method, results_df, target_log_path, city_folder_name):
    results_df.sort_values(['tile', 'step_index', 'met_filename'], inplace=True)

    results_df['run_status'] = results_df['return_code'].apply(_evaluate_return_code)
    results_df['city_folder_name'] = city_folder_name
    results_df['method'] = task_method

    reporting_df = results_df.loc[:,
                   ['run_status', 'city_folder_name', 'tile', 'method', 'step_index',
                    'step_method', 'met_filename',
                    'return_code', 'start_time', 'run_duration_min']]

    create_folder(target_log_path)

    report_date_str =  datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    report_name = f'run_report_{report_date_str}.html'
    report_file_path = os.path.join(target_log_path, report_name)

    reporting_df.to_html(report_file_path)

    return report_file_path


def _evaluate_return_code(return_code):
    return 'success' if return_code == 0 else 'FAILURE'


def _find_files_with_name(root_folder, file_name):
    matching_files = []
    for dirpath, dirnames, filenames in os.walk(root_folder):
        if file_name in filenames:
            matching_files.append(os.path.join(dirpath, file_name))
    return matching_files


def find_files_with_substring_in_name(directory, start_substring, end_substring):
    matching_files = []
    for file_name in os.listdir(directory):
        if file_name.startswith(start_substring) and file_name.endswith(end_substring):
            matching_files.append(file_name)

    matching_files.sort()
    return matching_files
