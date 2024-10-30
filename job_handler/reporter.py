import os
from collections.abc import Iterable
import json
import pandas as pd
from datetime import datetime

from src.src_tools import create_folder
from workers.city_data import CityData

def _parse_row_results(dc):
    results = []
    # serialize the return information - one way or another
    for obj in dc:
        if isinstance(obj, Iterable):
            for row in obj:
                results.append(row)
        else:
            results.append(obj)

    # extract content from the return package and determine if there was a failure
    results_df = pd.DataFrame(columns=['status', 'task_index', 'tile', 'step_index', 'step_method', 'met_filename', 'return_code', 'start_time', 'run_duration_min'])
    all_passed = True
    failed_task_ids = []
    failed_task_details = []
    for tile_row in results:
        if hasattr(tile_row, 'stdout'):
            return_info = _get_inclusive_between_string(tile_row.stdout, '{"Return_package":', "]}")
            if return_info:
                return_package = json.loads(return_info)['Return_package']
                for result in return_package:
                    task_index = result['task_index']
                    tile = result['tile']
                    step_index = result['step_index']
                    step_method = result['step_method']
                    met_filename = result['met_filename']
                    return_code = result['return_code']
                    start_time = result['start_time']
                    run_duration_min = result['run_duration_min']

                    status = 'success' if return_code == 0 else 'FAILURE'
                    new_row = [status, task_index, tile, step_index, step_method, met_filename, return_code, start_time, run_duration_min]
                    results_df.loc[len(results_df.index)] = new_row

                    if return_code != 0:
                        failed_task_ids.append(task_index)
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

def _report_results(enabled_processing_tasks_df, results_df, target_base_path, city_folder_name):
    results_df.sort_values(['task_index', 'step_index', 'met_filename'], inplace=True)

    merged = pd.merge(enabled_processing_tasks_df, results_df, left_index=True, right_on='task_index',
                      how='outer')
    merged['run_status'] = merged['return_code'].apply(_evaluate_return_code)
    merged['city_folder_name'] = city_folder_name

    reporting_df = merged.loc[:,
                   ['run_status', 'task_index', 'city_folder_name', 'tile', 'method', 'step_index',
                    'step_method', 'met_filename',
                    'return_code', 'start_time', 'run_duration_min']]

    results_subfolder = CityData.folder_name_results
    report_folder = str(os.path.join(target_base_path, city_folder_name, results_subfolder, '.run_reports'))
    create_folder(report_folder)

    report_date_str =  datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    report_name = f'run_report_{report_date_str}.html'
    report_file_path = os.path.join(report_folder, report_name)

    reporting_df.to_html(report_file_path)

    return report_file_path

def _evaluate_return_code(return_code):
    return 'success' if return_code == 0 else 'FAILURE'



