import os
import math
import datetime

from src.data_validation.manager import validate_config
from src.worker_manager.ancillary_files import write_config_files
from src.worker_manager.job_manager import start_jobs
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder

"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""


def start_processing(source_base_path, target_base_path, city_folder_name, processing_option):
    abs_source_base_path = os.path.abspath(source_base_path)
    abs_target_base_path = os.path.abspath(target_base_path)

    non_tiled_city_data = CityData(city_folder_name, None, source_base_path, target_base_path)

    umep_solweig_cell_count, updated_aoi, config_return_code  = validate_config(non_tiled_city_data, processing_option)

    if config_return_code != 0:
        raise ValueError('Invalid configuration(s). Stopping.')

    # Print runtime estimate
    if umep_solweig_cell_count is not None:
        x = umep_solweig_cell_count
        if umep_solweig_cell_count < 1E4:
            print(f'\nEstimated runtime for full solweig without intermediate files is a few minutes or less for {x:,} raster cells.\n')
        else:
            if umep_solweig_cell_count < 4E6:
                est_runtime_mins = math.ceil(3.0166957E-5*x +3.2689E1)
            else:
                est_runtime_mins = math.ceil(6.26228065090E-4*x -2.28907E3)
            _print_runtime_estimate(x, est_runtime_mins)

    if processing_option == 'run_pipeline':
        # Remove existing target scenario folder
        target_scenario_path = non_tiled_city_data.target_city_path
        remove_folder(target_scenario_path)

        # write configs and last run metrics
        write_config_files(source_base_path, target_base_path, city_folder_name, updated_aoi)

        return_code, return_str = start_jobs(abs_source_base_path, abs_target_base_path, city_folder_name)

        if return_code == 0:
            print(return_str)
        else:
            _highlighted_yellow_print(return_str)
        return return_code


def _print_runtime_estimate(cell_count, est_runtime_mins):
    est_runtime_hours = round(est_runtime_mins / 60, 1)
    now = datetime.datetime.now()
    time_change = datetime.timedelta(minutes=est_runtime_mins)
    est_end_time = now + time_change
    print(f'\nEstimated runtime for full solweig without intermediate files is {est_runtime_hours} hours for {cell_count:,} raster cells.')
    print(f'Estimated completion time for processing of tile_001: {est_end_time.strftime('%m/%d/%Y %I:%M %p')}\n')


def _highlighted_yellow_print(msg):
    print('\n\x1b[6;30;43m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--source_base_path', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--target_base_path', metavar='str', required=True,
                        help='the path to city-based target data')
    parser.add_argument('--city_folder_name', metavar='str', required=True,
                        help='name of source city_folder')
    valid_methods = ['run_pipeline', 'pre_check']
    parser.add_argument('--processing_option', metavar='str', choices=valid_methods, required=True,
                        help=f'specifies type of configuration pre-check. Options are: {valid_methods}')
    args = parser.parse_args()

    start_processing(args.source_base_path, args.target_base_path, args.city_folder_name, args.processing_option)
