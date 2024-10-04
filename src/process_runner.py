import csv
import os
import ast

from datetime import datetime
from pathlib import Path
from src import tools as src_tools
from src.CityData import instantiate_city_data
from src.qgis_initializer import QgisHandler

from src.umep_for_processing_plugins import UmepProcessingQgisPlugins, log_method_failure

UMEP_CITY_PROCESSING_REGISTRY_FILE = 'umep_city_processing_registry.csv'
SOLWEIG_TIME_SERIES_CONFIG_FILE = 'time_series_config.csv'

def run_plugins(data_source_folder, results_target_folder):
    print('hi1')
    qh = QgisHandler(0)
    qgis_app = qh.qgis_app
    UPP = UmepProcessingQgisPlugins(qgis_app)
    source_data_path = os.path.abspath(data_source_folder)
    target_path = os.path.abspath(results_target_folder)

    config_processing_file_path = os.path.join(source_data_path, UMEP_CITY_PROCESSING_REGISTRY_FILE)
    with open(config_processing_file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader, None)  # skip the headers
        # Iterate over each row in the CSV file
        for row in csv_reader:
            runID = row[0]
            enabled = src_tools.toBool[row[1].lower()]

            success = True
            if enabled is True:
                start_time = datetime.now()
                city_folder_name = row[2]
                tile_folder_name = row[3]
                city_data = instantiate_city_data(city_folder_name, tile_folder_name, source_data_path, target_path)
                method = row[4].lower()

                # TODO - Add checks for prerequite data!!
                e_msg = ''
                if method not in ['all', 'wall_height_aspect', 'skyview_factor', 'solweig']:
                    e_msg = 'Invalid method'
                    success = False
                else:
                    return_code = _run_wall_height_aspect(UPP, runID, city_data, method)
                    if return_code == 0:
                        return_code = _run_skyview_factor(UPP, runID, city_data, method)
                        if return_code == 0:
                            return_code = _run_solweig(UPP, runID, city_data, method)
                            if return_code != 0:
                                success = False
                        else:
                            success = False
                    else:
                        success = False

            if success is not True:
                log_method_failure(start_time, city_data.city_folder_name, runID, None, city_data.source_base_path, e_msg)

def _run_wall_height_aspect(UPP, runID, city_data, method):
    return_code = 0
    if method in ['all', 'wall_height_aspect']:
        return_code = UPP.generate_wall_height_aspect(runID, city_data)
    return return_code

def _run_skyview_factor(UPP, runID, city_data, method):
    return_code = 0
    if method in ['all', 'skyview_factor']:
        return_code = UPP.generate_skyview_factor_files(runID, city_data)
    return return_code

def _run_solweig(UPP, runID, city_data, method):
    return_code = 0
    if method in ['all', 'solweig']:
        config_tcm_time_series_path = os.path.join(city_data.city_source_path, SOLWEIG_TIME_SERIES_CONFIG_FILE)
        return_code = 0
        with open(config_tcm_time_series_path, mode='r') as solweig_config_file:
            csv_reader = csv.reader(solweig_config_file)
            next(csv_reader, None)  # skip the headers

            for row in csv_reader:
                step = row[0]
                enabled = src_tools.toBool[row[1].lower()]
                if enabled:
                    met_file_name = row[2]
                    utc_offset = row[3]

                    return_code = UPP.generate_solweig(runID, step, city_data, met_file_name, utc_offset)
                if return_code != 0:
                    break
    return return_code

def _construct_result_path(result_folder, met_file_path):
    result_path = os.path.join(os.path.abspath(result_folder), Path(met_file_path).stem)
    return result_path