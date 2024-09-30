import csv
import os
import pytest
from setuptools.command.setopt import config_file

from main import main
from src import tools as src_tools
from test.test_umep_for_processing_algorithms import is_valid_output_directory

def test_default_run():
    package_folder = os.path.dirname(os.getcwd())
    data_source_folder = os.path.join(package_folder, 'sample_cities')
    results_target_folder = os.path.join(package_folder, 'test', 'resources')
    main(data_source_folder, results_target_folder)

    has_valid_results = _verify_expected_output_folders(data_source_folder, results_target_folder)
    assert has_valid_results


def _verify_expected_output_folders(data_source_folder, results_target_folder):
    enabled_target_folder = []
    config_file = os.path.join(data_source_folder, 'umep_city_processing_registry.csv')
    with open(config_file, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader, None)  # skip the headers
        for row in csv_reader:
            enabled = src_tools.toBool[row[1].lower()]

            if enabled is True:
                city_folder = row[2]
                result_folder = os.path.join(results_target_folder, city_folder)
                enabled_target_folder.append(result_folder)

    unique_target_folders = set(enabled_target_folder)
    expected_target_folder_count = len(unique_target_folders)
    actual_target_folder_count = 0
    for folder in unique_target_folders:
        out_directory = os.path.abspath(folder)
        if is_valid_output_directory(out_directory) is True:
            actual_target_folder_count += 1

    return True if expected_target_folder_count == actual_target_folder_count else False
