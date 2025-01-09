import os

from src.constants import PROCESSING_METHODS, FILENAME_METHOD_YML_CONFIG
from src.data_validation.tools import verify_path


def evaluate_basic_config(non_tiled_city_data):
    combined_invalids = []

    paths_invalids = verify_fundamental_paths(non_tiled_city_data)
    combined_invalids.extend(paths_invalids)

    basic_invalids = verify_other_settings(non_tiled_city_data)
    combined_invalids.extend(basic_invalids)

    return combined_invalids


def verify_fundamental_paths(non_tiled_city_data):
    source_base_path = non_tiled_city_data.source_base_path
    target_base_path = non_tiled_city_data.target_base_path
    city_folder_name = non_tiled_city_data.folder_name_city_data

    invalids = []
    if verify_path(source_base_path) is False:
        msg = f'Invalid source base path: {source_base_path}'
        invalids.append((msg, True))

    city_path = os.path.join(source_base_path, city_folder_name)
    if verify_path(city_path) is False:
        msg = f'Invalid source city path: {city_path}'
        invalids.append((msg, True))

    if verify_path(target_base_path) is False:
        msg = f'Invalid target base path: {target_base_path}'
        invalids.append((msg, True))

    if source_base_path == target_base_path:
        msg = f'Source and target base paths cannot be the same.'
        invalids.append((msg, True))

    return invalids


def verify_other_settings(non_tiled_city_data):
    task_method = non_tiled_city_data.new_task_method

    invalids = []
    if task_method not in PROCESSING_METHODS:
        msg = f"Invalid 'method' value ({task_method}) in {FILENAME_METHOD_YML_CONFIG} file. Valid values: {PROCESSING_METHODS}"
        invalids.append((msg, True))

    return invalids

