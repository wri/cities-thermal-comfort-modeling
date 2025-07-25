import math
import os

from src.constants import FOLDER_NAME_PRIMARY_RASTER_FILES
from src.data_validation.aoi_evaluator import evaluate_aoi
from src.data_validation.basic_validation import evaluate_basic_config

from src.data_validation.custom_intermediate_validator import evaluate_custom_intermediate_config
from src.data_validation.custom_primary_validator import evaluate_custom_primary_config
from src.data_validation.meteorological_data_validator import evaluate_meteorological_data
from src.worker_manager.tools import get_existing_tile_metrics, get_aoi_area_in_square_meters

def validate_config(non_tiled_city_data, existing_tiles_metrics, processing_option):
    combined_invalids = []

    basic_invalids = evaluate_basic_config(non_tiled_city_data)
    combined_invalids.extend(basic_invalids)

    if non_tiled_city_data.custom_primary_feature_list and len(existing_tiles_metrics) == 0:
        source_raster_folder = os.path.join(non_tiled_city_data.source_city_primary_data_path, FOLDER_NAME_PRIMARY_RASTER_FILES)
        msg = f"Primary custom raster files not found in {source_raster_folder}."
        combined_invalids.append((msg, True))
        updated_aoi = None
    else:
        custom_primary_invalids = evaluate_custom_primary_config(non_tiled_city_data, existing_tiles_metrics)
        combined_invalids.extend(custom_primary_invalids)

        aoi_invalids, updated_aoi = evaluate_aoi(non_tiled_city_data, existing_tiles_metrics, processing_option)
        combined_invalids.extend(aoi_invalids)

        if non_tiled_city_data.new_task_method != 'upenn_model':
            met_invalids = evaluate_meteorological_data(non_tiled_city_data, in_target_folder=False)
            combined_invalids.extend(met_invalids)

        custom_intermediate_invalids = evaluate_custom_intermediate_config(non_tiled_city_data)
        combined_invalids.extend(custom_intermediate_invalids)

    if combined_invalids:
        print_invalids(combined_invalids)

    return_code = 1 if _invalid_has_fatal_error(combined_invalids) else 0

    return updated_aoi, return_code


def _invalid_has_fatal_error(detailed_invalids):
    unique_fatal_error = {t[1] for t in detailed_invalids}
    has_fatal_error = {True}.issubset(unique_fatal_error)
    return has_fatal_error


def print_invalids(invalids):
    head_msg = ' vvvvvvvvvvvv Invalid configurations vvvvvvvvvvvv '
    tail_msg = ' ^^^^^^^^^^^^ Invalid configurations ^^^^^^^^^^^^ '

    print('\n')
    print(head_msg)
    _print_invalid_statements(invalids)
    print(tail_msg)
    print('\n')


def _print_invalid_statements(invalids):
    i=1
    for invalid in invalids:
        is_failure = invalid[1]
        if is_failure:
            print(f'({i}) ERROR: {invalid[0]}')
        else:
            print(f'({i}) WARNING: {invalid[0]}')
        i +=1