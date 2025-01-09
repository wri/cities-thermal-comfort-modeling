import math

from src.data_validation.aoi_evaluator import evaluate_aoi
from src.data_validation.basic_validation import evaluate_basic_config

from src.data_validation.custom_intermediate_validator import evaluate_custom_intermediate_config
from src.data_validation.custom_primary_validator import evaluate_custom_primary_config
from src.worker_manager.tools import get_existing_tile_metrics, get_aoi_area_in_square_meters

def validate_config(non_tiled_city_data, processing_option):
    combined_invalids = []

    basic_invalids = evaluate_basic_config(non_tiled_city_data)
    combined_invalids.extend(basic_invalids)

    existing_tiles_metrics = None
    if non_tiled_city_data.custom_primary_feature_list:
        source_city_path = non_tiled_city_data.source_city_path
        custom_primary_filenames = non_tiled_city_data.custom_primary_filenames
        existing_tiles_metrics = get_existing_tile_metrics(source_city_path, custom_primary_filenames,
                                                           project_to_wgs84=True)
        # Get representative cell count
        cell_count = existing_tiles_metrics['cell_count'][0]
    else:
        # Infer raster cell count from aoi
        square_meters = get_aoi_area_in_square_meters(non_tiled_city_data.min_lon, non_tiled_city_data.min_lat,
                                                      non_tiled_city_data.max_lon, non_tiled_city_data.max_lat)
        # Assume 1-meter resolution of target cif files
        cell_count = math.ceil(square_meters)

    aoi_invalids, updated_aoi = evaluate_aoi(non_tiled_city_data, existing_tiles_metrics, processing_option)
    combined_invalids.extend(aoi_invalids)

    custom_primary_invalids = evaluate_custom_primary_config(non_tiled_city_data, existing_tiles_metrics)
    combined_invalids.extend(custom_primary_invalids)

    custom_intermediate_invalids = evaluate_custom_intermediate_config(non_tiled_city_data)
    combined_invalids.extend(custom_intermediate_invalids)

    if combined_invalids:
        head_msg = ' vvvvvvvvvvvv Invalid configurations vvvvvvvvvvvv '
        tail_msg = ' ^^^^^^^^^^^^ Invalid configurations ^^^^^^^^^^^^ '

        print('\n')
        print(head_msg)
        _print_invalids(combined_invalids)
        print(tail_msg)
        print('\n')

    has_fatal_error = True if _invalid_has_fatal_error(combined_invalids) else False
    if has_fatal_error:
        return cell_count, updated_aoi, 1
    else:
        return cell_count, updated_aoi, 0


def _invalid_has_fatal_error(detailed_invalids):
    unique_fatal_error = {t[1] for t in detailed_invalids}
    has_fatal_error = {True}.issubset(unique_fatal_error)
    return has_fatal_error


def _print_invalids(invalids):
    i=1
    for invalid in invalids:
        print(f'{i}) {invalid[0]}')
        i +=1

