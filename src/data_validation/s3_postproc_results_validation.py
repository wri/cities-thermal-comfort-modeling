from city_metrix.metrix_dao import get_bucket_name_from_s3_uri

from src.constants import S3_PUBLICATION_BUCKET
from src.workers.city_data import CityData
from src.workers.worker_dao import identify_tiles_with_partial_file_set, get_scenario_folder_key, check_s3_folder_exists


def validate_tile_results(non_tiled_city_data: CityData):
    if non_tiled_city_data.city_json_str is None:
        print("A city is not specified in the yml file, so nothing to evaluate. Stopping")
    else:
        scenario_folder_key = get_scenario_folder_key(non_tiled_city_data)
        bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)
        scenario_exists = check_s3_folder_exists(bucket_name, scenario_folder_key)

        if not scenario_exists:
            print(f"Scenario not found, so nothing to evaluate at {scenario_folder_key}. Stopping")
        else:
            partial_tiles = identify_tiles_with_partial_file_set(non_tiled_city_data)

            print(f"\nValidating tiles in {scenario_folder_key}")
            print("NOTE: This analysis currently only validates file counts in tiles. It does not evaluate data quality.")

            if len(partial_tiles) == 0:
                print(f"\nAll files in {scenario_folder_key} have the correct count of files.")
            else:
                print(f"\nERROR: Partial sets of tiles files found in {scenario_folder_key}")
                print(f"\nRepair these tiles using the tile_names() method in the config yml file and specifying the below set of tile names.")
                print_list_no_quotes(partial_tiles)

# Custom print function to format list without quotes
def print_list_no_quotes(list):
    # Create a single string with each element separated by commas
    formatted = ', '.join(list)
    # Enclose the full string in square brackets and print
    print(f"[{formatted}]")