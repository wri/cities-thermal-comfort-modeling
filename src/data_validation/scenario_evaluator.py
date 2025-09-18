from src.constants import FILENAME_METHOD_YML_CONFIG


def evaluate_scenario(non_tiled_city_data):
    invalids = []

    if non_tiled_city_data.publishing_target is not None and non_tiled_city_data.publishing_target not in ('local', 's3', 'both'):
        msg = f'publish_target option has invalid value in {FILENAME_METHOD_YML_CONFIG}'
        invalids.append((msg, True))

    return invalids