import time
from workers.city_data import CityData
from workers.source_cif_data_downloader import get_cif_data

PROCESSING_PAUSE_TIME_SEC = 30

def process_tile(task_index, task_method, source_base_path, target_base_path, city_folder_name, tile_folder_name,
                 cif_features, tile_boundary, tile_resolution):
    from workers.umep_plugin_processor import run_plugin

    def _execute_retrieve_cif_data(task_idx, source_path, folder_city, folder_tile, features, boundary, resolution):
        cif_stdout = \
            get_cif_data(task_idx, source_path, folder_city, folder_tile, features, boundary, resolution)
        return cif_stdout

    def _execute_solweig_only_plugin(task_idx, step_index, folder_city, folder_tile, source_path, target_path):
        city_data = CityData(folder_city, folder_tile, source_path, target_path)

        out_list = []
        for met_file in city_data.met_files:
            met_filename = met_file.get('filename')
            utc_offset = met_file.get('utc_offset')

            solweig_stdout = run_plugin(task_idx, step_index, 'solweig_only', folder_city,
                                     folder_tile, source_path, target_path, met_filename, utc_offset)
            out_list.append(solweig_stdout)
        return out_list

    def _execute_solweig_full_plugin_steps(task_idx, folder_city, folder_tile, source_path, target_path):
        out_list = []
        this_stdout1 = run_plugin(task_idx, 1, 'wall_height_aspect', folder_city, folder_tile,
                   source_path, target_path)
        out_list.append(this_stdout1)

        this_stdout2 = run_plugin(task_idx, 2, 'skyview_factor', folder_city, folder_tile,
                   source_path, target_path)
        out_list.append(this_stdout2)

        time.sleep(PROCESSING_PAUSE_TIME_SEC)
        this_stdout3 = _execute_solweig_only_plugin(task_idx, 3, folder_city,
                                                    folder_tile, source_path, target_path)
        out_list.extend(this_stdout3)

        return out_list

    return_stdouts = []
    if cif_features != 'None' and cif_features != '':
        return_val = _execute_retrieve_cif_data(task_index, source_base_path, city_folder_name, tile_folder_name,
                                   cif_features, tile_boundary, tile_resolution)
        return_stdouts.append(return_val)
        time.sleep(PROCESSING_PAUSE_TIME_SEC)

    if task_method != 'cif_download_only':
        if task_method == 'solweig_full':
            return_vals = _execute_solweig_full_plugin_steps(task_index, city_folder_name, tile_folder_name, source_base_path, target_base_path)
            return_stdouts.extend(return_vals)
        elif task_method == 'solweig_only':
            return_vals = _execute_solweig_only_plugin(task_index, 1, city_folder_name,
                                         tile_folder_name, source_base_path, target_base_path)
            return_stdouts.extend(return_vals)
        elif task_method in CityData.processing_methods:
            return_val = run_plugin(task_index, 1, task_method, city_folder_name, tile_folder_name,
                       source_base_path, target_base_path)
            return_stdouts.append(return_val)
        else:
            return ''

    # Construct json of combined return values
    result_str = ','.join(return_stdouts)
    result_json = f'{{"Return_package": [{result_str}]}}'

    return result_json


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process tile.')
    parser.add_argument('--task_index', metavar='str', required=True, help='index from the processor config file')
    parser.add_argument('--task_method', metavar='str', required=True, help='method to run')
    parser.add_argument('--source_base_path', metavar='path', required=True, help='folder for source data')
    parser.add_argument('--target_base_path', metavar='path', required=True, help='folder for writing data')
    parser.add_argument('--city_folder_name', metavar='str', required=True, help='name of city folder')
    parser.add_argument('--tile_folder_name', metavar='str', required=True, help='name of tile folder')
    parser.add_argument('--cif_features', metavar='str', required=True, help='coma-delimited list of cif features to retrieve')
    parser.add_argument('--tile_boundary', metavar='str', required=True, help='geographic boundary of tile')
    parser.add_argument('--tile_resolution', metavar='str', required=True, help='resolution of tile in m.')

    args = parser.parse_args()

    return_stdout =process_tile(args.task_index, args.task_method, args.source_base_path, args.target_base_path,
                                args.city_folder_name, args.tile_folder_name, args.cif_features, args.tile_boundary,
                                args.tile_resolution)

    print(return_stdout)