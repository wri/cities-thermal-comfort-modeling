import os
from job_handler.config_validator import _validate_basic_inputs, _validate_config_inputs
from job_handler.graph_builder import _build_source_dataframes
from workers.job_manager import start_jobs


"""
Guide to creating standalone app for calling QGIS: https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html
https://medium.com/@giovannigallon/how-i-automate-qgis-tasks-using-python-54df35d8d63f
"""

def start_processing(source_base_path, target_base_path, city_folder_name, pre_check_option):
    abs_source_base_path = os.path.abspath(source_base_path)
    abs_target_base_path = os.path.abspath(target_base_path)
    return_code_basic = _validate_basic_inputs(abs_source_base_path, abs_target_base_path, city_folder_name)

    processing_config_df = _build_source_dataframes(abs_source_base_path, city_folder_name)
    return_code_configs = _validate_config_inputs(processing_config_df, abs_source_base_path, abs_target_base_path, city_folder_name, pre_check_option)

    # TODO - Add checks for prerequite met data, such as consistent CRS

    if pre_check_option == 'no_pre_check':
        return_code, return_str = start_jobs(abs_source_base_path, abs_target_base_path, city_folder_name, processing_config_df)

        if return_code == 0:
            print(return_str)
        else:
            _highlighted_print(return_str)
        return return_code
    else:
        if return_code_basic == 0 and return_code_configs == 0:
            print("\nPassed all validation checks")
            return 0
        else:
            _highlighted_print('Pre-check encountered errors.')
            return -99

def _highlighted_print(msg):
    print('\n\x1b[6;30;42m' + msg + '\x1b[0m')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run methods in the "UMEP for Processing" QGIS plugin.')
    parser.add_argument('--source_base_path', metavar='path', required=True,
                        help='the path to city-based source data')
    parser.add_argument('--target_base_path', metavar='path', required=True,
                        help='path to export results')
    parser.add_argument('--city_folder_name', metavar='path', required=True,
                        help='name of source city_folder')

    valid_methods = ['no_pre_check', 'check_all', 'check_enabled_only']
    parser.add_argument('--pre_check_option', metavar='str', choices=valid_methods, required=True,
                        help=f'specifies type of configuration pre-check. Options are: {valid_methods}')
    args = parser.parse_args()

    return_code = start_processing(args.source_base_path, args.target_base_path, args.city_folder_name, args.pre_check_option)

    print(f'return code: {return_code}')
