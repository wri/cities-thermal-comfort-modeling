import os
import fnmatch
import shutil
from pathlib import Path

def make_yml_backup(directory):
    yml_file_pattern = '.config_method_parameters.yml'
    for root, dirs, files in os.walk(directory):
        for filename in fnmatch.filter(files, yml_file_pattern):
            file_path = os.path.join(root, filename)
            # make backup of existing yml file
            parent_path = Path(file_path).parent
            yml_backup = os.path.join(parent_path, '.prior_config_method_parameters.yml')
            if os.path.exists(yml_backup):
                os.remove(yml_backup)
            shutil.copy(file_path, yml_backup)

def find_and_replace(directory, old_string, new_string, use_start_string_match):
    yml_file_pattern = '.config_method_parameters.yml'
    for root, dirs, files in os.walk(directory):
        for filename in fnmatch.filter(files, yml_file_pattern):
            file_path = os.path.join(root, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                if use_start_string_match:
                    new_content = ''
                    lines = file.readlines()
                    for line in lines:
                        if line.startswith(old_string):
                            new_lines = line + new_string + "\n"
                            new_content += new_lines
                        else:
                            new_content += line
                else:
                    content = file.read()
                    new_content = content.replace(old_string, new_string)

            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(new_content)

            print(f"Updated {file_path}")


# Example usage
directory = r'C:\Users\kenn.cartier\Documents\github\cities-thermal-comfort-modeling\data\sample_cities\_prior_format'

make_yml_backup(directory)

# old_string = '# Description of scenario'
# new_string = '# Description of scenario\n# Options for publishing_target: [local, s3, both].'
# find_and_replace(directory, old_string, new_string, False)
#
# old_string = '  version:'
# new_string = '  infra_id: baseline\n  version:'
# find_and_replace(directory, old_string, new_string, False)
#
# old_string = '# Processing Area of Interest used '
# new_string = '  publishing_target: local\n# Processing Area of Interest used '
# find_and_replace(directory, old_string, new_string, False)

# old_string = '# Specify None for tile_side_meters'
# new_string = '# Specify either city_id/aoi_id json, an aoi_bounds, or both for when city extent includes the aoi_bounds. Specify the\n# EPSG code for aoi_bounds coordinates. See ZZZ_template_city for example ciy specification.\n# Specify None for tile_side_meters'
# find_and_replace(directory, old_string, new_string, False)
#
# old_string = '  min_lon:'
# new_string = '  city: None\n  aoi_bounds:\n    epsg_code: 4326\n    west:'
# find_and_replace(directory, old_string, new_string, False)
#
# old_string = '  min_lat:'
# new_string = '    south:'
# find_and_replace(directory, old_string, new_string, False)
#
# old_string = '  max_lon:'
# new_string = '    east:'
# find_and_replace(directory, old_string, new_string, False)
#
# old_string = '  max_lat:'
# new_string = '    north:'
# find_and_replace(directory, old_string, new_string, False)

# old_string = ''
# new_string = ''
# find_and_replace(directory, old_string, new_string, False)

