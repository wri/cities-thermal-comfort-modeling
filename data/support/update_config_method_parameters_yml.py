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
directory = r'C:\Users\kenn.cartier\Documents\temp_gh\cities-thermal-comfort-modeling\data\sample_cities'

make_yml_backup(directory)

old_string = '# Processing Area of Interest used for tile construction and ERA5-data download. If any custom files\n# are specified above, then the system will not construct a new tile grid and will instead use the existing one.'
new_string = '# Processing Area of Interest used for tile construction and ERA5-data download. If any custom files are specified in\n# CustomTiffFilenames section, then the system will not construct a new tile grid and will instead use the existing one.\n# Specify the seasonal_utc_offset including DST offset if AOI follows this practice for the anticipated hottest season.'
find_and_replace(directory, old_string, new_string, False)

old_string = "  utc_offset:"
new_string = "  seasonal_utc_offset:"
find_and_replace(directory, old_string, new_string, False)

old_string = '# Names of meteorological files used by SOLWEIG method in "UMEP for Processing" plugin.\n# Specify custom files or "<download_era5>" method to automatically download ERA5 data into a file named met_era5_hottest_days.txt'
new_string = '# Names of custom meteorological file(s) or ERA5 data range. Maximum of two filenames can be specified.\n# For custom files, specify the filename with either .csv extension for UPenn model or .txt extension for UMEP model\n# For era5 download, specify either "ERA5:<prior_5_years>" or two dates as "ERA5:<2023-01-01,2023-12-31>"'
find_and_replace(directory, old_string, new_string, False)

old_string = '- filename: <download_era5>'
new_string = '- filename: ERA5:<2023-01-01,2023-12-31>'
find_and_replace(directory, old_string, new_string, False)

old_string = '# All four file mappings must be specified here. Specify None for filename where custom file is not available for processing.\n# Recommended filenames: dem.tif, dsm_ground_build.tif, tree_canopy.tif, lulc.tif\n# Auto-generated filenames from CIF: cif_dem.tif, cif_dsm_ground_build.tif, cif_lulc.tif, cif_open_urban.tif, cif_tree_canopy.tif'
new_string = '# All file mappings must be specified here. Specify None for filename where custom file is not available for processing.\n# Recommended filenames: albedo.tif, dem.tif, dsm_ground_build.tif, lulc.tif, open_urban.tif, tree_canopy.tif\n# Auto-generated CIF filenames: cif_albedo.tif, cif_dem.tif, cif_dsm_ground_build.tif, cif_lulc.tif, cif_open_urban.tif, cif_tree_canopy.tif'
find_and_replace(directory, old_string, new_string, False)

old_string = '- CustomTiffFilenames:'
new_string = '  albedo_tif_filename: None'
find_and_replace(directory, old_string, new_string, True)

old_string = '# Valid methods are [download_only, umep_solweig]'
new_string = '# Valid methods are [download_only, umep_solweig, upenn_model]'
find_and_replace(directory, old_string, new_string, False)

# old_string = ''
# new_string = ''
# find_and_replace(directory, old_string, new_string, False)

