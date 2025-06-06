import os
import fnmatch


def find_and_replace(directory, old_string, new_string, use_start_string_match):
    pattern = '*.yml'
    for root, dirs, files in os.walk(directory):
        for filename in fnmatch.filter(files, pattern):
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
directory = r'C:\Users\kenn.cartier\Documents\github\cities-thermal-comfort-modeling\data\sample_cities'

old_string = "cif_tree_canopy.tif, cif_lulc.tif"
new_string = "cif_lulc.tif, cif_open_urban.tif, cif_tree_canopy.tif"
find_and_replace(directory, old_string, new_string, False)

old_string = 'lulc_tif_filename: lulc.tif'
new_string = 'lulc_tif_filename: lulc.tif\n  open_urban_tif_filename: open_urban.tif'
find_and_replace(directory, old_string, new_string, False)

old_string = 'lulc_tif_filename: cif_lulc.tif'
new_string = 'lulc_tif_filename: cif_lulc.tif\n  open_urban_tif_filename: cif_open_urban.tif'
find_and_replace(directory, old_string, new_string, False)

old_string = 'lulc_tif_filename: None'
new_string = 'lulc_tif_filename: None\n  open_urban_tif_filename: None'
find_and_replace(directory, old_string, new_string, False)



