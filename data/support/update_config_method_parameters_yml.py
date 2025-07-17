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

old_string = "# Recommended filenames: dem.tif, dsm_ground_build.tif, tree_canopy.tif, lulc.tif"
new_string = "# Recommended filenames: dem.tif, dsm_ground_build.tif, lulc.tif, open_urban.tif, tree_canopy.tif, albedo.cif"
find_and_replace(directory, old_string, new_string, False)

old_string = "# Auto-generated filenames from CIF: cif_dem.tif, cif_dsm_ground_build.tif, cif_lulc.tif, cif_open_urban.tif, cif_tree_canopy.tif"
new_string = "# Auto-generated CIF filenames: cif_dem.tif, cif_dsm_ground_build.tif, cif_lulc.tif, cif_open_urban.tif, cif_tree_canopy.tif, cif_albedo.tif"
find_and_replace(directory, old_string, new_string, False)

old_string = "- PreparedIntermediateFilenames:"
new_string = "  albedo_tif_filename: None\n- PreparedIntermediateFilenames:"
find_and_replace(directory, old_string, new_string, False)

# open_urban_tif_filename: None
# old_string = ""
# new_string = ""
# find_and_replace(directory, old_string, new_string, False)

