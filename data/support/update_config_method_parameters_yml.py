import os
import fnmatch


def find_and_replace(directory, old_string, new_string):
    pattern = '*.yml'
    for root, dirs, files in os.walk(directory):
       for filename in fnmatch.filter(files, pattern):
           file_path = os.path.join(root, filename)
           with open(file_path, 'r', encoding='utf-8') as file:
               content = file.read()
           new_content = content.replace(old_string, new_string)
           with open(file_path, 'w', encoding='utf-8') as file:
               file.write(new_content)
           print(f"Updated {file_path}")


# Example usage
directory = r'C:\Users\kenn.cartier\Documents\github\cities-thermal-comfort-modeling\data\sample_cities'

old_string = "If you don't want to subdivide the AOI, specify None for tile_side_meters and tile_buffer_meters."
new_string = "Specify None for tile_side_meters to not subdivide the AOI. Recommended value for tile_buffer_meters is 600."
find_and_replace(directory, old_string, new_string)

old_string = 'tile_buffer_meters: None'; new_string = 'tile_buffer_meters: 600'
find_and_replace(directory, old_string, new_string)





