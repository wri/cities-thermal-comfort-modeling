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

old_string = 'north_temperate_leaf_start: 97'; new_string = 'north_temperate_leaf_start: 96'
find_and_replace(directory, old_string, new_string)

old_string = 'north_temperate_leaf_end: 300'; new_string = 'north_temperate_leaf_end: 301'
find_and_replace(directory, old_string, new_string)

old_string = 'south_temperate_leaf_start: 283'; new_string = 'south_temperate_leaf_start: 282'
find_and_replace(directory, old_string, new_string)

old_string = 'south_temperate_leaf_end: 96'; new_string = 'south_temperate_leaf_end: 97'
find_and_replace(directory, old_string, new_string)

