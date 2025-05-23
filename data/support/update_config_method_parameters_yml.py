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

old_string = "Recommended value for tile_buffer_meters is 600."
new_string = "Recommended value for tile_buffer_meters is 600.\n# Specify True for remove_mrt_buffer_for_final_output to remove buffer around MRT result otherwise False. Default is True."
find_and_replace(directory, old_string, new_string, False)

old_string = '  tile_buffer_meters:'; new_string = '  remove_mrt_buffer_for_final_output: True'
find_and_replace(directory, old_string, new_string, True)





