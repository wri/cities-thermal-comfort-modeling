import os

def is_valid_output_directory(path):
    is_valid = True if os.path.isdir(path) and len(os.listdir(path)) > 0 else False
    return is_valid

def is_valid_output_file(file_path):
    is_valid = True if os.path.isfile(file_path) and os.path.getsize(file_path) > 0 else False
    return is_valid