import os

def verify_path(path):
    is_valid = os.path.exists(path)
    return is_valid
