import os
from pathlib import Path

ROOT_DIR = str(Path(os.path.dirname(os.path.abspath(__file__))).parent)

SRC_DIR = os.path.join(ROOT_DIR, 'src')

DATA_DIR = os.path.join(ROOT_DIR, 'data')



