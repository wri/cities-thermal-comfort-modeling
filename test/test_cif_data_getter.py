import os
from src.solweig_source_getter import get_cif_data
from src.tools import get_application_path


def test1():
    ## Name of the area of interest
    aoi_name = 'amsterdam-test'

    file_name = f'{aoi_name}.geojson'

    ## Path to polygon file the area you want data for
    app_path = get_application_path()
    folder = str(os.path.join(app_path, 'data', file_name))

    get_cif_data(aoi_name, folder)



