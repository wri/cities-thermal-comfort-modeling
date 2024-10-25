import os
import xarray as xr
from rasterio.enums import Resampling
from dask.diagnostics import ProgressBar
from shapely.geometry import Polygon
import geopandas as gp
from workers.tools import save_raster_file, save_vector_file

TARGET_VEG_CANOPY_FILENAME = 'veg_canopy'
TARGET_LULC_FILENAME = 'lulc'


def get_cif_data(target_path, folder_name_city_data, folder_name_tile_data, aoi_boundary):
    tile_data_path = os.path.join(target_path, folder_name_city_data, 'source_data', 'primary_source_data', folder_name_tile_data)

    d = {'geometry': [Polygon(aoi_boundary)]}
    aoi_gdf = gp.GeoDataFrame(d, crs="EPSG:4326")

    # get_era5(aoi_gdf)
    get_lulc(tile_data_path, aoi_gdf)
    get_canopy_height(tile_data_path, aoi_gdf)

    return

def get_era5(aoi_gdf):
    from city_metrix.metrics import era_5_met_preprocessing

    aoi_era_5 = era_5_met_preprocessing(aoi_gdf)
    b = 2

def get_lulc(tile_data_path, aoi_gdf):
    from workers.open_urban import OpenUrban, reclass_map

    # Load data
    lulc = OpenUrban().get_data(aoi_gdf.total_bounds)

    # Get resolution of the data
    lulc.rio.resolution()

    # Reclassify
    from xrspatial.classify import reclassify
    lulc_to_solweig = reclassify(lulc, bins=list(reclass_map.keys()), new_values=list(reclass_map.values()), name='lulc')

    # Remove zeros
    remove_value = 0
    count = count_occurrences(lulc_to_solweig, remove_value)
    if count > 0:
        print(f'Found {count} occurrences of the value {remove_value}. Removing...')
        lulc_to_solweig = lulc_to_solweig.where(lulc_to_solweig != remove_value, drop=True)
        count = count_occurrences(lulc_to_solweig, remove_value)
        print(f'There are {count} occurrences of the value {remove_value} after removing.')
    else:
        print(f'There were no occurrences of the value {remove_value} found in data.')

    # Save data to file
    save_raster_file(lulc_to_solweig, tile_data_path, TARGET_LULC_FILENAME)


def count_occurrences(data, value):
    return data.where(data == value).count().item()


def get_canopy_height(tile_data_path, aoi_gdf):
    from city_metrix.layers import TreeCanopyHeight

    # Load layer
    tree_canopy_height = TreeCanopyHeight().get_data(aoi_gdf.total_bounds)
    tree_canopy_height_float32 = tree_canopy_height.astype('float32')
    save_raster_file(tree_canopy_height_float32, tile_data_path, TARGET_VEG_CANOPY_FILENAME)

