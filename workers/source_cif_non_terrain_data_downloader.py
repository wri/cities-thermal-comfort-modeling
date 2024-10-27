import os
from shapely.geometry import Polygon
import geopandas as gp

from workers.city_data import CityData
from workers.tools import save_tiff_file

def get_cif_data(output_base_path, folder_name_city_data, folder_name_tile_data, aoi_boundary,
                 retrieve_era5=False, retrieve_lulc=False, retrieve_tree_canopy=False):
    city_data = CityData(folder_name_city_data, folder_name_tile_data, output_base_path, None)
    tile_data_path = city_data.source_tile_data_path

    d = {'geometry': [Polygon(aoi_boundary)]}
    aoi_gdf = gp.GeoDataFrame(d, crs="EPSG:4326")

    if retrieve_era5:(
        get_era5(aoi_gdf))
    if retrieve_lulc:
        get_lulc(tile_data_path, aoi_gdf)
    if retrieve_tree_canopy:
        get_tree_canopy_height(tile_data_path, aoi_gdf)

    return

def get_era5(aoi_gdf):
    from city_metrix.metrics import era_5_met_preprocessing

    aoi_era_5 = era_5_met_preprocessing(aoi_gdf.total_bounds)
    b = 2

def get_lulc(tile_data_path, aoi_gdf):
    from workers.open_urban import OpenUrban, reclass_map

    # Load data
    lulc = OpenUrban().get_data(aoi_gdf.total_bounds)

    # Get resolution of the data
    lulc.rio.resolution()

    # Reclassify
    from xrspatial.classify import reclassify
    lulc_to_solweig_class = reclassify(lulc, bins=list(reclass_map.keys()), new_values=list(reclass_map.values()), name='lulc')

    # Remove zeros
    remove_value = 0
    count = count_occurrences(lulc_to_solweig_class, remove_value)
    if count > 0:
        print(f'Found {count} occurrences of the value {remove_value}. Removing...')
        lulc_to_solweig_class = lulc_to_solweig_class.where(lulc_to_solweig_class != remove_value, drop=True)
        count = count_occurrences(lulc_to_solweig_class, remove_value)
        print(f'There are {count} occurrences of the value {remove_value} after removing.')
    else:
        print(f'There were no occurrences of the value {remove_value} found in data.')

    # Save data to file
    save_tiff_file(lulc_to_solweig_class, tile_data_path, CityData.filename_cif_lulc)


def count_occurrences(data, value):
    return data.where(data == value).count().item()


def get_tree_canopy_height(tile_data_path, aoi_gdf):
    from city_metrix.layers import TreeCanopyHeight

    # Load layer
    tree_canopy_height = TreeCanopyHeight().get_data(aoi_gdf.total_bounds)
    tree_canopy_height_float32 = tree_canopy_height.astype('float32')
    save_tiff_file(tree_canopy_height_float32, tile_data_path, CityData.filename_cif_tree_canopy)

