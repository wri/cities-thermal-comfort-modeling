import os
import rioxarray
import xarray as xr
# import xee
import ee
# import rasterio
import geopandas as gpd
from rasterio.enums import Resampling
from src.tools import remove_file, create_folder
from dask.diagnostics import ProgressBar
from shapely.geometry import Polygon
from city_metrix.layers import layer, Layer

DEBUG = True

TREE_CANOPY_FILE_NAME = 'tree_canopy'
BUILDING_FOOTPRINT_FILE_NAME = 'building_footprints'
DSM_FILE_NAME = 'alos_dsm'
DEM_FILE_NAME = 'nasa_dem'
RESAMPLED_DEM_FILE_NAME = 'nasa_dem_1m'
BUILDING_HEIGHT_FILE_NAME = 'building_height'
LAND_COVER_FILE_NAME = 'land_cover'

def get_cif_data(target_path, folder_name_city_data, folder_name_tile_data, aoi_boundary):
    tile_data_path = os.path.join(target_path, folder_name_city_data, 'source_data', 'primary_source_data', folder_name_tile_data)

    aoi_bounds = Polygon(aoi_boundary).bounds

    get_lulc(tile_data_path, aoi_bounds)
    get_canopy_height(tile_data_path, aoi_bounds)
    get_building_footprints(tile_data_path, aoi_bounds)
    get_dsm(tile_data_path, aoi_bounds)
    get_dem(tile_data_path, aoi_bounds)
    get_building_height(tile_data_path)
    # get_era5()

    return


def get_lulc(tile_data_path, aoi_bounds):
    # Load data
    aoi_LULC = OpenUrban().get_data(aoi_bounds)

    # Get resolution of the data
    aoi_LULC.rio.resolution()

    # Reclassify
    from xrspatial.classify import reclassify
    aoi_LULC_to_solweig = reclassify(aoi_LULC, bins=list(reclass_map.keys()), new_values=list(reclass_map.values()), name='lulc')

    # Remove zeros
    remove_value = 0
    count = count_occurrences(aoi_LULC_to_solweig, remove_value)
    if count > 0:
        print(f'Found {count} occurrences of the value {remove_value}. Removing...')
        aoi_LULC_to_solweig = aoi_LULC_to_solweig.where(aoi_LULC_to_solweig != remove_value, drop=True)
        count = count_occurrences(aoi_LULC_to_solweig, remove_value)
        print(f'There are {count} occurrences of the value {remove_value} after removing.')
    else:
        print(f'There were no occurrences of the value {remove_value} found in data.')

    # Save data to file
    save_raster_file(aoi_LULC_to_solweig, tile_data_path, LAND_COVER_FILE_NAME)


def count_occurrences(data, value):
    return data.where(data == value).count().item()


def get_canopy_height(tile_data_path, aoi_bounds):
    from city_metrix.layers import TreeCanopyHeight

    # Load layer
    aoi_TreeCanopyHeight = TreeCanopyHeight().get_data(aoi_bounds)
    aoi_TreeCanopyHeight_float32 = aoi_TreeCanopyHeight.astype('float32')

    save_raster_file(aoi_TreeCanopyHeight_float32, tile_data_path, TREE_CANOPY_FILE_NAME)


def get_building_footprints(tile_data_path, aoi_bounds):
    from city_metrix.layers import OvertureBuildings

    aoi_OvertureBuildings = OvertureBuildings().get_data(aoi_bounds)

    save_vector_file(aoi_OvertureBuildings, tile_data_path, BUILDING_FOOTPRINT_FILE_NAME)


def get_dsm(tile_data_path, aoi_bounds):
    from city_metrix.layers import AlosDSM

    aoi_AlosDSM = AlosDSM().get_data(aoi_bounds)

    save_raster_file(aoi_AlosDSM, tile_data_path, DSM_FILE_NAME)

    # resample to finer resolution of 1 meter
    # dsm_1m = resample_raster(aoi_AlosDSM, 1)
    # save_raster_file(dsm_1m, aoi_name, 'aoi_AlosDSM_1m')


def get_dem(tile_data_path, aoi_bounds):
    from city_metrix.layers import NasaDEM

    aoi_NasaDEM = NasaDEM().get_data(aoi_bounds)

    save_raster_file(aoi_NasaDEM, tile_data_path, DEM_FILE_NAME)

    # resample to finer resolution of 1 meter
    dem_1m = resample_raster(aoi_NasaDEM, 1)

    save_raster_file(dem_1m, tile_data_path, RESAMPLED_DEM_FILE_NAME)


def get_building_height(tile_data_path):
    aoi_OvertureBuildings = read_vector_file(tile_data_path, BUILDING_FOOTPRINT_FILE_NAME)
    aoi_AlosDSM = read_tiff_file(tile_data_path, DSM_FILE_NAME)
    aoi_NasaDEM = read_tiff_file(tile_data_path, DEM_FILE_NAME)
    aoi_NasaDEM_1m = read_tiff_file(tile_data_path, RESAMPLED_DEM_FILE_NAME)

    # (aoi_name, aoi_gdf, aoi_OvertureBuildings, aoi_AlosDSM, aoi_NasaDEM, dem_1m):
    from exactextract import exact_extract

    target_crs = aoi_AlosDSM.rio.crs
    aoi_OvertureBuildings = aoi_OvertureBuildings.to_crs(target_crs)

    # get maximum raster values for rasters intersecting the building footprints
    aoi_OvertureBuildings['AlosDSM_max'] = (
        exact_extract(aoi_AlosDSM, aoi_OvertureBuildings, ["max"], output='pandas')['max'])
    aoi_OvertureBuildings['NasaDEM_max'] = (
        exact_extract(aoi_NasaDEM, aoi_OvertureBuildings, ["max"], output='pandas')['max'])
    aoi_OvertureBuildings['height_max'] = (
            aoi_OvertureBuildings['AlosDSM_max'] - aoi_OvertureBuildings['NasaDEM_max'])

    # Write to file
    save_vector_file(aoi_OvertureBuildings, tile_data_path, BUILDING_HEIGHT_FILE_NAME)

    # rasterize the building footprints
    aoi_OvertureBuildings_raster = rasterize_polygon(aoi_OvertureBuildings, values=["height_max"], snap_to_raster=aoi_NasaDEM_1m)

    # Save data to file
    save_raster_file(aoi_OvertureBuildings_raster, tile_data_path, BUILDING_HEIGHT_FILE_NAME)

def get_era5():
    return


def resample_raster(xarray, resolution_m):
    resampled_array = xarray.rio.reproject(
        dst_crs=xarray.rio.crs,
        resolution=resolution_m,
        resampling=Resampling.bilinear
    )
    return resampled_array


def rasterize_polygon(gdf, values=["Value"], snap_to_raster=None):
    from geocube.api.core import make_geocube
    if gdf.empty:
        feature_1m = xr.zeros_like(snap_to_raster)
    else:
        feature_1m = make_geocube(
            vector_data=gdf,
            measurements=values,
            like=snap_to_raster,
            fill=0
        )

    return feature_1m

def save_raster_file(raster_data_array, tile_data_path, tiff_data_file_name):
    create_folder(tile_data_path)
    file_path = os.path.join(tile_data_path, f'{tiff_data_file_name}.tif')
    remove_file(file_path)
    raster_data_array.rio.to_raster(raster_path=file_path, driver="COG")


def save_vector_file(vector_geodataframe, tile_data_path, tiff_data_file_name):
    create_folder(tile_data_path)
    file_path = os.path.join(tile_data_path, f'{tiff_data_file_name}.geojson')
    remove_file(file_path)
    vector_geodataframe.to_file(file_path, driver='GeoJSON')


def read_tiff_file(tile_data_path, file_name):
    file_path = os.path.join(tile_data_path, f'{file_name}.tif')
    raster_data = rioxarray.open_rasterio(file_path)
    return raster_data

def read_vector_file(tile_data_path, file_name):
    file_path = os.path.join(tile_data_path, f'{file_name}.geojson')
    vector_data = gpd.read_file(file_path)
    return vector_data


class OpenUrban(Layer):
    def __init__(self, band='b1', **kwargs):
        super().__init__(**kwargs)
        self.band = band

    def get_data(self, bbox):
        dataset = ee.ImageCollection("projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC")
        ## It is important if the cif code is pulling data from GEE to take the maximum value where the image tiles overlap

        # Check for data
        if dataset.filterBounds(ee.Geometry.BBox(*bbox)).size().getInfo() == 0:
            print("No Data Available")
        else:
            ulu = ee.ImageCollection(dataset
                                     .filterBounds(ee.Geometry.BBox(*bbox))
                                     .select(self.band)
                                     .reduce(ee.Reducer.firstNonNull())
                                     .rename('lulc')
                                     )

        data = layer.get_image_collection(ulu, bbox, 1, "urban land use").lulc

        return data

# Define reclassification
from enum import Enum

# From https://gfw.atlassian.net/wiki/spaces/CIT/pages/872349733/Surface+characteristics+by+LULC#Major-update-to-LULC-codes
class OpenUrbanClass(Enum):
    GREEN_SPACE_OTHER = 110.0
    BUILT_UP_OTHER = 120.0
    BARREN = 130.0
    PUBLIC_OPEN_SPACE = 200.0
    WATER = 300.0
    PARKING = 400.0
    ROADS = 500.0
    BUILDINGS_UNCLASSIFIED = 600.0
    BUILDINGS_UNCLASSIFIED_LOW_SLOPE = 601.0
    BUILDINGS_UNCLASSIFIED_HIGH_SLOPE = 602.0
    BUILDINGS_RESIDENTIAL = 610.0
    BUILDINGS_RESIDENTIAL_LOW_SLOPE = 611.0
    BUILDINGS_RESIDENTIAL_HIGH_SLOPE = 612.0
    BUILDINGS_NON_RESIDENTIAL = 620.0
    BUILDINGS_NON_RESIDENTIAL_LOW_SLOPE = 621.0
    BUILDINGS_NON_RESIDENTIAL_HIGH_SLOPE = 622.0

# Note, it seems these have to be in the same order as the OpenUrbanClass
reclass_map = {
    OpenUrbanClass.GREEN_SPACE_OTHER.value: 5.0,
    OpenUrbanClass.BUILT_UP_OTHER.value: 1.0,
    OpenUrbanClass.BARREN.value: 6.0,
    OpenUrbanClass.PUBLIC_OPEN_SPACE.value: 5.0,
    OpenUrbanClass.WATER.value: 7.0,
    OpenUrbanClass.PARKING.value: 1.0,
    OpenUrbanClass.ROADS.value: 1.0,
    OpenUrbanClass.BUILDINGS_UNCLASSIFIED.value: 2.0,
    OpenUrbanClass.BUILDINGS_UNCLASSIFIED_LOW_SLOPE.value: 2.0,
    OpenUrbanClass.BUILDINGS_UNCLASSIFIED_HIGH_SLOPE.value: 2.0,
    OpenUrbanClass.BUILDINGS_RESIDENTIAL.value: 2.0,
    OpenUrbanClass.BUILDINGS_RESIDENTIAL_LOW_SLOPE.value: 2.0,
    OpenUrbanClass.BUILDINGS_RESIDENTIAL_HIGH_SLOPE.value: 2.0,
    OpenUrbanClass.BUILDINGS_NON_RESIDENTIAL.value: 2.0,
    OpenUrbanClass.BUILDINGS_NON_RESIDENTIAL_LOW_SLOPE.value: 2.0,
    OpenUrbanClass.BUILDINGS_NON_RESIDENTIAL_HIGH_SLOPE.value: 2.0,
    }