import os
import xarray as xr
import xee
import ee
from rasterio.enums import Resampling
from src.tools import get_application_path, remove_file
from dask.diagnostics import ProgressBar

from city_metrix.layers import layer, Layer
DEBUG = True

def get_cif_data(aoi_name, aoi_url):
    aoi_gdf = get_polygon_for_aoi(aoi_name, aoi_url)

    get_lulc(aoi_name, aoi_gdf)

    get_canopy_height(aoi_name, aoi_gdf)

    aoi_OvertureBuildings = get_building_footprints(aoi_name, aoi_gdf)
    aoi_AlosDSM = get_dsm(aoi_name, aoi_gdf)
    aoi_NasaDEM, dem_1m = get_dem(aoi_name, aoi_gdf)
    get_building_height(aoi_name, aoi_gdf, aoi_OvertureBuildings, aoi_AlosDSM, aoi_NasaDEM, dem_1m)
    get_era5()

    return


def get_lulc(aoi_name, aoi_gdf):
    # Load data
    aoi_LULC = OpenUrban().get_data(aoi_gdf.total_bounds)

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
    save_raster_file(aoi_LULC_to_solweig, aoi_name, 'lulc')

    # Check results
    if DEBUG:
        aoi_LULC_counts = aoi_LULC.groupby(aoi_LULC).count().to_dataframe()
        print(aoi_LULC_counts)

        aoi_LULC_to_solweig_counts = aoi_LULC_to_solweig.groupby(aoi_LULC_to_solweig).count().to_dataframe()
        print(aoi_LULC_to_solweig_counts)


def count_occurrences(data, value):
    return data.where(data == value).count().item()


def get_canopy_height(aoi_name, aoi_gdf):
    from city_metrix.layers import TreeCanopyHeight

    # Load layer
    aoi_TreeCanopyHeight = TreeCanopyHeight().get_data(aoi_gdf.total_bounds)

    # Save data to file
    save_raster_file(aoi_TreeCanopyHeight.astype('float32'), aoi_name, 'TreeCanopyHeight')

    if DEBUG:
        print_resolutions('TreeCanopyHeight', aoi_TreeCanopyHeight, None)

def get_building_footprints(aoi_name, aoi_gdf):
    from city_metrix.layers import OvertureBuildings

    # Load layer
    aoi_OvertureBuildings = OvertureBuildings().get_data(aoi_gdf.total_bounds)

    # Get row and column count
    # aoi_OvertureBuildings.shape

    # Save data to file
    save_vector_file(aoi_OvertureBuildings, aoi_name, 'OvertureBuildings')

    return aoi_OvertureBuildings

def get_dsm(aoi_name, aoi_gdf):
    from city_metrix.layers import AlosDSM

    aoi_AlosDSM = AlosDSM().get_data(aoi_gdf.total_bounds)
    save_raster_file(aoi_AlosDSM, aoi_name, 'aoi_AlosDSM')

    # resample to finer resolution of 1 meter
    dsm_1m = resample_raster(aoi_AlosDSM, 1)
    save_raster_file(dsm_1m, aoi_name, 'aoi_AlosDSM_1m')

    if DEBUG:
        print_resolutions('AlosDSM', aoi_AlosDSM, dsm_1m)

    return aoi_AlosDSM

def get_dem(aoi_name, aoi_gdf):
    from city_metrix.layers import NasaDEM

    aoi_NasaDEM = NasaDEM().get_data(aoi_gdf.total_bounds)
    save_raster_file(aoi_NasaDEM, aoi_name, 'aoi_NasaDEM')

    # resample to finer resolution of 1 meter
    dem_1m = resample_raster(aoi_NasaDEM, 1)
    save_raster_file(dem_1m, aoi_name, 'aoi_NasaDEM_1m')

    if DEBUG:
        print_resolutions('NasaDEM', aoi_NasaDEM, dem_1m)

    return aoi_NasaDEM, dem_1m

def get_building_height(aoi_name, aoi_gdf, aoi_OvertureBuildings, aoi_AlosDSM, aoi_NasaDEM, dem_1m):
    from exactextract import exact_extract

    aoi_OvertureBuildings = aoi_OvertureBuildings.to_crs(aoi_AlosDSM.rio.crs)

    # get maximum raster values for rasters intersecting the building footprints
    aoi_OvertureBuildings['AlosDSM_max'] = (
        exact_extract(aoi_AlosDSM, aoi_OvertureBuildings, ["max"], output='pandas')['max'])
    aoi_OvertureBuildings['NasaDEM_max'] = (
        exact_extract(aoi_NasaDEM, aoi_OvertureBuildings, ["max"], output='pandas')['max'])
    aoi_OvertureBuildings['height_max'] = (
            aoi_OvertureBuildings['AlosDSM_max'] - aoi_OvertureBuildings['NasaDEM_max'])

    # Get row and column count
    # aoi_OvertureBuildings.shape

    # Write to file
    save_vector_file(aoi_OvertureBuildings, aoi_name, 'BuildingHeights')

    # rasterize the building footprints
    aoi_OvertureBuildings_raster = rasterize_polygon(aoi_OvertureBuildings, values=["height_max"], snap_to_raster=dem_1m)

    # Save data to file
    save_raster_file(aoi_OvertureBuildings_raster, aoi_name, 'aoi_building_height')

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


def save_vector_file(vector_geodataframe, aoi_name, file_name_qualifier):
    file_path = f'{file_path_prefix(aoi_name)}-{file_name_qualifier}.geojson'
    remove_file(file_path)
    vector_geodataframe.to_file(file_path, driver='GeoJSON')
    print(f'File saved to {file_path}')


def save_raster_file(raster_data_array, aoi_name, file_name_qualifier):
    file_path = f'{file_path_prefix(aoi_name)}-{file_name_qualifier}.tif'
    remove_file(file_path)
    raster_data_array.rio.to_raster(raster_path=file_path, driver="COG")
    print(f'\nFile saved to {file_path}')

def print_resolutions(dataset_name, source_xarray, resampled_array):
    source_res = source_xarray.rio.resolution()
    if resampled_array:
        resampled_res = resampled_array.rio.resolution()
    else:
        resampled_res = 'N/A'
    print(f'\nResolutions for {dataset_name}dataset: source_res:{source_res}, resampled_res:{resampled_res}')

def get_polygon_for_aoi(aoi_name, aoi_url):
    # load boundary
    import geopandas as gpd

    # If you are using an SSO account, you need to be authenticated first
    # !aws sso login
    aoi_gdf = gpd.read_file(aoi_url, driver='GeoJSON')

    aoi_gdf = aoi_gdf.to_crs(epsg=4326)

    ## Write to file
    file_path = f'{file_path_prefix(aoi_name)}-boundary.geojson'
    remove_file(file_path)
    aoi_gdf.to_file(file_path, driver='GeoJSON')
    print(f'\nAOI boundary saved to:{file_path}')

    ## Get area in km2 of the city rounded to the nearest integer
    # pseudo_mercator_proj = 3857
    equal_earth_proj = 8857
    sq_m_to_sq_km_factor = 10**6
    aoi_gdf_area = aoi_gdf['geometry'].to_crs(epsg=equal_earth_proj).area/sq_m_to_sq_km_factor  # in km2
    aoi_gdf_area = round(aoi_gdf_area.values[0], 3)
    print(f'Area: {aoi_gdf_area} sqkm')

    return aoi_gdf


    ## Data folder
def file_path_prefix(file_prefix):
    app_path = get_application_path()
    folder = str(os.path.join(app_path, 'data', file_prefix))
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, file_prefix)


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