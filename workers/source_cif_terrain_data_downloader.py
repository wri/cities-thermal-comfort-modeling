import os
import xarray as xr
from rasterio.enums import Resampling
from dask.diagnostics import ProgressBar
from shapely.geometry import Polygon
import geopandas as gp
from workers.tools import save_raster_file, save_vector_file

TARGET_DSM_BUILDING_FILENAME = 'dsm_ground_build'
TARGET_RESAMPLED_DEM_FILENAME = 'nasa_dem'

DEBUG = False
DEBUG_DSM_FILENAME = 'alos_dsm'
DEBUG_RESAMPLED_DSM_FILENAME = 'alos_dsm_1m'
DEBUG_DEM_FILENAME = 'nasa_dem'
DEBUG_OPEN_BUILDING_FOOTPRINT_FILENAME = 'open_building_footprints'
DEBUG_BUILDING_FOOTPRINT_FILENAME = 'building_footprints'


def get_cif_data(target_path, folder_name_city_data, folder_name_tile_data, aoi_boundary):
    tile_data_path = os.path.join(target_path, folder_name_city_data, 'source_data', 'primary_source_data',
                                  folder_name_tile_data)

    d = {'geometry': [Polygon(aoi_boundary)]}
    aoi_gdf = gp.GeoDataFrame(d, crs="EPSG:4326")

    alos_dsm_1m = get_dsm(tile_data_path, aoi_gdf)
    nasa_dem_1m = get_dem(tile_data_path, aoi_gdf)
    overture_buildings = get_building_footprints(tile_data_path, aoi_gdf)
    get_building_height(tile_data_path, overture_buildings, alos_dsm_1m, nasa_dem_1m)

    return


def get_dsm(tile_data_path, aoi_gdf):
    from city_metrix.layers import AlosDSM

    alos_dsm = AlosDSM().get_data(aoi_gdf.total_bounds)
    if DEBUG:
        save_raster_file(alos_dsm, tile_data_path, DEBUG_DSM_FILENAME)

    # resample to finer resolution of 1 meter
    alos_dsm_1m = resample_raster(alos_dsm, 1)
    if DEBUG:
        save_raster_file(alos_dsm_1m, tile_data_path, DEBUG_RESAMPLED_DSM_FILENAME)

    return alos_dsm_1m


def get_dem(tile_data_path, aoi_gdf):
    from city_metrix.layers import NasaDEM

    nasa_dem = NasaDEM().get_data(aoi_gdf.total_bounds)
    if DEBUG:
        save_raster_file(nasa_dem, tile_data_path, DEBUG_DEM_FILENAME)

    # resample to finer resolution of 1 meter
    nasa_dem_1m = resample_raster(nasa_dem, 1)
    save_raster_file(nasa_dem_1m, tile_data_path, TARGET_RESAMPLED_DEM_FILENAME)

    return nasa_dem_1m


def get_building_footprints(tile_data_path, aoi_gdf):
    from city_metrix.layers import OvertureBuildings

    overture_buildings = OvertureBuildings().get_data(aoi_gdf.total_bounds)
    if DEBUG:
        save_vector_file(overture_buildings, tile_data_path, DEBUG_BUILDING_FOOTPRINT_FILENAME)

    return overture_buildings


def get_building_height(tile_data_path, overture_buildings, alos_dsm_1m, nasa_dem_1m):
    # Below reads are for use during debugging
    # AlosDSM = read_tiff_file(tile_data_path, DSM_FILENAME)
    # AlosDSM_1m = read_tiff_file(tile_data_path, RESAMPLED_DSM_FILENAME)
    # NasaDEM = read_tiff_file(tile_data_path, DEM_FILENAME)
    # NasaDEM_1m = read_tiff_file(tile_data_path, RESAMPLED_DEM_FILENAME)

    from exactextract import exact_extract

    # re-apply crs
    target_crs = alos_dsm_1m.rio.crs
    overture_buildings = overture_buildings.to_crs(target_crs)

    # Determine extreme height ranges using the 1-meter terrain models. The higher resolution smooths the surface
    # where it changes abruptly from a tall structure to lower ones and generally improves estimates.
    overture_buildings['AlosDSM_max'] = (
        exact_extract(alos_dsm_1m, overture_buildings, ["max"], output='pandas')['max'])
    overture_buildings['NasaDEM_max'] = (
        exact_extract(nasa_dem_1m, overture_buildings, ["max"], output='pandas')['max'])
    overture_buildings['NasaDEM_min'] = (
        exact_extract(nasa_dem_1m, overture_buildings, ["min"], output='pandas')['min'])

    overture_buildings['Height_diff'] = (
            overture_buildings['AlosDSM_max'] - overture_buildings['NasaDEM_min'])
    overture_buildings['height_estimate'] = (
            overture_buildings['Height_diff'] + overture_buildings['NasaDEM_max'])

    overture_buildings['height_estimate'] = round(overture_buildings['height_estimate'], 0)

    # TODO Prototype for building-height refinement
    # prototype_refinement_building_height_estimates(overture_buildings)

    if DEBUG:
        save_vector_file(overture_buildings, tile_data_path, TARGET_DSM_BUILDING_FILENAME)

    # rasterize the building footprints
    overture_buildings = overture_buildings.drop(['AlosDSM_max', 'NasaDEM_max', 'NasaDEM_min', 'Height_diff'], axis=1)
    overture_buildings_raster = rasterize_polygons(overture_buildings, values=["height_estimate"],
                                                   snap_to_raster=nasa_dem_1m)

    # Save data to file
    save_raster_file(overture_buildings_raster, tile_data_path, TARGET_DSM_BUILDING_FILENAME)


def resample_raster(xarray, resolution_m):
    resampled_array = xarray.rio.reproject(
        dst_crs=xarray.rio.crs,
        resolution=resolution_m,
        resampling=Resampling.bilinear
    )
    return resampled_array


def rasterize_polygons(gdf, values=["Value"], snap_to_raster=None):
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


def prototype_refinement_building_height_estimates(overture_bldgs):
    # TODO This function is an initial exploration of using OSM tags to refine estimate of building height.
    # TODO Results are moderately successful for Amsterdam, but could be improved with additional building-height data.
    '''
    https://www.33rdsquare.com/how-tall-is-a-floor-meters/
    Single Family Homes: 2.4 – 2.7 meters (8 – 9 feet)
    Condominiums/Apartments: 2.1 – 2.4 meters (7 – 8 feet)
    Office Spaces: 2.4 – 3 meters (8 – 10 feet)
    Retail Stores: 3 – 4.5 meters (10 – 15 feet)
    Industrial Spaces: 3 – 6 meters (10 – 20 feet)
    class = transportation, sports_hall, service, school, retail, industrial, houseboat,house, commercial, apartments,
    class/subtype = {null, entertainment}
    '''

    house_typical_floors = 4
    apartment_typical_floors = 5
    industrial_typical_floors = 3
    commercial_typical_floors = 4
    houseboat_typical_floors = 1
    school_typical_floors = 3
    other_typical_floors = 2
    floor_height = 2.5
    overture_bldgs.loc[overture_bldgs['num_floors'].notnull(), 'inferred_height'] = (
                overture_bldgs['num_floors'] * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'apartments'), 'inferred_height'] = \
        (apartment_typical_floors * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'house'), 'inferred_height'] = \
        (house_typical_floors * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'industrial'), 'inferred_height'] = \
        (industrial_typical_floors * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'commercial'), 'inferred_height'] = \
        (commercial_typical_floors * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'commercial'), 'inferred_height'] = \
        (commercial_typical_floors * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'houseboat'), 'inferred_height'] = \
        (houseboat_typical_floors * floor_height)
    overture_bldgs.loc[
        (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'school'), 'inferred_height'] = \
        (school_typical_floors * floor_height)
    overture_bldgs['guessed_height'] = overture_bldgs['inferred_height'] + overture_bldgs['NasaDEM_min']

    overture_bldgs.loc[overture_bldgs['height'].notnull(), 'best_guess_height'] = overture_bldgs['height']
    overture_bldgs.loc[((overture_bldgs['best_guess_height'].isnull()) & (
        overture_bldgs['guessed_height'].isnull())), 'best_guess_height'] = \
        overture_bldgs['height_estimate']
    overture_bldgs.loc[(
                (overture_bldgs['best_guess_height'].isnull()) & (overture_bldgs['height_estimate'].notnull()) & (
            overture_bldgs['guessed_height'].notnull())), 'best_guess_height'] = \
        0.6 * overture_bldgs['height_estimate'] + 0.4 * overture_bldgs['guessed_height']
    overture_bldgs.loc[(
                (overture_bldgs['best_guess_height'].isnull()) & (overture_bldgs['height_estimate'].isnull()) & (
            overture_bldgs['inferred_height'].notnull())), 'best_guess_height'] = \
        overture_bldgs['inferred_height']
    overture_bldgs.loc[overture_bldgs['best_guess_height'].isnull(), 'best_guess_height'] = (
                other_typical_floors * floor_height)
