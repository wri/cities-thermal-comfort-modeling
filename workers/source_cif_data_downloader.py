import xarray as xr
from rasterio.enums import Resampling
from dask.diagnostics import ProgressBar
import shapely.wkt
import geopandas as gp
import numpy as np
from datetime import datetime

from workers.city_data import CityData
from workers.worker_tools import compute_time_diff_mins, save_tiff_file, save_geojson_file, log_method_failure

# Unify the layers on the same resolution
DEFAULT_LULC_RESOLUTION = 1
DEBUG = False

def get_cif_data(task_index, output_base_path, folder_name_city_data, tile_id, features, tile_boundary, tile_resolution=None):
    start_time = datetime.now()

    city_data = CityData(folder_name_city_data, tile_id, output_base_path, None)
    tile_data_path = city_data.source_tile_data_path

    d = {'geometry': [shapely.wkt.loads(tile_boundary)]}
    aoi_gdf = gp.GeoDataFrame(d, crs="EPSG:4326")

    feature_list = features.split(',')

    output_resolution = int(tile_resolution) if tile_resolution is not None else DEFAULT_LULC_RESOLUTION

    result_flags = []
    if 'era5' in feature_list:
        get_era5(aoi_gdf)

    if 'lulc' in feature_list:
        this_success = get_lulc(city_data, tile_data_path, aoi_gdf, output_resolution)
        result_flags.append(this_success)

    if 'tree_canopy' in feature_list:
        this_success = get_tree_canopy_height(city_data, tile_data_path, aoi_gdf, output_resolution)
        result_flags.append(this_success)

    if 'dem' in feature_list or 'dsm' in feature_list:
        retrieve_dem = True if 'dem' in feature_list else False
        this_success, nasa_dem_1m = get_dem(city_data, tile_data_path, aoi_gdf, retrieve_dem, output_resolution)
        result_flags.append(this_success)
    else:
        nasa_dem_1m = None

    if 'dsm' in feature_list:
        this_success, alos_dsm_1m = get_dsm(tile_data_path, aoi_gdf, output_resolution)
        result_flags.append(this_success)
        this_success, overture_buildings = get_building_footprints(tile_data_path, aoi_gdf)
        result_flags.append(this_success)
        this_success = get_building_height_dsm(city_data, tile_data_path, overture_buildings, alos_dsm_1m, nasa_dem_1m)
        result_flags.append(this_success)

    run_duration_min = compute_time_diff_mins(start_time)
    return_code = 0 if all(result_flags) else 1

    run_duration_min = compute_time_diff_mins(start_time)

    met_filename_str = 'N/A'
    step_method = 'retrieve_cif_data'
    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    return_stdout = (f'{{"task_index": {task_index}, "tile": "{tile_id}", "step_index": {0}, '
                     f'"step_method": "{step_method}", "met_filename": "{met_filename_str}", "return_code": {return_code}, '
                     f'"start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}')

    return return_stdout


def get_era5(aoi_gdf):
    from city_metrix.metrics import era_5_met_preprocessing

    aoi_era_5 = era_5_met_preprocessing(aoi_gdf.total_bounds)

def get_lulc(city_data, tile_data_path, aoi_gdf, output_resolution):
    try:
        from workers.open_urban import OpenUrban, reclass_map

        # Load data
        lulc = OpenUrban().get_data(aoi_gdf.total_bounds)

        # Get resolution of the data
        # lulc.rio.resolution()

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

        if output_resolution != DEFAULT_LULC_RESOLUTION:
            lulc_to_solweig_class = resample_categorical_raster(lulc_to_solweig_class, output_resolution)

        # Save data to file
        save_tiff_file(lulc_to_solweig_class, tile_data_path, city_data.lulc_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Lulc processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, city_data.source_base_path, '')
        return False


def count_occurrences(data, value):
    return data.where(data == value).count().item()


def get_tree_canopy_height(city_data, tile_data_path, aoi_gdf, output_resolution):
    try:
        from city_metrix.layers import TreeCanopyHeight

        # Load layer
        tree_canopy_height = TreeCanopyHeight(spatial_resolution=output_resolution).get_data(aoi_gdf.total_bounds)
        tree_canopy_height_float32 = tree_canopy_height.astype('float32')
        save_tiff_file(tree_canopy_height_float32, tile_data_path, city_data.tree_canopy_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Tree-canopy processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, city_data.source_base_path, '')
        return False

def get_dem(city_data, tile_data_path, aoi_gdf, retrieve_dem, output_resolution):
    try:
        from city_metrix.layers import NasaDEM

        nasa_dem_1m = NasaDEM(spatial_resolution=output_resolution).get_data(aoi_gdf.total_bounds)
        if retrieve_dem:
            save_tiff_file(nasa_dem_1m, tile_data_path, city_data.dem_tif_filename)

        return True, nasa_dem_1m

    except Exception as e_msg:
        msg = f'DEM processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, city_data.source_base_path, '')
        return False, None


def get_dsm(tile_data_path, aoi_gdf, output_resolution):
    try:
        from city_metrix.layers import AlosDSM

        alos_dsm_1m = AlosDSM(spatial_resolution=output_resolution).get_data(aoi_gdf.total_bounds)
        if DEBUG:
            save_tiff_file(alos_dsm_1m, tile_data_path, 'debug_alos_dsm_1m.tif')

        return True, alos_dsm_1m

    except Exception as e_msg:
        msg = f'DSM processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, None, '')
        return False, None


def get_building_footprints(tile_data_path, aoi_gdf):
    try:
        from city_metrix.layers import OvertureBuildings

        overture_buildings = OvertureBuildings().get_data(aoi_gdf.total_bounds)
        if DEBUG:
            save_geojson_file(overture_buildings, tile_data_path, 'debug_building_footprints.geojson')

        return True, overture_buildings

    except Exception as e_msg:
        msg = f'Building-footprint processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, None, '')
        return False, None


def get_building_height_dsm(city_data, tile_data_path, overture_buildings, alos_dsm_1m, nasa_dem_1m):
    try:
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

        overture_buildings_raster = rasterize_polygons(overture_buildings, values=["height_estimate"],
                                                       snap_to_raster=nasa_dem_1m)
        if DEBUG:
            save_tiff_file(overture_buildings_raster, tile_data_path, 'debug_raw_building_footprints.tif')

        composite_bldg_dem = combine_building_and_dem(nasa_dem_1m, overture_buildings_raster, target_crs)

        # Save data to file
        save_tiff_file(composite_bldg_dem, tile_data_path, city_data.dsm_tif_filename)
        return True

    except Exception as e_msg:
        msg = f'Building DSM processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, None, '')
        return False

def combine_building_and_dem(dem, buildings, target_crs):
    coords_dict = {dim: dem.coords[dim].values for dim in dem.dims}

    # Conver to ndarray in order to mask and combine layers
    dem_nda = dem.to_numpy()
    bldg_nda = buildings.to_dataarray().to_numpy().reshape(dem_nda.shape)

    # Mask of building cells
    mask = (bldg_nda != -9999) & (~np.isnan(bldg_nda))

    # Mask buildings onto DEM
    dem_nda[mask] = bldg_nda[mask]

    #Conver resuls into DataArray and re-add coordinates and CRS
    composite_xa = xr.DataArray(dem_nda,
                      dims = ["y","x"],
                      coords = coords_dict
                      )
    composite_xa.rio.write_crs(target_crs, inplace=True)

    return composite_xa

def resample_categorical_raster(xarray, resolution_m):
    resampled_array = xarray.rio.reproject(
        dst_crs=xarray.rio.crs,
        resolution=resolution_m,
        resampling=Resampling.nearest
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
            # fill=0
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
