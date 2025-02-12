import xarray as xr
import shapely.wkt
import geopandas as gp
import numpy as np
import random
import time

from city_metrix.layers.layer_geometry import GeoExtent
from city_metrix.layers.layer_tools import standardize_y_dimension_direction
from rasterio.enums import Resampling
from datetime import datetime
from src.constants import VALID_PRIMARY_TYPES
from src.workers.city_data import CityData
from src.workers.logger_tools import setup_logger, log_method_start, log_method_completion, log_method_failure
from src.workers.worker_tools import compute_time_diff_mins, save_tiff_file, save_geojson_file, \
    unpack_quoted_value

DEBUG = False

# Unify the layers on the same resolution
DEFAULT_LULC_RESOLUTION = 1

MINIMUM_QUERY_WAIT_SEC = 10
MAXIMUM_QUERY_WAIT_SEC = 30

def get_cif_data(source_base_path, target_base_path, folder_name_city_data, tile_id, cif_primary_features,
                 tile_boundary, crs, tile_resolution):
    start_time = datetime.now()

    tiled_city_data = CityData(folder_name_city_data, tile_id, source_base_path, target_base_path)

    logger = setup_logger(tiled_city_data.target_model_log_path)
    log_method_start(f'CIF download: ({cif_primary_features}) in tile {tile_id}', None, '', logger)

    tile_cif_data_path = tiled_city_data.target_primary_tile_data_path

    d = {'geometry': [shapely.wkt.loads(tile_boundary)]}
    tiled_aoi_gdf = gp.GeoDataFrame(d, crs=crs)

    feature_list = cif_primary_features.split(',')

    tile_resolution = unpack_quoted_value(tile_resolution)
    output_resolution = DEFAULT_LULC_RESOLUTION if tile_resolution is None else tile_resolution

    result_flags = []
    # randomize retrieval to reduce contention against GEE
    number_of_retrieval_groups = len(VALID_PRIMARY_TYPES)-1
    random_group_sequence = _generate_unique_random_list(start=1, end=number_of_retrieval_groups, count = number_of_retrieval_groups)
    for group in random_group_sequence:
        # wait random length of time to help reduce contention and GEE throttling
        wait_time_sec = random.uniform(MINIMUM_QUERY_WAIT_SEC, MAXIMUM_QUERY_WAIT_SEC)
        if group == 1:
            if 'albedo' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-albedo download for {tile_id}', None, '', logger)
                this_success = _get_albedo(tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-albedo download for tile {tile_id}', None, '', logger)
        elif group == 2:
            if 'lulc' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-lulc download for {tile_id}', None, '', logger)
                this_success = _get_lulc(tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-lulc download for tile {tile_id}', None, '', logger)
        elif group == 3:
            if 'tree_canopy' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-Tree_canopy download for {tile_id}', None, '', logger)
                this_success = _get_tree_canopy_height(tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time,f'CIF-Tree_canopy download for {tile_id}', None, '', logger)
        elif group == 4:
            time.sleep(wait_time_sec)
            if 'dem' in feature_list or 'dsm' in feature_list:
                retrieve_dem = True if 'dem' in feature_list else False
                log_method_start(f'CIF-DEM download for {tile_id}', None, '', logger)
                this_success, nasa_dem = _get_dem(tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, retrieve_dem, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-DEM download for {tile_id}', None, '', logger)
            else:
                nasa_dem = None

            if 'dsm' in feature_list:
                log_method_start(f'CIF-DSM download for {tile_id}', None, '', logger)
                this_success, alos_dsm = _get_dsm(tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                this_success, overture_buildings = _get_building_footprints(tile_cif_data_path, tiled_aoi_gdf, logger)
                result_flags.append(this_success)
                this_success = _get_building_height_dsm(tiled_city_data, tile_cif_data_path, overture_buildings, alos_dsm, nasa_dem, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-DSM download for {tile_id}', None, '', logger)

    return_code = 0 if all(result_flags) else 1
    run_duration_min = compute_time_diff_mins(start_time)

    met_filename_str = 'N/A'
    step_method = 'retrieve_cif_data'
    start_time_str = start_time.strftime('%Y_%m_%d_%H:%M:%S')
    return_stdout = (f'{{"tile": "{tile_id}", "step_index": {0}, '
                     f'"step_method": "{step_method}", "met_filename": "{met_filename_str}", "return_code": {return_code}, '
                     f'"start_time": "{start_time_str}", "run_duration_min": {run_duration_min}}}')

    log_method_completion(start_time, f'CIF download: ({cif_primary_features}) in tile {tile_id}', None, '', logger)

    return return_stdout

def _generate_unique_random_list(start, end, count):
    if count > (end - start + 1):
        raise ValueError("Count is larger than the range of unique values available.")
    return random.sample(range(start, end + 1), count)


def _random_list(in_list):
    random.shuffle(in_list)
    return in_list


def _get_albedo(city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import Albedo

        # Load layer
        bbox = GeoExtent(aoi_gdf.total_bounds, aoi_gdf.crs.srs)
        albedo = Albedo().get_data(bbox, spatial_resolution=output_resolution)
        albedo_float32 = albedo.astype('float32')

        # reverse y direction, if y values increase in NS direction from LL corner
        was_reversed, albedo_float32 = standardize_y_dimension_direction(albedo_float32)

        save_tiff_file(albedo_float32, tile_data_path, city_data.albedo_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Albedo processing cancelled due to failure.'
        log_method_failure(datetime.now(), msg, None, None, city_data.source_base_path, '', logger)
        return False


def _get_lulc(tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from src.workers.open_urban import OpenUrban, reclass_map

        # Load data
        bbox = GeoExtent(aoi_gdf.total_bounds, aoi_gdf.crs.srs)
        lulc = OpenUrban().get_data(bbox)

        # Get resolution of the data
        # lulc.rio.resolution()

        # Reclassify
        from xrspatial.classify import reclassify
        lulc_to_solweig_class = reclassify(lulc, bins=list(reclass_map.keys()), new_values=list(reclass_map.values()), name='lulc')

        # Remove zeros
        remove_value = 0
        count = _count_occurrences(lulc_to_solweig_class, remove_value)
        if count > 0:
            print(f'Found {count} occurrences of the value {remove_value}. Removing...')
            lulc_to_solweig_class = lulc_to_solweig_class.where(lulc_to_solweig_class != remove_value, drop=True)
            count = _count_occurrences(lulc_to_solweig_class, remove_value)
            print(f'There are {count} occurrences of the value {remove_value} after removing.')
        else:
            print(f'There were no occurrences of the value {remove_value} found in data.')

        # TODO Can we specify resolution through GEE and avoid below?
        if output_resolution != DEFAULT_LULC_RESOLUTION:
            lulc_to_solweig_class = _resample_categorical_raster(lulc_to_solweig_class, output_resolution)

        # reverse y direction, if y values increase in NS direction from LL corner
        was_reversed, lulc_to_solweig_class = standardize_y_dimension_direction(lulc_to_solweig_class)

        # Save data to file
        save_tiff_file(lulc_to_solweig_class, tile_data_path, tiled_city_data.lulc_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Lulc processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'lulc', tiled_city_data.source_base_path, msg, logger)
        return False


def _count_occurrences(data, value):
    return data.where(data == value).count().item()


def _get_tree_canopy_height(tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import TreeCanopyHeight

        # Load layer
        bbox = GeoExtent(aoi_gdf.total_bounds, aoi_gdf.crs.srs)
        tree_canopy_height = TreeCanopyHeight().get_data(bbox, spatial_resolution=output_resolution)
        tree_canopy_height_float32 = tree_canopy_height.astype('float32')

        # reverse y direction, if y values increase in NS direction from LL corner
        was_reversed, tree_canopy_height_float32 = standardize_y_dimension_direction(tree_canopy_height_float32)

        save_tiff_file(tree_canopy_height_float32, tile_data_path, tiled_city_data.tree_canopy_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Tree-canopy processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'tree_canopy_height', tiled_city_data.source_base_path, msg, logger)
        return False


def _get_dem(tiled_city_data, tile_data_path, aoi_gdf, retrieve_dem, output_resolution, logger):
    try:
        from city_metrix.layers import NasaDEM

        bbox = GeoExtent(aoi_gdf.total_bounds, aoi_gdf.crs.srs)
        nasa_dem = NasaDEM().get_data(bbox, spatial_resolution=output_resolution)

        # reverse y direction, if y values increase in NS direction from LL corner
        was_reversed, nasa_dem = standardize_y_dimension_direction(nasa_dem)

        if retrieve_dem:
            save_tiff_file(nasa_dem, tile_data_path, tiled_city_data.dem_tif_filename)

        return True, nasa_dem

    except Exception as e_msg:
        msg = f'DEM processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'DEM', tiled_city_data.source_base_path, msg, logger)
        return False, None


def _get_dsm(tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import AlosDSM

        bbox = GeoExtent(aoi_gdf.total_bounds, aoi_gdf.crs.srs)
        alos_dsm = AlosDSM().get_data(bbox, spatial_resolution=output_resolution)

        # reverse y direction, if y values increase in NS direction from LL corner
        was_reversed, alos_dsm = standardize_y_dimension_direction(alos_dsm)

        if DEBUG:
            save_tiff_file(alos_dsm, tile_data_path, 'debug_alos_dsm.tif')

        return True, alos_dsm

    except Exception as e_msg:
        msg = f'DSM processing cancelled due to failure f{e_msg}.'
        log_method_failure(datetime.now(), 'DSM', tile_data_path, msg, logger)
        return False, None


def _get_building_footprints(tile_data_path, aoi_gdf, logger):
    try:
        from city_metrix.layers import OvertureBuildings

        bbox = GeoExtent(aoi_gdf.total_bounds, aoi_gdf.crs.srs)
        overture_buildings = OvertureBuildings().get_data(bbox)
        if DEBUG:
            save_geojson_file(overture_buildings, tile_data_path, 'debug_building_footprints.geojson')

        return True, overture_buildings

    except Exception as e_msg:
        msg = f'Building-footprint processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'Building_footprint', tile_data_path, msg, logger)
        return False, None


def _get_building_height_dsm(tiled_city_data, tile_data_path, overture_buildings, alos_dsm, nasa_dem, logger):
    try:
        from exactextract import exact_extract

        # re-apply crs
        target_crs = alos_dsm.rio.crs
        overture_buildings = overture_buildings.to_crs(target_crs)

        base_dtm = _create_base_dtm(alos_dsm, nasa_dem)

        # Determine extreme height ranges using the 1-meter terrain models. The higher resolution smooths the surface
        # where it changes abruptly from a tall structure to lower ones and generally improves estimates.
        overture_buildings['AlosDSM_max'] = (
            exact_extract(alos_dsm, overture_buildings, ["max"], output='pandas')['max'])
        overture_buildings['BaseDTM_max'] = (
            exact_extract(nasa_dem, overture_buildings, ["max"], output='pandas')['max'])
        overture_buildings['BaseDTM_min'] = (
            exact_extract(nasa_dem, overture_buildings, ["min"], output='pandas')['min'])

        overture_buildings['Height_diff'] = (
                overture_buildings['AlosDSM_max'] - overture_buildings['BaseDTM_min'])
        overture_buildings['Elevation_estimate'] = (
                overture_buildings['Height_diff'] + overture_buildings['BaseDTM_max'])

        overture_buildings['Elevation_estimate'] = round(overture_buildings['Elevation_estimate'], 0)

        # TODO Prototype for building-height refinement
        # prototype_refinement_building_Elevation_estimates(overture_buildings)

        overture_buildings_raster = _rasterize_polygons(overture_buildings, values=["Elevation_estimate"],
                                                        snap_to_raster=nasa_dem)
        if DEBUG:
            save_tiff_file(overture_buildings_raster, tile_data_path, 'debug_raw_building_footprints.tif')

        composite_bldg_dem = _combine_building_and_dem(nasa_dem, overture_buildings_raster, target_crs)

        # Save data to file
        save_tiff_file(composite_bldg_dem, tile_data_path, tiled_city_data.dsm_tif_filename)
        return True

    except Exception as e_msg:
        msg = f'Building DSM processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'Building DSM', tiled_city_data.source_base_path, msg, logger)
        return False

def _create_base_dtm(dsm, dem):
    min_raster = np.minimum(dsm, dem)
    return min_raster


def _combine_building_and_dem(dem, buildings, target_crs):
    coords_dict = {dim: dem.coords[dim].values for dim in dem.dims}

    # Convert to ndarray in order to mask and combine layers
    dem_nda = dem.to_numpy()
    bldg_nda = buildings.to_dataarray().to_numpy().reshape(dem_nda.shape)

    # Mask of building cells
    mask = (bldg_nda != -9999) & (~np.isnan(bldg_nda))

    # Mask buildings onto DEM
    dem_nda[mask] = bldg_nda[mask]

    #Convert results into DataArray and re-add coordinates and CRS
    composite_xa = xr.DataArray(dem_nda,
                      dims = ["y","x"],
                      coords = coords_dict
                      )
    composite_xa.rio.write_crs(target_crs, inplace=True)

    return composite_xa

def _resample_categorical_raster(xarray, resolution_m):
    resampled_array = xarray.rio.reproject(
        dst_crs=xarray.rio.crs,
        resolution=resolution_m,
        resampling=Resampling.nearest
    )
    return resampled_array


def _rasterize_polygons(gdf, values=["Value"], snap_to_raster=None):
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


def prototype_refinement_building_Elevation_estimates(overture_bldgs):
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
    overture_bldgs['guessed_height'] = overture_bldgs['inferred_height'] + overture_bldgs['BaseDTM_min']

    overture_bldgs.loc[overture_bldgs['height'].notnull(), 'best_guess_height'] = overture_bldgs['height']
    overture_bldgs.loc[((overture_bldgs['best_guess_height'].isnull()) & (
        overture_bldgs['guessed_height'].isnull())), 'best_guess_height'] = \
        overture_bldgs['Elevation_estimate']
    overture_bldgs.loc[(
                (overture_bldgs['best_guess_height'].isnull()) & (overture_bldgs['Elevation_estimate'].notnull()) & (
            overture_bldgs['guessed_height'].notnull())), 'best_guess_height'] = \
        0.6 * overture_bldgs['Elevation_estimate'] + 0.4 * overture_bldgs['guessed_height']
    overture_bldgs.loc[(
                (overture_bldgs['best_guess_height'].isnull()) & (overture_bldgs['Elevation_estimate'].isnull()) & (
            overture_bldgs['inferred_height'].notnull())), 'best_guess_height'] = \
        overture_bldgs['inferred_height']
    overture_bldgs.loc[overture_bldgs['best_guess_height'].isnull(), 'best_guess_height'] = (
                other_typical_floors * floor_height)

