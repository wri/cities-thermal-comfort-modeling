import os.path
import tempfile

import xarray as xr
import shapely.wkt
import geopandas as gp
import numpy as np
import random
import time
import multiprocessing as mp

from city_metrix.constants import CTCM_PADDED_AOI_BUFFER
from osgeo import gdal
from city_metrix.metrix_model import GeoExtent, GeoZone
from rasterio.enums import Resampling
from datetime import datetime
from src.constants import VALID_PRIMARY_TYPES, S3_PUBLICATION_BUCKET, S3_PUBLICATION_ENV
from src.workers.city_data import CityData
from src.workers.logger_tools import setup_logger, log_method_start, log_method_completion, log_method_failure
from src.workers.worker_tools import compute_time_diff_mins, save_tiff_file, \
    unpack_quoted_value, ctcm_standardize_y_dimension_direction

DEBUG = False

# Unify the layers on the same resolution
DEFAULT_LULC_RESOLUTION = 1

MINIMUM_QUERY_WAIT_SEC = 5
MAXIMUM_QUERY_WAIT_SEC = 10

MIN_TILE_PAUSE_MINS = 2
MAX_TILE_PAUSE_MINS = 5
MAX_RETRY_COUNT = 3

# The wait time constants below were reduced from 10-30 seconds to 5-10 seconds.
# This change was made to improve responsiveness and reduce overall processing time,
# based on recent testing which showed no increase in contention or throttling issues
# with the external services being accessed. If contention or rate limiting is observed
# in the future, consider increasing these values or implementing exponential backoff.

def get_cif_data(city_json_str, source_base_path, target_base_path, folder_name_city_data, tile_id, cif_primary_features,
                 tile_boundary, tile_resolution, grid_crs, start_date, end_date, single_date):
    start_time = datetime.now()

    tiled_city_data = CityData(None, folder_name_city_data, tile_id, source_base_path, target_base_path)

    logger = setup_logger(tiled_city_data.target_model_log_path)
    log_method_start(f'CIF download: ({cif_primary_features}) in tile {tile_id}', None, '', logger)

    tile_cif_data_path = tiled_city_data.target_primary_tile_data_path

    geom = shapely.wkt.loads(tile_boundary) if isinstance(tile_boundary, str) else tile_boundary
    tile_boundary_df = {'geometry': [geom]}
    tiled_aoi_gdf = gp.GeoDataFrame(tile_boundary_df, crs=grid_crs)

    if city_json_str == 'None' or city_json_str is None:
        city_geoextent = GeoExtent(tiled_aoi_gdf.total_bounds, tiled_aoi_gdf.crs.srs)
        city_aoi = None
    else:
        city_geozone = GeoZone(geo_zone=city_json_str)
        city_geoextent = GeoExtent(city_geozone)
        city_aoi = tiled_aoi_gdf.total_bounds

    feature_list = cif_primary_features.split(',')

    tile_resolution = unpack_quoted_value(tile_resolution)
    output_resolution = DEFAULT_LULC_RESOLUTION if tile_resolution is None else tile_resolution

    result_flags = []
    # randomize retrieval to reduce contention against GEE
    random_feature_sequence= random.sample(range(1, len(VALID_PRIMARY_TYPES) + 1), len(VALID_PRIMARY_TYPES))
    for feature_sequence_id in random_feature_sequence:
        # wait random length of time to help reduce contention between threads and GEE throttling
        wait_time_sec = random.uniform(MINIMUM_QUERY_WAIT_SEC, MAXIMUM_QUERY_WAIT_SEC)
        if feature_sequence_id == 1:
            if 'dem' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-DEM download for {tile_id}', None, '', logger)
                this_success = _get_dem(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-DEM download for {tile_id}', None, '', logger)
        elif feature_sequence_id == 2:
            if 'dsm' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-DSM download for {tile_id}', None, '', logger)
                this_success = _get_building_height_dsm(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-DSM download for {tile_id}', None, '', logger)
        elif feature_sequence_id == 3:
            if 'lulc' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-lulc download for {tile_id}', None, '', logger)
                this_success = _get_lulc(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-lulc download for tile {tile_id}', None, '', logger)
        elif feature_sequence_id == 4:
            if 'open_urban' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-open_urban download for {tile_id}', None, '', logger)
                this_success = _get_open_urban(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time, f'CIF-open_urban download for tile {tile_id}', None, '', logger)
        elif feature_sequence_id == 5:
            if 'tree_canopy' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-Tree_canopy download for {tile_id}', None, '', logger)
                this_success = _get_tree_canopy_height(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
                result_flags.append(this_success)
                log_method_completion(start_time,f'CIF-Tree_canopy download for {tile_id}', None, '', logger)
        elif feature_sequence_id == 6:
            if 'air_temperature' in feature_list:
                time.sleep(wait_time_sec)
                log_method_start(f'CIF-air_temperature download for {tile_id}', None, '', logger)
                this_success = _get_air_temperature(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger, start_date, end_date, single_date)
                result_flags.append(this_success)
                log_method_completion(start_time,f'CIF-air_temperature download for {tile_id}', None, '', logger)

    # Execute Albedo last since it must be aligned with other layers
    if 'albedo_cloud_masked' in feature_list:
        log_method_start(f'CIF-albedo_cloud_masked download for {tile_id}', None, '', logger)
        this_success = _get_albedo_cloud_masked(city_geoextent, city_aoi, tiled_city_data, tile_cif_data_path, tiled_aoi_gdf, output_resolution, logger)
        result_flags.append(this_success)
        log_method_completion(start_time,f'CIF-albedo_cloud_masked download for {tile_id}', None, '', logger)


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


def _get_lulc(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers.open_urban import reclass_map
        from city_metrix.layers import OpenUrban

        # Load data
        for i in range(MAX_RETRY_COUNT):
            try:
                lulc = OpenUrban().retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                                 aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi)
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"LULC download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer LULC download failed due to error: {e_msg}")

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

        if lulc_to_solweig_class is None:
            return False

        try:
            # first attempt to save the file in the preferred NS direction
            was_reversed, standardized_lulc_to_solweig_class = ctcm_standardize_y_dimension_direction(
                lulc_to_solweig_class)
            save_tiff_file(standardized_lulc_to_solweig_class, tile_data_path, tiled_city_data.lulc_tif_filename)
        except:
            # otherwise save without flipping direction
            save_tiff_file(lulc_to_solweig_class, tile_data_path, tiled_city_data.lulc_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Lulc processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'lulc', tiled_city_data.source_base_path, msg, logger)
        return False


def _get_open_urban(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import OpenUrban

        for i in range(MAX_RETRY_COUNT):
            try:
                open_urban = OpenUrban().retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                                       aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi)
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"OpenUrban download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer OpenUrban download failed due to error: {e_msg}")

        if open_urban is None:
            return False

        try:
            # first attempt to save the file in the preferred NS direction
            was_reversed, standardized_open_urban = ctcm_standardize_y_dimension_direction(open_urban)
            save_tiff_file(standardized_open_urban, tile_data_path, tiled_city_data.open_urban_tif_filename)
            return True
        except:
            # otherwise save without flipping direction
            save_tiff_file(open_urban, tile_data_path, tiled_city_data.open_urban_tif_filename)
            return True

    except Exception as e_msg:
        msg = f'OpenUrban processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'OpenUrban', tiled_city_data.source_base_path, msg, logger)
        return False


def _count_occurrences(data, value):
    return data.where(data == value).count().item()


def _get_tree_canopy_height(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import TreeCanopyHeightCTCM

        # Load layer
        tree_canopy_height = None
        for i in range(MAX_RETRY_COUNT):
            try:
                tree_canopy_height = (TreeCanopyHeightCTCM()
                                      .retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                                     aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi,
                                                     spatial_resolution=output_resolution))
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"TreeCanopyHeight download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer TreeCanopyHeight download failed due to error: {e_msg}")

        tree_canopy_height_float32 = tree_canopy_height.astype('float32')

        if tree_canopy_height_float32 is None:
            return False

        try:
            # first attempt to save the file in the preferred NS direction
            was_reversed, standardized_tree_canopy_height_float32 = ctcm_standardize_y_dimension_direction(
                tree_canopy_height_float32)
            save_tiff_file(standardized_tree_canopy_height_float32, tile_data_path, tiled_city_data.tree_canopy_tif_filename)
        except:
            # otherwise save without flipping direction
            save_tiff_file(tree_canopy_height_float32, tile_data_path, tiled_city_data.tree_canopy_tif_filename)

        return True

    except Exception as e_msg:
        msg = f'Tree-canopy processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'tree_canopy_height', tiled_city_data.source_base_path, msg, logger)
        return False

def _get_albedo_cloud_masked(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import AlbedoCloudMasked

        albedo_cloud_masked = None
        for i in range(MAX_RETRY_COUNT):
            try:
                albedo_cloud_masked = (AlbedoCloudMasked()
                                       .retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                                      aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi,
                                                      spatial_resolution=output_resolution))
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"AlbedoCloudMasked download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer AlbedoCloudMasked download failed due to error: {e_msg}")

        if albedo_cloud_masked is None:
            return False

        with tempfile.TemporaryDirectory() as tmpdirname:
            temp_file = 'albedo_cloud_mask_temporary.tif'
            try:
                # first attempt to save the file in the preferred NS direction
                was_reversed, standardized_albedo_cloud_masked = ctcm_standardize_y_dimension_direction(albedo_cloud_masked)
                save_tiff_file(standardized_albedo_cloud_masked, tmpdirname, temp_file)
            except:
                # otherwise save without flipping direction
                save_tiff_file(albedo_cloud_masked, tmpdirname, temp_file)

            # Open the reference file to get its spatial reference and resolution
            reference_ds = gdal.Open(tiled_city_data.target_dem_path)
            reference_proj = reference_ds.GetProjection()
            reference_geotransform = reference_ds.GetGeoTransform()
            reference_width = reference_ds.RasterXSize
            reference_height = reference_ds.RasterYSize

            # Warp the input file to match the reference file
            output_tif = tiled_city_data.target_albedo_cloud_masked_path
            input_tif = os.path.join(tmpdirname, temp_file)
            gdal.Warp(
                output_tif,
                input_tif,
                format='GTiff',
                dstSRS=reference_proj,  # Set the spatial reference
                outputBounds=[
                    reference_geotransform[0],
                    reference_geotransform[3] + reference_height * reference_geotransform[5],
                    reference_geotransform[0] + reference_width * reference_geotransform[1],
                    reference_geotransform[3]
                ],
                width=reference_width,
                height=reference_height,
                resampleAlg="bilinear"
            )

        return True

    except Exception as e_msg:
        msg = f'Albedo-cloud-masked processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'Albedo_cloud_masked', tiled_city_data.source_base_path, msg, logger)
        return False

def _get_dem(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import FabDEM

        dem = None
        for i in range(MAX_RETRY_COUNT):
            try:
                dem = FabDEM().retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                             aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi,
                                             spatial_resolution=output_resolution)
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"FabDEM download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer FabDEM download failed due to error: {e_msg}")

        if dem is None:
            return False

        try:
            # first attempt to save the file in the preferred NS direction
            was_reversed, standardized_nasa_dem = ctcm_standardize_y_dimension_direction(dem)
            save_tiff_file(standardized_nasa_dem, tile_data_path, tiled_city_data.dem_tif_filename)
            return True
        except:
            # otherwise save without flipping direction
            save_tiff_file(dem, tile_data_path, tiled_city_data.dem_tif_filename)
            return True

    except Exception as e_msg:
        msg = f'DEM processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'DEM', tiled_city_data.source_base_path, msg, logger)
        return False

def _get_building_height_dsm(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger):
    try:
        from city_metrix.layers import OvertureBuildingsDSM

        building_dsm = None
        for i in range(MAX_RETRY_COUNT):
            try:
                building_dsm = (OvertureBuildingsDSM()
                                .retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                               aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi,
                                               spatial_resolution=output_resolution))
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"OvertureBuildingsDSM download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer OvertureBuildingsDSM download failed due to error: {e_msg}")

        if building_dsm is None:
            return False

        try:
            # first attempt to save the file in the preferred NS direction
            was_reversed, standardized_nasa_dem = ctcm_standardize_y_dimension_direction(building_dsm)
            save_tiff_file(standardized_nasa_dem, tile_data_path, tiled_city_data.dsm_tif_filename)
            return True
        except:
            # otherwise save without flipping direction
            save_tiff_file(building_dsm, tile_data_path, tiled_city_data.dsm_tif_filename)
            return True

    except Exception as e_msg:
        msg = f'Building DSM processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'DEM', tiled_city_data.source_base_path, msg, logger)
        return False

def _get_air_temperature(city_geoextent, city_aoi, tiled_city_data, tile_data_path, aoi_gdf, output_resolution, logger, start_date, end_date, single_date):
    try:
        from city_metrix.layers import BuAirTemperature

        seasonal_utc_offset = tiled_city_data.seasonal_utc_offset
        sampling_local_hours = tiled_city_data.sampling_local_hours

        air_temperature = None
        for i in range(MAX_RETRY_COUNT):
            try:
                air_temperature = (BuAirTemperature(start_date, end_date, single_date, seasonal_utc_offset, sampling_local_hours)
                                   .retrieve_data(city_geoextent, s3_bucket=S3_PUBLICATION_BUCKET, s3_env=S3_PUBLICATION_ENV,
                                             aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=city_aoi,
                                             spatial_resolution=output_resolution))
                break
            except Exception as e_msg:
                if i < MAX_RETRY_COUNT - 1:
                    print(f"BuAirTemperature download failed. Retrying...")
                    wait_secs = int(random.uniform(MIN_TILE_PAUSE_MINS * 60, MAX_TILE_PAUSE_MINS * 60))
                    time.sleep(wait_secs)
                else:
                    raise Exception(f"Layer BuAirTemperature download failed due to error: {e_msg}")

        if air_temperature is None:
            return False

        try:
            # first attempt to save the file in the preferred NS direction
            was_reversed, standardized_air_temperature = ctcm_standardize_y_dimension_direction(air_temperature)
            save_tiff_file(standardized_air_temperature, tile_data_path, tiled_city_data.air_temperature_tif_filename)
            return True
        except:
            # otherwise save without flipping direction
            save_tiff_file(air_temperature, tile_data_path, tiled_city_data.air_temperature_tif_filename)
            return True

    except Exception as e_msg:
        msg = f'BuAirTemperature processing cancelled due to failure {e_msg}.'
        log_method_failure(datetime.now(), 'BuAirTemperature', tiled_city_data.source_base_path, msg, logger)
        return False

def _resample_categorical_raster(xarray, resolution_m):
    resampled_array = xarray.rio.reproject(
        dst_crs=xarray.rio.crs,
        resolution=resolution_m,
        resampling=Resampling.nearest
    )
    return resampled_array




# def prototype_refinement_building_Elevation_estimates(overture_bldgs):
#     # TODO This function is an initial exploration of using OSM tags to refine estimate of building height.
#     # TODO Results are moderately successful for Amsterdam, but could be improved with additional building-height data.
#     '''
#     https://www.33rdsquare.com/how-tall-is-a-floor-meters/
#     Single Family Homes: 2.4 – 2.7 meters (8 – 9 feet)
#     Condominiums/Apartments: 2.1 – 2.4 meters (7 – 8 feet)
#     Office Spaces: 2.4 – 3 meters (8 – 10 feet)
#     Retail Stores: 3 – 4.5 meters (10 – 15 feet)
#     Industrial Spaces: 3 – 6 meters (10 – 20 feet)
#     class = transportation, sports_hall, service, school, retail, industrial, houseboat,house, commercial, apartments,
#     class/subtype = {null, entertainment}
#     '''
#
#     house_typical_floors = 4
#     apartment_typical_floors = 5
#     industrial_typical_floors = 3
#     commercial_typical_floors = 4
#     houseboat_typical_floors = 1
#     school_typical_floors = 3
#     other_typical_floors = 2
#     floor_height = 2.5
#     overture_bldgs.loc[overture_bldgs['num_floors'].notnull(), 'inferred_height'] = (
#                 overture_bldgs['num_floors'] * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'apartments'), 'inferred_height'] = \
#         (apartment_typical_floors * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'house'), 'inferred_height'] = \
#         (house_typical_floors * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'industrial'), 'inferred_height'] = \
#         (industrial_typical_floors * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'commercial'), 'inferred_height'] = \
#         (commercial_typical_floors * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'commercial'), 'inferred_height'] = \
#         (commercial_typical_floors * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'houseboat'), 'inferred_height'] = \
#         (houseboat_typical_floors * floor_height)
#     overture_bldgs.loc[
#         (overture_bldgs['inferred_height'].isnull()) & (overture_bldgs['class'] == 'school'), 'inferred_height'] = \
#         (school_typical_floors * floor_height)
#     overture_bldgs['guessed_height'] = overture_bldgs['inferred_height'] + overture_bldgs['BaseDTM_min']
#
#     overture_bldgs.loc[overture_bldgs['height'].notnull(), 'best_guess_height'] = overture_bldgs['height']
#     overture_bldgs.loc[((overture_bldgs['best_guess_height'].isnull()) & (
#         overture_bldgs['guessed_height'].isnull())), 'best_guess_height'] = \
#         overture_bldgs['Elevation_estimate']
#     overture_bldgs.loc[(
#                 (overture_bldgs['best_guess_height'].isnull()) & (overture_bldgs['Elevation_estimate'].notnull()) & (
#             overture_bldgs['guessed_height'].notnull())), 'best_guess_height'] = \
#         0.6 * overture_bldgs['Elevation_estimate'] + 0.4 * overture_bldgs['guessed_height']
#     overture_bldgs.loc[(
#                 (overture_bldgs['best_guess_height'].isnull()) & (overture_bldgs['Elevation_estimate'].isnull()) & (
#             overture_bldgs['inferred_height'].notnull())), 'best_guess_height'] = \
#         overture_bldgs['inferred_height']
#     overture_bldgs.loc[overture_bldgs['best_guess_height'].isnull(), 'best_guess_height'] = (
#                 other_typical_floors * floor_height)

