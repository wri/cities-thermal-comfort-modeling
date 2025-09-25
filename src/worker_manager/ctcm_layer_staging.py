import json

from cif_portal import get_data
from city_metrix.constants import CIF_ACTIVE_PROCESSING_FILE_NAME, CTCM_PADDED_AOI_BUFFER, CTCM_CACHE_S3_BUCKET_URI
from city_metrix.metrix_dao import get_bucket_name_from_s3_uri

from src.constants import S3_PUBLICATION_BUCKET
from src.worker_manager.tools import list_files_in_s3_folder
from src.workers.worker_dao import get_ctcm_data_folder_key
from src.workers.city_data import CityData


def check_ctcm_staging(non_tiled_city_data: CityData, fail_on_error:bool=True):
    # evaluate CTCM staging availability
    try:
        acm_return_code, acm_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='AlbedoCloudMasked')
        dem_return_code, dem_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='FabDEM')
        ou_return_code, ou_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='OpenUrban')
        dsm_return_code, dsm_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='OvertureBuildingsDSM')
        tch_return_code, tch_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='TreeCanopyHeightCTCM')
    except Exception as ex_msg:
        raise Exception(f"Error call in CTCM layer staging: {ex_msg}")

    if (acm_data_file_count == 0 and dem_data_file_count == 0 and ou_data_file_count == 0 and dsm_data_file_count == 0
            and tch_data_file_count == 0):
        e_msg = _build_exception_message("CTCM staging folders are unavailable",
                                         acm_data_file_count, dem_data_file_count, ou_data_file_count,
                                         dsm_data_file_count,tch_data_file_count)
        if fail_on_error:
            raise Exception(e_msg)
        else:
            return -1
    elif (acm_data_file_count == 0 or dem_data_file_count == 0 or ou_data_file_count == 0 or dsm_data_file_count == 0
            or tch_data_file_count == 0):
        e_msg = _build_exception_message("CTCM staging folders partially available",
                                         acm_data_file_count, dem_data_file_count, ou_data_file_count,
                                         dsm_data_file_count,tch_data_file_count)
        if fail_on_error:
            raise Exception(e_msg)
        else:
            return -2
    elif (acm_return_code != 0 or dem_return_code != 0 or ou_return_code != 0 or dsm_return_code != 0
            or tch_return_code!= 0):
        e_msg = _build_exception_message("CTCM staging folders not available or partially available",
                                         acm_data_file_count, dem_data_file_count, ou_data_file_count,
                                         dsm_data_file_count, tch_data_file_count)
        if fail_on_error:
            raise Exception(e_msg)
        else:
            return -3
    elif (acm_data_file_count != dem_data_file_count or acm_data_file_count != ou_data_file_count
          or acm_data_file_count != dsm_data_file_count or acm_data_file_count != tch_data_file_count):
        e_msg = _build_exception_message("Inconsistent tile counts in CTCM staging folders",
                                         acm_data_file_count, dem_data_file_count, ou_data_file_count,
                                         dsm_data_file_count,tch_data_file_count)
        if fail_on_error:
            raise Exception(e_msg)
        else:
            return -4
    else:
        city = json.loads(non_tiled_city_data.city_json_str)
        city_id = city["city_id"]
        aoi_id = city["aoi_id"]
        print(f"\nCTCM staging folders appear to be complete for city: {city_id} and aoi: {aoi_id}.")
        return 0

def publish_ctcm_staging_files(non_tiled_city_data: CityData):
    return_code = check_ctcm_staging(non_tiled_city_data, fail_on_error=False)

    if return_code == 0:
        print("CTCM staging folders are already available so stopping.")
    elif return_code == -1:
        print("\n============= Starting CTCM caching ================\n")
        cities_json = f"[{non_tiled_city_data.city_json_str}]"
        layer_params_json = (
            "["
            '{"name": "FabDEM", "params": "", "getdata_params": "spatial_resolution=1"},'
            '{"name": "OpenUrban", "params": ""},'
            '{"name": "TreeCanopyHeightCTCM", "params": "", "getdata_params": "spatial_resolution=1"},'
            '{"name": "AlbedoCloudMasked", "params": "", "getdata_params": "spatial_resolution=1"},'
            '{"name": "OvertureBuildingsDSM", "params": "", "getdata_params": "spatial_resolution=1"},'
            "]"
        )
        bucket_name = S3_PUBLICATION_BUCKET
        aoi_buffer_m = CTCM_PADDED_AOI_BUFFER
        get_data(cities_json, layer_params_json, False, bucket_name, False, aoi_buffer_m=aoi_buffer_m)

        return_code = check_ctcm_staging(non_tiled_city_data, fail_on_error=False)
        if return_code != 0:
            raise Exception("Publishing of CTCM staging folders may have failed Please investigate.")
        else:
            print("CTCM staging finished.")
    else:
        raise Exception("Existing CTCM staging folders may be corrupted. Please investigate.")


def _evaluate_ctcm_staging(non_tiled_city_data, feature_name):
    folder_key = get_ctcm_data_folder_key(non_tiled_city_data, feature_name=feature_name)

    bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)
    folder_files = list_files_in_s3_folder(bucket_name, folder_key)
    notice_files = [file for file in folder_files if CIF_ACTIVE_PROCESSING_FILE_NAME in file]
    tif_files = [file for file in folder_files if file.endswith(".tif")]
    txt_files = [file for file in folder_files if file.endswith(".txt")]
    data_file_count = len(tif_files) + len(txt_files)
    del folder_files

    uri = f"s3://{bucket_name}/{folder_key}"

    return_code = 0
    if len(notice_files) > 0:
        print(f'Active processing of staging files in {uri}')
        return -1, 0
    if len(txt_files) > 0:
        print(f"Warning: Unprocessed files found in {uri}")

    return return_code, data_file_count


def _build_exception_message(header_msg, acm_data_file_count, dem_data_file_count, ou_data_file_count,
                             dsm_data_file_count, tch_data_file_count):
    e_msg = (f"\n{header_msg}:"+
             "\nFile counts in layer folder," +
             f"\n   AlbedoCloudMasked: {acm_data_file_count}, " +
             f"\n   FabDEM: {dem_data_file_count}, " +
             f"\n   OpenUrban: {ou_data_file_count}, " +
             f"\n   OvertureBuildingsDSM: {dsm_data_file_count}, " +
             f"\n   TreeCanopyHeightCTCM: {tch_data_file_count}")

    return e_msg

