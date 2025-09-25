import json

from city_metrix.constants import CIF_ACTIVE_PROCESSING_FILE_NAME
from city_metrix.metrix_dao import get_bucket_name_from_s3_uri
from src.constants import S3_PUBLICATION_BUCKET
from src.worker_manager.tools import list_files_in_s3_folder
from src.workers.worker_dao import get_ctcm_data_folder_key


def check_ctcm_staging(non_tiled_city_data):
    # evaluate CTCM staging availability
    try:
        acm_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='AlbedoCloudMasked')
        dem_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='FabDEM')
        ou_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='OpenUrban')
        dsm_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='OvertureBuildingsDSM')
        tch_data_file_count = _evaluate_ctcm_staging(non_tiled_city_data, feature_name='TreeCanopyHeightCTCM')
    except:
        raise Exception("CTCM staging folders not available or partially available")

    if (acm_data_file_count == 0 or dem_data_file_count == 0 or ou_data_file_count == 0 or dsm_data_file_count == 0
            or tch_data_file_count == 0):
        e_msg = _build_exception_message("CTCM staging folders not available or partially available",
                                         acm_data_file_count, dem_data_file_count, ou_data_file_count,
                                         dsm_data_file_count,tch_data_file_count)
        raise Exception(e_msg)
    elif acm_data_file_count != dem_data_file_count != ou_data_file_count != dsm_data_file_count != tch_data_file_count:
        e_msg = _build_exception_message("Inconsistent tile counts in CTCM staging folders",
                                         acm_data_file_count, dem_data_file_count, ou_data_file_count,
                                         dsm_data_file_count,tch_data_file_count)
        raise Exception(e_msg)
    else:
        city = json.loads(non_tiled_city_data.city_json_str)
        city_id = city["city_id"]
        aoi_id = city["aoi_id"]
        print(f"\nCTCM staging folders appear to be complete for city: {city_id} and aoi: {aoi_id}.")
        return 0


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

    if len(notice_files) > 0:
        raise Exception(f'Active processing of staging files in {uri}')
    if len(txt_files) > 0:
        print(f"Warning: Unprocessed files in {uri}")

    return data_file_count

def _build_exception_message(header_msg, acm_data_file_count, dem_data_file_count, ou_data_file_count, dsm_data_file_count,tch_data_file_count):
    e_msg = (f"\n{header_msg}:"+
             f"\n   AlbedoCloudMasked: {acm_data_file_count}, " +
             f"\n   FabDEM: {dem_data_file_count}, " +
             f"\n   OpenUrban: {ou_data_file_count}, " +
             f"\n   OvertureBuildingsDSM: {dsm_data_file_count}, " +
             f"\n   TreeCanopyHeightCTCM: {tch_data_file_count}")

    return e_msg

