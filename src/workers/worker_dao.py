import json
import os
import rasterio
import xml.etree.ElementTree as ET

from city_metrix import s3_client
from city_metrix.metrix_dao import create_uri_target_folder, get_bucket_name_from_s3_uri
from pyproj import CRS
from osgeo import gdal

from src.constants import S3_PUBLICATION_BUCKET, FOLDER_NAME_PRIMARY_RASTER_FILES, FOLDER_NAME_INTERMEDIATE_DATA, \
    FOLDER_NAME_UMEP_TCM_RESULTS, FOLDER_NAME_ADMIN_DATA, FOLDER_NAME_QGIS_DATA, FILENAME_TILE_GRID, \
    FILENAME_UNBUFFERED_TILE_GRID, FILENAME_METHOD_YML_CONFIG
from src.workers.city_data import CityData
from src.workers.worker_tools import remove_folder, _does_s3_folder_exist


def write_raster_vrt_gdal(output_file_path:str, raster_files):
    # Create the VRT
    vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', addAlpha=False)
    vrt_result = gdal.BuildVRT(output_file_path, raster_files, options=vrt_options)
    # Note: It's not possible to capture exceptions from executing the BuildVRT command


def write_raster_vrt_wri(output_xml_path, input_paths):
    overall_x_size, overall_y_size = _compute_vrt_size(input_paths) # works for capetown

    vrt_root = ET.Element("VRTDataset", attrib={
        "rasterXSize": str(overall_x_size),
        "rasterYSize": str(overall_y_size)
    })

    datasets = [rasterio.open(path) for path in input_paths]
    first_ds = datasets[0]

    crs_list = [ds.crs.to_epsg() for ds in datasets]
    epsg_code = CRS.from_wkt(first_ds.crs.wkt).to_epsg()

    datatype = first_ds.dtypes[0]
    first_datatype = 'Byte' if datatype.lower() == 'uint8' else datatype.title()

    srs = ET.SubElement(vrt_root, 'SRS', dataAxisToSRSAxisMapping="1,2")
    srs.text = f"EPSG:{epsg_code}"

    # Ensure all GeoTIFFs share the same EPSG:32631 projection
    if len(set(crs_list)) > 1 or crs_list[0] != epsg_code:
        raise ValueError(f"All input GeoTIFFs must have EPSG:{epsg_code} projection.")

    # Determine global raster bounds
    min_x = min(ds.bounds.left for ds in datasets)
    max_y = max(ds.bounds.top for ds in datasets)

    # Compute GeoTransform for entire VRTDataset
    global_transform = first_ds.transform
    global_geo_transform = f"{min_x}, {global_transform.a}, {global_transform.b}, {max_y}, {global_transform.d}, {global_transform.e}"

    # Add global GeoTransform to VRTDataset
    ET.SubElement(vrt_root, "GeoTransform").text = global_geo_transform

    # Create a **single** VRTRasterBand for all tiles
    raster_band = ET.SubElement(vrt_root, "VRTRasterBand", attrib={
        "dataType": first_datatype,  # Use first tile's datatype for consistency
        "band": "1"
    })

    # Read NoData value
    nodata_value = None if first_ds.nodata is None else str(int(first_ds.nodata))
    ET.SubElement(raster_band, "NODATA").text = nodata_value

    # Read ColorInterp
    color_interp = first_ds.colorinterp[0].name.title() if first_ds.colorinterp else "Gray"
    ET.SubElement(raster_band, "ColorInterp").text = color_interp

    # Process each GeoTIFF file
    for ds in datasets:
        # Compute correct offsets using resolution & top-left origin
        x_offset = int((ds.bounds.left - min_x) / global_transform.a)
        y_offset = int((max_y - ds.bounds.top) / abs(global_transform.e))

        # Read Pixel Size
        x_pixel_size = global_transform.a
        y_pixel_size = abs(global_transform.e)  # Ensure positive pixel size

        # Define tile source inside the **single VRTRasterBand**
        src_filename = ET.SubElement(raster_band, "SimpleSource", resampling="nearest")
        ET.SubElement(src_filename, "SourceFilename", relativeToVRT="1").text = ds.name  # Full file path
        ET.SubElement(src_filename, "SourceBand").text = "1"
        ET.SubElement(src_filename, 'SourceProperties', RasterXSize=str(ds.width), RasterYSize=str(ds.height),
                                  DataType=first_datatype, BlockXSize=str(x_pixel_size), BlockYSize=str(y_pixel_size))

        ET.SubElement(src_filename, "SrcRect", attrib={
            "xOff": "0", "yOff": "0",
            "xSize": str(ds.width), "ySize": str(ds.height)
        })
        ET.SubElement(src_filename, "DstRect", attrib={
            "xOff": str(x_offset),
            "yOff": str(y_offset),
            "xSize": str(ds.width),
            "ySize": str(ds.height)
        })
        if nodata_value is not None:
            ET.SubElement(src_filename, "NODATA").text = nodata_value

    # Save XML VRT file with proper indentation
    tree = ET.ElementTree(vrt_root)
    ET.indent(tree, space="  ", level=0)
    tree.write(output_xml_path, encoding="utf-8", xml_declaration=False)


def _compute_vrt_size(geotiff_files):
    total_raster_x_size = 0
    total_raster_y_size = 0
    resolutions = []

    for file in geotiff_files:
        with rasterio.open(file) as dataset:
            width = dataset.width
            height = dataset.height
            resolution_x, resolution_y = dataset.res  # Assuming resolution is in meters/pixel or similar
            resolutions.append((resolution_x, resolution_y))
            total_raster_x_size += width
            total_raster_y_size += height

    # Check if all resolutions are the same
    if len(set(resolutions)) > 1:
        print("Warning: GeoTIFF files have different resolutions.")
        # Adjust calculations if needed
        base_resolution_x, base_resolution_y = resolutions[0]
        total_raster_x_size = sum(width * (res_x / base_resolution_x) for width, (res_x, res_y) in zip([rasterio.open(file).width for file in geotiff_files], resolutions))
        total_raster_y_size = sum(height * (res_y / base_resolution_y) for height, (res_x, res_y) in zip([rasterio.open(file).height for file in geotiff_files], resolutions))

    return total_raster_x_size, total_raster_y_size


def cache_tile_files(tiled_city_data:CityData, publishing_target:str):
    city = json.loads(tiled_city_data.city_json_str)
    city_id = city["city_id"]
    aoi_id = city["aoi_id"]

    infra_id = tiled_city_data.infra_id
    scenario_id = tiled_city_data.scenario_title
    tile_id = tiled_city_data.folder_name_tile_data

    scenario_folder_key = f"city_projects/{city_id}/{aoi_id}/scenarios/{infra_id}/{scenario_id}"
    tile_folder_key = f"{scenario_folder_key}/{tile_id}"
    s3_folder_uri = f"{S3_PUBLICATION_BUCKET}/{tile_folder_key}"
    create_uri_target_folder(s3_folder_uri)

    bucket_name = get_bucket_name_from_s3_uri(S3_PUBLICATION_BUCKET)

    # Cache primary raster
    local_folder = tiled_city_data.target_raster_files_path
    s3_folder_uri = f"{tile_folder_key}/{FOLDER_NAME_PRIMARY_RASTER_FILES}"
    _process_tile_folder(local_folder, tile_id, bucket_name, s3_folder_uri, publishing_target, '.tif')

    # Cache intermediate files
    local_folder = tiled_city_data.target_intermediate_data_path
    s3_folder_uri = f"{tile_folder_key}/{FOLDER_NAME_INTERMEDIATE_DATA}"
    _process_tile_folder(local_folder, tile_id, bucket_name, s3_folder_uri, publishing_target, '.tif')

    # Cache tcm results
    tcm_path = tiled_city_data.target_tcm_results_path
    met_folders = [f for f in os.listdir(tcm_path) if os.path.isdir(os.path.join(tcm_path, f))]
    for met_folder in met_folders:
        local_folder = os.path.join(tiled_city_data.target_tcm_results_path, met_folder)
        s3_folder_uri = f"{tile_folder_key}/{FOLDER_NAME_UMEP_TCM_RESULTS}/{met_folder}"
        _process_tile_folder(local_folder, tile_id, bucket_name, s3_folder_uri, publishing_target, extension_filter='.tif')

    s3_metadata_folder_uri = f"{scenario_folder_key}/metadata/"
    has_metadata_folder = _does_s3_folder_exist(bucket_name, s3_metadata_folder_uri)

    if not has_metadata_folder:
        # attempt to populate the metadata folder one time, but this may happen with coincident uploads

        # Create the folder by uploading an empty object
        s3_client.put_object(Bucket=bucket_name, Key=s3_metadata_folder_uri)

        # Cache admin
        local_folder = tiled_city_data.target_log_path
        s3_folder_uri = f"{s3_metadata_folder_uri}/{FOLDER_NAME_ADMIN_DATA}".replace('//','/')
        _process_tile_folder(local_folder, None, bucket_name, s3_folder_uri, publishing_target)

        # Cache qgis data for tile grids
        local_folder = tiled_city_data.target_qgis_data_path
        s3_folder_uri = f"{s3_metadata_folder_uri}/{FOLDER_NAME_QGIS_DATA}".replace('//','/')
        file_list = [FILENAME_TILE_GRID, FILENAME_UNBUFFERED_TILE_GRID]
        _process_tile_folder(local_folder, None, bucket_name, s3_folder_uri, publishing_target, file_list=file_list)

        # Cache yml file
        local_folder = tiled_city_data.target_city_path
        file_list = [FILENAME_METHOD_YML_CONFIG]
        _process_tile_folder(local_folder, None, bucket_name, s3_metadata_folder_uri, publishing_target, file_list=file_list)


def _process_tile_folder(local_folder_root, tile_id, bucket_name, raster_folder_uri, publishing_target,
                         extension_filter=None, file_list=None):
    local_tile_folder = local_folder_root if tile_id is None else str(os.path.join(local_folder_root, tile_id))
    _upload_tiff_files_in_folder_to_s3(local_tile_folder, bucket_name, raster_folder_uri, extension_filter, file_list)
    if publishing_target == 's3':
        remove_folder(local_tile_folder)
        notice_file = os.path.join(local_folder_root, f"{tile_id}_contents_cached_to_s3.txt")
        with open(notice_file, "w") as file:
            pass  # Creates an empty file


def _upload_tiff_files_in_folder_to_s3(local_folder:str, bucket_name:str, s3_folder:str, extension_filter:str=None,
                                       file_list=None):
    for root, dirs, files in os.walk(local_folder):
        for file_name in files:
            if extension_filter is not None and not file_name.endswith(extension_filter):
                continue

            if file_list is not None and file_name not in file_list:
                continue

            local_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(local_path, local_folder)
            s3_path = os.path.join(s3_folder, relative_path).replace("\\", "/")  # Ensure S3 uses forward slashes
            s3_client.upload_file(local_path, bucket_name, s3_path)
