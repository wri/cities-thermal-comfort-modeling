import rasterio
import xml.etree.ElementTree as ET

from pyproj import CRS
from osgeo import gdal


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
