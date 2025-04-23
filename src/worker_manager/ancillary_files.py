import os
import shutil
import pandas as pd

from pathlib import Path
from src.constants import DATA_DIR, FILENAME_METHOD_YML_CONFIG, FILENAME_ERA5, METHOD_TRIGGER_ERA5_DOWNLOAD
from src.worker_manager.reporter import _find_files_with_name
from src.worker_manager.tools import delete_files_with_extension
from src.workers.worker_dao import write_raster_vrt_gdal, write_raster_vrt_wri
from src.workers.worker_tools import create_folder, write_commented_yaml, read_commented_yaml, remove_file


def write_qgis_files(city_data, crs_str):
    from src.worker_manager.reporter import find_files_with_substring_in_name

    target_city_path = city_data.target_city_path
    target_qgis_data_folder = city_data.target_qgis_data_path
    create_folder(target_qgis_data_folder)

    # Build VRTs for base layers
    target_folder = city_data.target_city_primary_data_path
    dem_file_name = city_data.dem_tif_filename
    dsm_file_name = city_data.dsm_tif_filename
    tree_canopy_file_name = city_data.tree_canopy_tif_filename
    lulc_file_name = city_data.lulc_tif_filename

    target_raster_files = []
    target_bucket = Path(target_folder).stem
    dem = _build_file_dict(target_bucket, 'base', 'dem', 0, [dem_file_name])
    target_raster_files += dem
    dsm = _build_file_dict(target_bucket, 'base','dsm', 0, [dsm_file_name])
    target_raster_files += dsm
    tree_canopy = _build_file_dict(target_bucket, 'base','tree_canopy', 0, [tree_canopy_file_name])
    target_raster_files += tree_canopy
    lulc = _build_file_dict(target_bucket, 'base','lulc', 0, [lulc_file_name])
    target_raster_files += lulc
    _write_raster_vrt_file_for_folder(target_folder, target_raster_files, target_qgis_data_folder)

    # Build VRTs for preprocessed results
    preprocessed_files = []
    target_preproc_folder = city_data.target_intermediate_data_path
    target_preproc_first_tile_folder = os.path.join(target_preproc_folder, 'tile_001')
    if os.path.exists(target_preproc_first_tile_folder):
        wall_aspect_file_stem = Path(city_data.wall_aspect_filename).stem
        wall_aspect_file_names = find_files_with_substring_in_name(target_preproc_first_tile_folder, wall_aspect_file_stem, '.tif')
        wall_aspect_files = _build_file_dict('preprocessed_data', 'preproc', 'wallaspect', 0, wall_aspect_file_names)
        _write_raster_vrt_file_for_folder(target_preproc_folder, wall_aspect_files, target_qgis_data_folder)
        preprocessed_files += wall_aspect_files

        wall_height_file_stem = Path(city_data.wall_height_filename).stem
        wall_height_file_names = find_files_with_substring_in_name(target_preproc_first_tile_folder, wall_height_file_stem, '.tif')
        wall_height_files = _build_file_dict('preprocessed_data', 'preproc', 'wallheight', 0, wall_height_file_names)
        _write_raster_vrt_file_for_folder(target_preproc_folder, wall_height_files, target_qgis_data_folder)
        preprocessed_files += wall_height_files


    # Build VRTs for tcm results
    met_filenames = []
    set_id = 0
    for met_file in city_data.met_filenames:
        if met_file.get('filename') == METHOD_TRIGGER_ERA5_DOWNLOAD:
            met_file_name = FILENAME_ERA5
        else:
            met_file_name = met_file.get('filename')

        met_folder_name = Path(met_file_name).stem
        target_tcm_folder = str(os.path.join(city_data.target_tcm_results_path, met_folder_name))
        target_tcm_first_tile_folder = os.path.join(target_tcm_folder, 'tile_001')

        if os.path.exists(target_tcm_first_tile_folder):
            shadow_file_names = find_files_with_substring_in_name(target_tcm_first_tile_folder, 'Shadow_', '.tif')
            shadow_files = _build_file_dict(met_folder_name, 'tcm', 'shadow', set_id, shadow_file_names)
            _write_raster_vrt_file_for_folder(target_tcm_folder, shadow_files, target_qgis_data_folder)
            met_filenames += shadow_files

            tmrt_file_names = find_files_with_substring_in_name(target_tcm_first_tile_folder, 'Tmrt_', '.tif')
            tmrt_files = _build_file_dict(met_folder_name, 'tcm', 'tmrt', set_id, tmrt_file_names)
            _write_raster_vrt_file_for_folder(target_tcm_folder, tmrt_files, target_qgis_data_folder)
            met_filenames += tmrt_files

            set_id += 1

    # write the QGIS viewer file
    vrt_files = target_raster_files + preprocessed_files + met_filenames
    _modify_and_write_qgis_file(vrt_files, city_data, crs_str, target_city_path)

    # remove tif.aux.xml files
    delete_files_with_extension(city_data.target_city_path, '.tif.aux.xml')

    _convert_vrts_to_relative_path(target_qgis_data_folder, target_city_path)

def _convert_vrts_to_relative_path(target_vrt_folder:str, target_city_path):
    target_vrt_directory = Path(target_vrt_folder)
    for file_path in target_vrt_directory.iterdir():
        if file_path.is_file() and Path(file_path).suffix == '.vrt':
            with open(file_path, "r") as file:
                data = file.read()
                data = data.replace('relativeToVRT="0"', 'relativeToVRT="1"')
                data = data.replace(target_city_path, '..')
            with open(file_path, 'w') as file:
                file.write(data)


def _modify_and_write_qgis_file(vrt_files, city_data, crs_str, target_city_path):
    source_qgs_file = os.path.join(DATA_DIR, 'support', 'template_viewer.qgs')

    with open(source_qgs_file, "r") as file:
        data = file.read()

        # Modify layers
        for vrt_file in vrt_files:
            set_id = vrt_file.get('set_id')
            ordinal = vrt_file.get('ordinal')
            type_name = vrt_file.get('type_name')
            group_name = vrt_file.get('group_name')
            target = vrt_file.get('target')
            # change layer name
            layer_search_text = f'template_{type_name}_{set_id}_{ordinal}'
            layer_replace_name = Path(vrt_file.get('vrt_name')).stem
            data = data.replace(layer_search_text, layer_replace_name)
            # change group name
            if group_name == 'tcm':
                group_search_text = f'template_{group_name}_{type_name}_{set_id}_group'
                group_replace_text = f'{group_name}_{target}_{type_name}_group'
                data = data.replace(group_search_text, group_replace_text)

        # Reset the three forms of crs specifications in the qgis file
        template_crs = '32734'
        target_crs = _get_substring_after_char(crs_str,':')
        data = data.replace(f'["EPSG",{template_crs}]', f'["EPSG",{target_crs}]')
        data = data.replace(f'<srid>{template_crs}</srid>', f'<srid>{target_crs}</srid>')
        data = data.replace(f'EPSG:{template_crs}', f'EPSG:{target_crs}')

        # Reset the default view extent
        source_line = _get_string_line_by_line(source_qgs_file, '<DefaultViewExtent')

        min_lon = city_data.min_lon
        min_lat = city_data.min_lat
        max_lon = city_data.max_lon
        max_lat = city_data.max_lat
        reproj_bbox = _get_reprojected_bbox(min_lon, min_lat, max_lon, max_lat, target_crs)
        target_minx, target_miny, target_maxx, target_maxy = reproj_bbox.squeeze().bounds
        target_line =f'    <DefaultViewExtent ymin="{target_miny}" xmin="{target_minx}" xmax="{target_maxx}" ymax="{target_maxy}">\n'

        data = data.replace(source_line, target_line)

    target_qgs_file = os.path.join(target_city_path, 'qgis_viewer.qgs')
    with open(target_qgs_file, 'w') as file:
        file.write(data)


def _get_reprojected_bbox(min_lon, min_lat, max_lon, max_lat, crs):
    import shapely.wkt
    import geopandas as gpd
    from shapely import Polygon

    coords = ((min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat), (max_lon, min_lat), (min_lon, min_lat))
    bbox_boundary = Polygon(coords).wkt
    d = {'geometry': [shapely.wkt.loads(bbox_boundary)]}
    aoi_gdf = gpd.GeoDataFrame(d, crs='EPSG:4326')
    aoi_gdf.to_crs(epsg=crs, inplace=True)

    return aoi_gdf


def _get_string_line_by_line(file_path, search_string):
    with open(file_path, 'r') as file:
        for line in file:
            if search_string in line:
                return line
        return None


def _get_substring_after_char(string, char):
    # Find the position of the character in the string
    pos = string.find(char)

    # If the character is found, return the substring after it
    if pos != -1:
        return string[pos + 1:]
    else:
        return ""


def _build_file_dict(target_folder_name, group_name, type_name, set_id, file_names):
    files = []
    file_names.sort()
    i = 0
    for file in file_names:
        file_stem = Path(file).stem
        vrt_file = f'{file_stem}.vrt'
        file_dict = {'group_name': group_name, 'type_name': type_name, 'set_id': set_id, 'ordinal': i,
                     'target': target_folder_name, 'filename': file, 'vrt_name': vrt_file}
        files.append(file_dict)
        i += 1
    return files


def _write_raster_vrt_file_for_folder(source_folder, files, target_viewer_folder):
    # get list of files in tiles
    for file in files:
        output_vrt_file = file.get('vrt_name')
        output_file_path = str(os.path.join(target_viewer_folder, output_vrt_file))

        filename = file.get('filename')
        source_raster_files = _find_files_with_name(source_folder, filename)

        # Below logic compensates for the fact that failures of gdal.BuildVRT cannot be caught.
        remove_file(output_file_path)
        write_raster_vrt_gdal(output_file_path, source_raster_files)
        if not os.path.exists(output_file_path):
            write_raster_vrt_wri(output_file_path, source_raster_files)
            if not os.path.exists(output_file_path):
                print(f"Failed to create VRT for {output_file_path}")



def write_config_files(non_tiled_city_data, updated_aoi):
    # write updated config files to target
    source_yml_config_path = non_tiled_city_data.city_method_config_path
    target_yml_config_path = os.path.join(non_tiled_city_data.target_city_path, FILENAME_METHOD_YML_CONFIG)

    updated_yml_config = _update_custom_yml_parameters(non_tiled_city_data, updated_aoi)
    write_commented_yaml(updated_yml_config, target_yml_config_path)

    # write source config files to cache subdirectory
    source_config_dir = str(os.path.join(non_tiled_city_data.target_log_path, 'last_run_cache'))
    create_folder(source_config_dir)
    target_original_yml_config_path = os.path.join(source_config_dir, FILENAME_METHOD_YML_CONFIG)
    shutil.copyfile(source_yml_config_path, target_original_yml_config_path)

    # TODO write primary checksums to source_config subdirectory


def _update_custom_yml_parameters(non_tiled_city_data, updated_aoi):
    city_configs = os.path.join(non_tiled_city_data.source_city_path, FILENAME_METHOD_YML_CONFIG)

    list_doc = read_commented_yaml(city_configs)

    if updated_aoi:
        updated_min_lon = updated_aoi.bounds[0]
        updated_min_lat = updated_aoi.bounds[1]
        updated_max_lon = updated_aoi.bounds[2]
        updated_max_lat = updated_aoi.bounds[3]

        processing_area = list_doc[1]
        processing_area['min_lon'] = updated_min_lon
        processing_area['min_lat'] = updated_min_lat
        processing_area['max_lon'] = updated_max_lon
        processing_area['max_lat'] = updated_max_lat

    met_filenames = list_doc[2].get('MetFiles')
    has_era_met_download = non_tiled_city_data.has_era_met_download
    # Replace era_download keyword with standard name for era data file
    if has_era_met_download:
        for item in met_filenames:
            if item["filename"] == METHOD_TRIGGER_ERA5_DOWNLOAD:
                item["filename"] = FILENAME_ERA5

    custom_primary_filenames = list_doc[3]
    custom_primary_filenames['dem_tif_filename'] = non_tiled_city_data.dem_tif_filename
    custom_primary_filenames['dsm_tif_filename'] = non_tiled_city_data.dsm_tif_filename
    custom_primary_filenames['lulc_tif_filename'] = non_tiled_city_data.lulc_tif_filename
    custom_primary_filenames['tree_canopy_tif_filename'] = non_tiled_city_data.tree_canopy_tif_filename

    custom_intermediate_filenames = list_doc[4]
    custom_intermediate_filenames['skyview_factor_filename'] = non_tiled_city_data.skyview_factor_filename
    custom_intermediate_filenames['wall_aspect_filename'] = non_tiled_city_data.wall_aspect_filename
    custom_intermediate_filenames['wall_height_filename'] = non_tiled_city_data.wall_height_filename

    return list_doc


def write_tile_grid(tile_grid, source_crs, target_qgis_data_path, filename):
    from shapely import wkt
    import geopandas as gpd

    if isinstance(tile_grid,dict):
        modified_tile_grid = pd.DataFrame(columns=['id', 'geometry'])
        for key, value in tile_grid.items():
            poly = wkt.loads(str(value[0]))
            modified_tile_grid.loc[len(modified_tile_grid)] = [key, poly]
    elif isinstance(tile_grid, gpd.GeoDataFrame):
        # TODO figure out how to retain the index
        if 'fishnet_geometry' in tile_grid.columns:
            modified_tile_grid = tile_grid.drop(columns='fishnet_geometry', axis=1)
        else:
            modified_tile_grid = tile_grid
    elif isinstance(tile_grid, pd.DataFrame):
        modified_tile_grid = pd.DataFrame(columns=['id', 'geometry'])
        for index, value in tile_grid.iterrows():
            tile_id = value['tile_name']
            geom = value['boundary']
            poly = wkt.loads(str(geom))
            modified_tile_grid.loc[len(modified_tile_grid)] = [tile_id, poly]
    else:
        raise Exception("inconvertible")

    # projected_gdf = gpd.GeoDataFrame(modified_tile_grid, crs=source_crs)

    target_file_name = f'{filename}.geojson'
    target_path = target_qgis_data_path
    create_folder(target_path)
    file_path = os.path.join(target_path, target_file_name)

    modified_tile_grid.to_file(file_path, driver='GeoJSON')


