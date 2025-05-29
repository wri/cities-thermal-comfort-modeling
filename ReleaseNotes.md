# Release notes for cities-thermal-comfort-modeling (CTCM) processing framework

## 2025/05/29
1. Added generation and handling of open_urban primary file.
1. Changed error message to a warning for when the actual AOI differs by a large distance from stated AOI in the config yml file.
1. Updated test datasets for the new layer and to ensure all test files have matching extents.

## 2025/05/24
1. CDB-322. Fixed building height computation in CIF that lead to C-TCM failure to produce DSM layer. 
2. CDB-321. Expanded extents of C-TCM CIF layers to outermost dimensions of the AOI converted from geo to UTM coordinates.
3. CDB-320. Added option to config yml to control post-processing clipping of MRT results.

## 2025/05/12
1. Replaced NasaDEM with FabDEM for both DEM and building-ground DSM layers

## 2025/05/09
1. Now allows buffering of untiled AOI
2. For fully CIF setup, minimum buffer size is 100 with suggested 600 m.
3. For custom setup, buffer size and side length are ignored but return warning.

## 2025/05/01
1. Now outputs parameter settings that were passed to UMEP plugins into the .admin/model_metadata folder in the outcome location.
   1. One metadata file is written per tile.

## 2025/04/28
1. Replaced ground_build_dsm with data from OvertureHeightDSM CIF layer
2. Adjusted symbology in qgis viewer.

## 2025/03/31
1. Upgraded QGIS to 3.40.4
2. Upgraded for compatibility with CIF handling of cached layers.


## 2025/02/25
1. Buffered areas are now automatically clipped from the MRT results per https://gfw.atlassian.net/browse/CDB-182
2. Primary files and intermediate files are not impacted by this change.

## 2025/02/18
1. Moved the QGIS viewer to the top level of the city folder and renamed to "qgis_viewer.qgs".
2. Renamed the ".qgis_viewer" folder to ".qgis_data".
3. Eliminated the "results_data" folder and moved the "tcm_results_umep" folder to the top level.
4. Modified the VRT files so they are now portable to other locations.
5. Added the new "Sharing the City Folder" section to the README.md file including stating that the city folder must be compressed using 7-Zip in order to preserve all folders. 

## 2025/02/09
1. Broadly updated to utilize the new CIF get_data interface with GeoExtent instead of tuple[float] parameter for specifying bbox selection area
2. Change in behavior for custom primary files, as result of updated CIF bounding-box handling:
      1. Raster resolution must match in x and y directions in custom primary raster files, e.g. both have value of 1.
      2. Raster resolution must be an integer for custom primary raster files.

## 2025/01/16
1. Updated ERA5 retrieval and validation
2. QGIS viewer
   1. Improved symbology
   2. Restored rendering of intermediate layers

## 2025/01/08
1. For datasets with customer primary files, changed handling of discrepancies between the yml AOI and the tile-grid extent of the custom tiles as:
   1. For pre-check option:
      1. For any discrepancy, the code reports a warning with the lat/long values for the tile grid from the raster files.
   1. For processing option:
      1. For small discrepancies, the code reports a warning and automatically updates the target yml file with updated lat/long values. Small discrepancies are definded as being < 100m.
      1. For large discrepancies, the code reports an integrity failure and also lists the lat/long values for the tile grid in the error message. The user then has option to update the AOI in the yml file with these values.

## 2025/01/07
1. Methods renamed as:
   1. cif_download_only renamed to download_only
   1. solweig_full renamed to umep_solweig
1. Available methods now listed in the yml file
1. Now preserves yml comments when outputting to target folder

## 2024/12/30
1. Added option to provide intermediate data files.
2. Moved leaf start/end values into a section named seasonal_leaf_coverage.

## 2024/12/27
1. Processing folder renamed as C:\CTCM_data_setup. (Old folder renamed to C:\CTCM_processing_legacy)
2. Processing now outputs 'scenario' results to C:\CTCM_outcome folder.
3. Folder and file renaming:
   1. In CTCM_data_setup folder:
      1. Configuration yml file renamed to .config_method_parameters.yml
      2. Configuration csv file renamed to .config_city_processing.csv
      3. source_data folder renamed to primary_data
      4. primary_source_data folder renamed to raster_files
      5. output files are not written to CTCM_data_setup folder, but to CTCM_outcome folder
   2. In CTCM_outcome folder: 
      1. all log files, including run_reports moved to .logs folder
      2. the original source configuration files are written to the .source_config_files folder
      3. the raster_files and met files used by the scenario are written to CTCM_outcome folder
 4. .config_method_parameters.yml
    1. The order of yml sections are re-ordered to a more logical sequence
    2. Now includes a new section for scenario title, version, and author
    3. The solweig parameters now specifies different leaf start/end values for both hemisphere_north and hemisphere_south
    4. In CTCM_outcome folder, the names of raster files derived from CIF are renamed to the CIF name
 5. the _run_CTCM_.. batch files are updated. Please copy in the files from the C:\CTCM_data_setup\ZZZ_template_city folder.
