# Release notes for cities-thermal-comfort-modeling (CTCM) processing framework

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
    3. The solweig parameters now specifies different leaf start/end values for both northern_hemisphere and southern_hemisphere
    4. In CTCM_outcome folder, the names of raster files derived from CIF are renamed to the CIF name
 5. the _run_CTCM_.. batch files are updated. Please copy in the files from the C:\CTCM_data_setup\ZZZ_template_city folder.
