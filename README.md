# cities-thermal-comfort-modeling (CTCM) processing framework

## How to use the CTCM framework for calling the UMEP QGIS Plugin and generating results
1. Pre-execution configuration.
   * In following steps, yse the "c:\CTCM_processing" folder as location for all your data. 
   * Copy the ZZZ_template_city folder (under sample_cities) into the CTCM_processing folder.
   * Rename the copied ZZZ_template_copy folder to the names of the country and city for your dataset, such as "MEX_Monterrey". (This is "your folder" for below instructions.)
   * Copy your tile-folders of TIFF files into the source_data\primary_soure_data filder with one sub-folder per tile. See the sample_cities\ZAF_Capetown_small_tile folder for an example setup.
   * Copy your meteorological files into the source_data\met_files folder.
1. Configuration for a city
   * In C:\CTCM_processing\<city_folder> folder:
     * modify the .config_umep_city_processing.csv file to specify which tiles and UMEP method to run
       * There are 4 possible method options, but the majority of the time choose "solweig_full". This option will first run the wall_height_aspect method, the skyview_factor method, and then the solweig method.
       * The other three options allow runs of just wall_height_aspect, skyview_factor, or solweig_only
   * In C:\CTCM_processing\<city_folder>\source_data folder:
     * modify the .config_city_processing.yml file to specify:
        * methods attributes (the file already contains default values which you may want to leave unaltered)
        * the names of source files stored in the primary_source_data\<tile> folders. Note that the system assumes that all source tiff files in all tiles have the same name. 
     * modify the .config_met_time_series.csv file to specify:
       * the name(s) for meteorological files to be processes by the solweig method
1. To practice running the system:
    * First, run the following script to validate the configuration in the C:\CTCM_processing\ZAF_Capetown_small_tiles practice folde:
      * Execute the _sample_run_CTM_processing_pre_check.bat batch script and confirm that it returned "Passed all validation checks"
    * Second, run the following script to process data in the C:\CTCM_processing\ZAF_Capetown_small_tiles practice folder.
      * Execute the _sample_run_CTM_processing.bat batch script and confirm that it returned "Processing encountered no errors." 
      * Note that processing will take approximately 1 minute.
2.To run the system for your data:
   * Copy and modify batch files:
     * Copy the _sample_run_CTM_processing_pre_check.bat file and name for your project area, such as _<my_city_name>_run_CTM_processing_pre_check.bat
     * Copy the _sample_run_CTM_processing.bat file and name for your project area, such as _<my_city_name>_run_CTM_processing.bat.
     * Edit both files to specify the city_folder name that you specified in "Configuration for a city" step above.
   * Execute the pre-check script
     *  Execute the _<my_city_name>_run_CTM_processing_pre_check.bat batch script and confirm that it returned "Passed all validation checks".
   * Execute the processing script
     * Execute the _<my_city_name>_run_CTM_processing.bat batch script and confirm that it returned "Processing encountered no errors."
     * Note that processing may take an extended period of time even up to 2+ hours depending on the size of your dataset.
1. Post-execution
   * Results of your run are written to the result_data folder under your target path specified in the batch script, such as C:\CTCM_processing\<my_city_name>\results_data
   * To see a report of success/failure, see html file in the .reports folder for the time that you started your run. 
   * For details about any failures, see the log file(s) in the .logs folder
   


## CTCM Deployment Instructions

Use `SOLWEIG-inputs.ipynb` to generate the input files for [SOLWEIG](https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/IntroductionToSolweig.html).

### Setup
1. Install Miniconda3 and add to system path, such as "C:\ProgramData\miniconda3\Scripts"
2. Install QGIS (v3.34.11) standalone app and add "UMEP for Processing" plugin.
3. Install PyCharm and create batch script with name "pycharm" pointing to PyCharm.bat such as "C:\Program Files\JetBrains\PyCharm Community Edition 2024.2.1\bin\pycharm.bat"
4. Determine paths to both QGIS and QGIS plugins and modify existing config.ini file as follows:
   * Open QGIS app, enter the following in the python console:
 ~~~
import sys
print(sys.path)
 ~~~
   * Parse through the results and determine paths to both QGIS app and QGIS plugins:
5. Copy the .config_sample.ini file and rename as .config.ini
   * Using the paths determined from QGIS python console above, populated the paths in the .config.ini file for qgis_home_path and qgis_plugin_path
6. Copy the environment_post_processing_sample.bat file and rename as environment_post_processing.bat
   * Substitute <qgis_plugin_path> with the path determined above.
7. Use Conda with the environment.yml file to configure the Conda environment.
   * 
~~~
'conda env create -f environment.yml`
~~~
   * Activate the cities-thermal conda environment
   * Execute the environment_post_processing.bat file
8. Create the C:\CTM_processing folder
   * Copy the ZAF_Capetown_small_tile folder from the codebase into C:\CTM_processing folder.
9. Confirm processing by running the test_processing_runs.py tests
   * Note that the tests will show exceptions even though the tests pass
10. Confirm processing using the C:\CTM_processing folder by executing the _sample_run_CTM_processing batch script.
   * After execution completes, confirm that:
     * The C:\CTCM_processing\ZAF_Capetown_small_tile folder contains the new C:\CTCM_processing\ZAF_Capetown_small_tile\results_data folder
     * The result_data folder contains two subfolder with each of these folders containing subfolders.

### Execution in Pycharm
1. Create input dataset based on the ZZZ_template_city folder, providing source data, meteorological timeseries files, and configuration settings
1. Configure runs of city data using the .config_umep_city_processing.csv file in the source folder located above the cities folders
1. In PyCharm, run main.py specifying source/target folders
1. In command prompt, run the _sample_run_main.bat batch file


