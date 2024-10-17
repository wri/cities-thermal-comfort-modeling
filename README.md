# cities-thermal-comfort-modeling (CTCM) processing framework

## How to use the CTCM framework for calling the UMEP QGIS Plugin and generating results
1. Pre-execution configuration.
   * In following steps, yse the "c:\CTCM_processing" folder as location for all your data. 
   * Copy the ZZZ_template_city folder (under sample_cities) into the CTCM_processing folder.
   * Rename the copied ZZZ_template_copy folder to the names of the country and city for your dataset, such as "MEX_Monterrey". (This is "your folder" for below instructions.)
   * Copy your tile-folders of TIFF files into the source_data\primary_soure_data filder with one sub-folder per tile. See the sample_cities\ZAF_Capetown_small_tile folder for an example setup.
   * Copy your meteorological files into the source_data\met_files folder.
1. Configuration
   * In C:\CTCM_processing folder modify the .config_umep_city_processing.csv file to specify which city, tiles, and method should be run
   * For each city:
     * In C:\CTCM_processing\<city_folder>\source_data modify the .config_city_processing.yml file to specify:
        * methods attriubutes
        * the names of source files stored in the primary_source_data\<tile> folders
     * In C:\CTCM_processing\<city_folder>\source_data modify the .config_met_time_series.csv file to specify the name(s) for meteorological files to be processes by the solweig method
1. CTCM execution from the Windows command prompt:
   * Execute the _sample_run_main_pre_check.bat batch script with your path modifications to validate the configurations files. 
   * Execute the _sample_run_main.bat batch script with your path modifications to process the configured tasks.
1. Post-execution
   * Run reports are written to the .reports folder 
   * Log files are written to the .logs folder
   * Results are written the target path specified in the _sample_run_main.bat batch script


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
8Create the empty C:\CTM_processing folder

### Execution in Pycharm
1. Create input dataset based on the ZZZ_template_city folder, providing source data, meteorological timeseries files, and configuration settings
1. Configure runs of city data using the .config_umep_city_processing.csv file in the source folder located above the cities folders
1. In PyCharm, run main.py specifying source/target folders
1. In command prompt, run the _sample_run_main.bat batch file


