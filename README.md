# cities-thermal-comfort-modeling (CTCM) processing framework

## How to use the CTCM framework for calling the UMEP QGIS Plugin and generating results
1. Pre-execution configuration.
   1. Open Windows File Explorer and go to the C:\CTCM_processing folder
   1. Copy the ZZZ_template_city folder and rename to the country and city for your dataset, such as "USA_WashingtonDC". (This is "your folder" for below instructions.)
   1. Copy the TIFF files for your city into the .\source_data\primary_source_data folder underneath the folder you created above.
      1. You can use the "aws s3" command to download files from S3.
         1. For example, to download a zip file for a sample tile, run "aws.exe s3 cp s3://wri-cities-heat/demo/kenn_transfer/tile_001.zip ." 
      1. Separate your TIFF files by tiled area into folders named 'tile_001', 'tile_002', and so on.
      1. See the ZAF_Capetown_small_tile\source_data\primary_source_data folder for an example setup.
   1. Copy your meteorological files into the .\source_data\met_files folder.
      1. Note: Since you may currently not have meteorological data in the correct format for your city, copy over a file from the ZAF_Capetown_small_tile\source_data\met_files folder.
1. Configuration for a city
   * In your C:\CTCM_processing\<city_folder> folder:
   1. modify the .config_umep_city_processing.csv file to specify which tiles and UMEP method to run
      1. There are 4 possible method options, but the majority of the time choose "solweig_full". This option will first generate files needed for solweig and then the solweig plugin.
      1. Note: The other three options allow runs of just wall_height_aspect, skyview_factor, or solweig_only in case you want to separately run each plugin.
   1. modify the .config_method_parameters.yml file to specify:
      1. methods attributes (the file already contains default values which you may want to leave unaltered)
      1. the name(s) for meteorological files to be processes by the solweig method
      1. the names of source files stored in the primary_source_data\<tile> folders. Note that the system assumes that all source tiff files in all tiles have the same name.
1. To practice running the system:
   1. Open a Windows command prompt
      1. Then run the gotcm command to take you to the C:\CTCM_processing folder and open the cities-thermal conda environment. 
   1. First, run the following script to validate the configuration in the C:\CTCM_processing\ZAF_Capetown_small_tiles practice folde:
      1. Execute the _sample_run_CTM_processing_pre_check.bat batch script and confirm that it returned "Passed all validation checks"
   1. Second, run the following script to process data in the C:\CTCM_processing\ZAF_Capetown_small_tiles practice folder.
      1. Execute the _sample_run_CTM_processing.bat batch script and confirm that it returned "Processing encountered no errors." 
      1. Note that processing will take approximately 1 minute.
1. To run the system for your data:
   1. Again, ensure you have opened a Windows command prompt and run the gotcm command.
   1. Copy and modify batch files:
      1. Copy the _sample_run_CTM_processing_pre_check.bat file and name for your project area, such as _<my_city_name>_run_CTM_processing_pre_check.bat
      1. Copy the _sample_run_CTM_processing.bat file and name for your project area, such as _<my_city_name>_run_CTM_processing.bat.
      1. Edit both files to specify the city_folder name that you specified in "Configuration for a city" step above.
   1. Execute the pre-check script
      1. Execute the _<my_city_name>_run_CTM_processing_pre_check.bat batch script and confirm that it returned "Passed all validation checks".
   1. Execute the processing script
      1. Execute the _<my_city_name>_run_CTM_processing.bat batch script and confirm that it returned "Processing encountered no errors."
      1. Note that processing may take an extended period of time even up to 2+ hours depending on the size of your dataset.
1. Post-execution
   1. Results of your run are written to the result_data folder under your target path specified in the batch script, such as C:\CTCM_processing\<my_city_name>\results_data
   1. To see a report of success/failure, see html file in the .results_data\.run_reports folder for the time that you started your run. 
   1. For details about any failures, see the log file(s) in the .results_data\.logs folder
   


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
~~~
'conda env create -f environment.yml`
~~~
   * Activate the cities-thermal conda environment
   * Execute the environment_post_processing.bat file
8. Create the C:\CTM_processing folder
   * Copy the ZAF_Capetown_small_tile and ZZZ_template_city folders from the codebase into C:\CTM_processing folder.
   * Copy the _sample_run_CTM_processing_pre_check.bat and _sample_run_CTM_processing.bat
     * Modify both batch files to include path to the main.py module.
9. Create a batch file for navigating to the C:\CTM_processing folder and starting the conda environment.
   * Create the "gotcm.bat" file in some directory such as C:\Users\Administrator\Documents\Batches with following content:
     ~~~
     cd C:\CTCM_processing
     conda activate cities-thermal
     ~~~
   * Add location of the batch file to the system path
10. Confirm processing by running the test_processing_runs.py tests in the local repository code
   * Note that the tests will show exceptions even though the tests pass
11. Confirm processing using the C:\CTM_processing folder
   * in windows command prompt, execute "gotcm" to go to the processing folder and start the conda environment.
   * Execute the _sample_run_CTM_processing_pre_check.bat batch script and ensure that no errors are report.
   * Execute the _sample_run_CTM_processing:
     * The C:\CTCM_processing\ZAF_Capetown_small_tile folder contains the new C:\CTCM_processing\ZAF_Capetown_small_tile\results_data folder
     * The result_data folder contains two subfolders with each of these folders containing subfolders.
     * The result_data folder also contains the .logs and .run_reports folders.

### Execution in Pycharm
1. Create input dataset based on the ZZZ_template_city folder, providing source data, meteorological timeseries files, and configuration settings
1. Configure runs of city data using the .config_umep_city_processing.csv file in the source folder located above the cities folders
1. In PyCharm, run main.py specifying source/target folders
1. In command prompt, run the _sample_run_main.bat batch file


