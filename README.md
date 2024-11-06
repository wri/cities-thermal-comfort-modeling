# cities-thermal-comfort-modeling (CTCM) processing framework

## Introduction
CTCM framework is used for:
1. retrieving data for a bounding box from the Cities-Indicators-Framework (CIF) system
2. calling the "UMEP for Processing" QGIS Plugin and generating SOLWEIG results

## User Instructions
Below steps are executed on one of the "Windows QGIS" EC2 instances maintained by WRI.
### Pre-execution configuration.
   1. Connect to one of the EC2 instance using RDP or some equivalent tool.
   1. Open Windows File Explorer and go to the C:\CTCM_processing folder
   1. Copy the ZZZ_template_city folder and rename to the country and city for your dataset, such as "USA_WashingtonDC". (This is **"your city folder"** in below instructions.)
   1. If you will be processing your own "custom" source TIFF files, then follow below steps, skip this step if you instead want the system to automatically retrieve the base TIFF files.
      1. Copy TIFF files for your city into subfolders under .\source_data\primary_source_data underneath your city folder.
         * **Note**: You can provide up to four source files for DSM, DEM, LULC, and TreeCanopy, but you can also have a partial set of these files.
         1. The subfolders must be named 'tile_001', 'tile_002', and so on.
            1. See the ZAF_Capetown_small_tile\source_data\primary_source_data folder for an example setup.
            1. **Tip**: You can use the "aws s3" command to download files from S3.
               * For example, to download a zip file for a sample tile, run "aws.exe s3 cp s3://wri-cities-heat/demo/kenn_transfer/tile_001.zip ." 
   1. If you are providing your own meteorological files, copy them into the .\source_data\met_files folder.
      * **Note**: Since you may currently not have meteorological data in the correct format for your city, copy over a file from the ZAF_Capetown_small_tile\source_data\met_files folder.

### Configuration for a city
   #### In your city folder:
   1. Modify the .config_umep_city_processing.csv file to specify which method to run and whether you want it run on all tiles or a range of tiles.
      1. There are 5 available methods, but the majority of the time choose "solweig_full". This option will first generate files needed for solweig before running the solweig plugin itself.
         * **Note**: The other four options allow runs of specific methods (cif_download_only, wall_height_aspect, skyview_factor, solweig_only) in case you want to separately run each method.
         * Methods:
           * cif_download_only - retrieves base TIF data from the CIF system without other processing.
           * wall_height_aspect, skyview_factor, solweig_only - runs each of these UMEP functions by itself.
           * solweig_full - this option runs cif-download (where needed), wall_height_aspect, skyview_factor, and finally solweig_only
   1. Modify the four sections of the .config_method_parameters.yml file to specify:
      1. methods attributes (the file already contains default values which you may want to leave unaltered)
      1. the name(s) for meteorological files to be processed by the solweig method
      1. the names of source files stored in the primary_source_data tile folders.
         1. If you want the system to automatically retrieve a tiff base file, instead specify None for the specific files.
           * **Note**: the system assumes that all source tiff files in all tiles have the same name.
         1. You can also specify a combination of None for some files and the name of the custom file if you want to provide some customer files and also have the system automatically retrieve others from CIF.
      1. the bounding coordinates of your area of interest and whether you want the AOI to be tiled into smaller sub-cells and buffered.
         * **Note**: These values are only used if you want to automatically retrieve base data from CIF.

### Running the system
   1. Open a Windows command prompt
      1. Enter and run the "gotcm" command to take you to the C:\CTCM_processing folder and activate the cities-thermal conda environment. 
      1. To run a pre-check of the configurations in your city folder:
         1. Execute the _run_CTCM_pre_check.bat script by simply entering the script name and hitting Enter.
         1. Confirm that it returns "Passed all validation checks"
         1. The _run_CTCM_pre_check.bat script validates your configuration files and source data files (if any).
      1. Finally, to run the method you specified in the .config_umep_city_processing.csv file: 
         1. Execute the _run_CTCM_processing.bat script by simply entering the script name and hitting Enter.
            * **Tip**: It his highly recommended that you hit the Enter key a couple of time after launch the run to force Windows to properly indicate when the job has completed.
            * **Note**: processing may take an extended period of time even up to 2+ hours per tile depending on the size of your dataset.
         1. Upon completion of the run, confirm that "Processing encountered no errors."
         1. Review the "results_data" folder looking for the run results as described below in Post-Execution section.

### Post-Execution Evaluation of Results
   1. Results of your run are written to the result_data folder under your city folder specified in the batch script.
   1. To see a report of success/failure, see the html files in the .results_data\.run_reports for the time that you started your run. 
   1. For details about any failures, see the log file(s) in the .results_data\.logs folder.
   


## Installation Instructions

### Setup
1. Install Miniconda3 and add to system path, such as "C:\ProgramData\miniconda3\Scripts"
2. Install QGIS (v3.34.11) standalone app and add "UMEP for Processing" plugin.
   * **Note**: The plugin is periodically updated and it's a good idea to stay current with the latest, so periodically check in QGIS plugins for updates.
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
6. Copy the environment_post_processing_sample.bat file and rename as environment_post_processing_local.bat
   * Substitute <qgis_plugin_path> with the path determined above.
7. Use Conda with the environment.yml file to configure the Conda environment.
~~~
'conda env create -f environment.yml`
~~~
   * Activate the cities-thermal conda environment
   * Execute the environment_post_processing.bat file
   * For later runs, you can simply execute the setup_conda.bat file
8. Add credentials for Google Earth Engine and ERA5
   * Install <https://cloud.google.com/sdk/docs/install>
   * If you want to use the ERA5 layer, you need to install the  [Climate Data Store (CDS) Application Program Interface (API)](https://cds.climate.copernicus.eu/how-to-api)
9. Create the C:\CTM_processing folder
   * Copy the ZAF_Capetown_small_tile and ZZZ_template_city folders from the codebase into C:\CTM_processing folder.
   * In these folders, modify the two "._run_CTCM_.." batch files to include the path to the main.py module if it does not already point to the correct local repository on the machine.
10. Create a batch file for navigating to the C:\CTM_processing folder and starting the conda environment.
   * Create the "gotcm.bat" file in some directory such as C:\Users\Administrator\Documents\Batches with following content:
     ~~~
     cd C:\CTCM_processing
     conda activate cities-thermal
     ~~~
   * Add location of the batch file to the system path
11. Confirm processing by running the test_processing_runs.py tests in the local repository code
    * **Note**: tests may show exceptions even though the tests pass
12. Confirm processing using the C:\CTM_processing folder
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


Use `SOLWEIG-inputs.ipynb` to generate the input files for [SOLWEIG](https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/IntroductionToSolweig.html).

