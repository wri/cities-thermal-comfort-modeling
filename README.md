# cities-thermal-comfort-modeling (CTCM) processing framework

## Introduction
CTCM framework is used for:
1. retrieving data for a bounding box from the Cities-Indicators-Framework (CIF) system
2. calling the "UMEP for Processing" QGIS Plugin and generating SOLWEIG results

## User Instructions
Below steps are executed on one of the "Windows QGIS" EC2 instances maintained by WRI.
### Pre-execution configuration.
   1. Connect to one of the EC2 instance using RDP or some equivalent tool.
   1. Open Windows File Explorer and go to the C:\CTCM_data_setup folder
   1. Copy the ZZZ_template_city folder and rename to the country and city for your dataset, such as "USA_WashingtonDC". (This is **"your city folder"** in below instructions.)
   1. If you will be processing your own "custom" source TIFF files, then follow below steps, skip this step if you instead want the system to automatically retrieve the base TIFF files.
      1. Copy TIFF files for your city into subfoqlders under .\primary_data\raster_files underneath your city folder.
         * **Note**: You can provide up to four source files for DSM, DEM, LULC, and TreeCanopy, but you can also have a partial set of these files.
         1. The subfolders must be named 'tile_001', 'tile_002', and so on.
            1. See the ZAF_Capetown_small_tile\primary_data\raster_files folder for an example setup.
            1. **Tip**: You can use the "aws s3" command to download files from S3.
               * For example, to download a zip file for a sample tile, run "aws.exe s3 cp s3://wri-cities-heat/demo/kenn_transfer/tile_001.zip ." 
   1. If you are providing your own meteorological files, copy them into the .\primary_data\met_files folder.
      * **Note**: Since you may currently not have meteorological data in the correct format for your city, copy over a file from the ZAF_Capetown_small_tile\primary_data\met_files folder.

### Configuration for a city
   #### In your city folder:
   1. Modify the five sections of the .config_method_parameters.yml file to specify:
      1. description of the scenario
         * The short tile is used to name the target folder
      1. Area-of-Interest parameters:
         1. the utc offset in hours
         1. the bounding coordinates of the area of interest
         1. tile-side-length in meters or None depending on whether you want the area sub-tiled
         1. tile-buffer length in meters or None depending on whether you want the constructed tiles to be buffered
           * Note: the buffered area will be automatically clipped from the resultant MRT files, but not from primary or intermediate files.
      1. the name(s) for meteorological file(s) to be processed by the solweig method.
         * Enter "<download_era5>" if you instead or also want the system to automatically download ERA5 data into a file named "met_era5_hottest_days.txt"
         *  **TIP**: On the next run, you can enter the file name met_era5_hottest_days.txt to avoid the download from ERA5 since the file is already on the system
      1. the names of source files stored in the primary_data\raster_files tile folders.
         1. If you want the system to automatically retrieve a tiff base file, instead specify None for the specific files.
           * **Note**: the system assumes that all source tiff files in all tiles have the same name.
         1. You can also specify a combination of None for some files and the name of the custom file if you want to provide some customer files and also have the system automatically retrieve others from CIF.
      1. methods attributes (the file already contains default values which you may want to leave unaltered)
         * **IMPORTANT** The default values for the leaf start and leaf end dates are configurable for both the northern and southern hemispheres. The code determines which hemispheric values to use based on the center latitude of the AOI. Equitorial AOIs are assign start/end dates as 0/365.


### Running the system
   1. Open a Windows command prompt
      1. Enter and run the "gotcm" command to take you to the C:\CTCM_data_setup folder and activate the cities-thermal conda environment. 
      1. To run a pre-check of the configurations in your city folder:
         1. Execute the a_run_CTCM_pre_check.bat script by simply entering the script name and hitting Enter.
         1. Confirm that it returns "Passed all validation checks"
         1. The a_run_CTCM_pre_check.bat script validates your configuration files and source data files (if any).
      1. Finally, to run the method you specified in the .config_umep_city_processing.csv file: 
         1. Execute the b_run_CTCM_processing.bat script by simply entering the script name and hitting Enter.
            * **Tip**: It his highly recommended that you hit the Enter key a couple of time after launch the run to force Windows to properly indicate when the job has completed.
            * **Note**: processing may take an extended period of time even up to 2+ hours per tile depending on the size of your dataset.
         1. Upon completion of the run, confirm that "Processing encountered no errors."
         1. Review the "results_data" folder looking for the run results as described below in Post-Execution section.

### Post-Execution Evaluation of Results
   1. Results of your run are written to the C:\CTCM_outcome folder under your city folder specified in the batch script with a sub-folder for the scenario.
   1. To see a report of success/failure, see the html files in the .logs folder for the time that you started your run. 
   1. For details about any failures, see the log file(s) in the .logs folder.
   1. The system also generates a QGIS workfile named 'qgis_viewer.qgs' and is an excellent means for verifying and examining the outcome.
      * **Note** The current version of the workfile requires two steps to view your data:
        1. After opening the file in QGIS, it will likely give the warning that several files are unavailable. Click on the "Remove Unavailable Layers" and OK to continue.
        2. Next, right click on one of the layers and select "Zoom to Layer(s)" option. Your data should now be viewable.
      * **Notes on current limitations**
        1. The viewer only supports results for up to two meteorological files in the tcm_tmrt and tcm_shadow groups.
        2. Each meteorological group only supports up to five hourly layers
   
### Sharing the City Folder
   1. Zip the city folder using the 7-Zip compression tool. (Other compression tools may not preserver all folders and files.)
   2. Copy the zipped file to S3 or other locations.
   3. Note: Do not uncompress the city folder into a directory that is deeply nested in the directory tree, since the VRT files may not properly function.


## Installation Instructions

### Setup
1. Install the 7-ZIP compression tool
2. Create environment variable named PYTHONPATH and set to the location of the source code.
3. Download and install Miniconda3 for all users
4. Add install location to system path under environment variables, such as "C:\ProgramData\miniconda3\Scripts"
5. Install QGIS (v3.34.11) standalone app and add "UMEP for Processing" plugin.
   * **Note**: The plugin is periodically updated and it's a good idea to stay current with the latest, so periodically check in QGIS plugins for updates.
6. Install PyCharm and create batch script with name "pycharm" pointing to PyCharm.bat such as "C:\Program Files\JetBrains\PyCharm Community Edition 2024.2.1\bin\pycharm.bat"
7. Determine paths to both QGIS and QGIS plugins and modify existing config.ini file as follows:
   * Open QGIS app, enter the following in the python console:
 ~~~
import sys
print(sys.path)
 ~~~
   * Parse through the results and determine paths to both QGIS app and QGIS plugins:
8. Copy the .config_sample.ini file and rename as .config.ini
   * Using the paths determined from QGIS python console above, populated the paths in the .config.ini file for qgis_home_path and qgis_plugin_path
9. Copy the environment_post_processing_sample.bat file and rename as environment_post_processing_local.bat
   * Substitute <qgis_plugin_path> with the path determined above.
10. Use Conda with the environment.yml file to configure the Conda environment.
~~~
'conda env create -f environment.yml`
~~~
   * Activate the cities-thermal conda environment
   * Execute the environment_post_processing.bat file
   * For later runs, you can simply execute the setup_conda.bat file
11. Add credentials for Google Earth Engine and ERA5
   * Install <https://cloud.google.com/sdk/docs/install>
   * If you want to use the ERA5 layer, you need to install the  [Climate Data Store (CDS) Application Program Interface (API)](https://cds.climate.copernicus.eu/how-to-api)
12. Create the C:\CTCM_data_setup folder
   * Copy the ZAF_Capetown_small_tile and ZZZ_template_city folders from the codebase into C:\CTCM_data_setup folder.
   * In these folders, modify the two "._run_CTCM_.." batch files to include the path to the main.py module if it does not already point to the correct local repository on the machine.
13. Create a batch file for navigating to the C:\CTCM_data_setup folder and starting the conda environment.
   * Create the "gotcm.bat" file in some directory such as C:\Users\Administrator\Documents\Batches with following content:
     ~~~
     cd C:\CTCM_data_setup
     conda activate cities-thermal
     ~~~
   * Add location of the batch file to the system path
14. Confirm processing by running the test_processing_runs.py tests in the local repository code
    * **Note**: tests may show exceptions even though the tests pass
15. Confirm processing using the C:\CTCM_data_setup folder
   * in windows command prompt, execute "gotcm" to go to the processing folder and start the conda environment.
   * Execute the _sample_run_CTM_processing_pre_check.bat batch script and ensure that no errors are report.
   * Execute the _sample_run_CTM_processing and then confirm:
     * The C:\CTCM_outcome\ZAF_Capetown_small_tile folder contains results.
     * The ..\result_data folder contains two subfolders with each of these folders containing subfolders.

### CIF setup
1. Download and install gcloud https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe but don't sign in
2. Add Environmental per https://github.com/wri/cities-cif?tab=readme-ov-file#credentials
2. ERA5 data api creds to home directory per https://cds.climate.copernicus.eu/how-to-api

### Execution in Pycharm
1. Create input dataset based on the ZZZ_template_city folder, providing source data, meteorological timeseries files, and configuration settings
1. Configure runs of city data using the .config_umep_city_processing.csv file in the source folder located above the cities folders
1. In PyCharm, run main.py specifying source/target folders
1. In command prompt, run the _sample_run_main.bat batch file


Use `SOLWEIG-inputs.ipynb` to generate the input files for [SOLWEIG](https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/IntroductionToSolweig.html).

