# This folder contains two sub-folders and two configuration files:

## Template:
See the ZZZ_template_city folder for a template of a city folder

## Folders:
1. **source_data\met_files** - contains time-series of meteorological data
1. **source_data\primary_source_data** - contains folders for each tile set of primary data TIF-files

## Configuration files:
1. **source_data\.config_umep_city_processing.csv**
   * List of tiles within the source_data\primary_source_data folder that will be processed
   * Also indicates whether processing is enabled and the method to use for processing.
2. **source_data\config_method_parameters.yml**
   * Specification of non-default input parameters for pre-processors and SOLWEIG methods.
   * List of meteorological time-series files in the met_files folder and UTC_offset in hours.
   * Mapping of data-type to name of TIF file in the source_data folder. Note: all file of a type must have the same name in all tiles.
