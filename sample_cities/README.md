Folder contains a sample dataset for Captown, South Africa and a template for other cities.

The .config_umep_city_processing.csv file controls execution of the city data through the processing system. 

## Columns:
1. ***enabled*** = indicates whether row will be run. values: {True, False}
1. ***city_folder_name*** = name of the city subfolder in the source folder
1. ***tile_folder_name*** = name of the source tile in the city subfolder
1. ***method*** = plugin method to run. values: {wall_height_aspect, skyview_factor, solweig_only, solweig_full}

## Methods explanation:
***solweig_full*** indicates that the solweig plugin will be called after first building the prior datasets.
***wall_height_aspect*** indicates that only the wall_height_aspect plugin will be called.
***skyview_factor*** indicates that only the skyview_fact plugin will be called.
***solweig_only*** indicates that only the solweig plugin will be called without building the prior datasets.

