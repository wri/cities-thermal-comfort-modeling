Folder contains a sample dataset for Captown, South Africa and a template for other cities.

The umep_city_processing_registry.csv file controls execution of the city data through the processing system. 

## Columns:
1. ***run_id*** = serial number
1. ***enable*** = indicates whether row will be run. values: {True, False}
1. ***city_folder_name*** = name of the city subfolder in the source folder
1. ***tile_name*** = name of the source tile in the city subfolder
1. ***method*** = plugin method to run. values: {all, wall_height_aspect, skyview_factor, solweig}

