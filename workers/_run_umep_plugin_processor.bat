REM @echo off
set task_id=-99
set method=%1
set city_folder_name=%2
set tile_folder_name=%3
set source_data_path=%4
set target_path=%5

python .\umep_plugin_processor.py^
 --task_id=%task_id% --method=%method%^
 --city_folder_name=%city_folder_name% --tile_folder_name=%tile_folder_name%^
 --source_data_path=%source_data_path% --target_path=%target_path%