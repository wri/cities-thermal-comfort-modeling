REM @echo off
set task_index=-99
set method=%1
set folder_name_city_data=%2
set folder_name_tile_data=%3
set source_data_path=%4
set target_path=%5

python .\umep_plugin_processor.py^
 --task_index=%task_index% --method=%method%^
 --folder_name_city_data=%folder_name_city_data% --folder_name_tile_data=%folder_name_tile_data%^
 --source_data_path=%source_data_path% --target_path=%target_path%