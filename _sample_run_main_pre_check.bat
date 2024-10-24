@echo off
set source_base_path=".\sample_cities"
set target_base_path=".\test\test_results"
set city_folder_name="ZAF_Capetown_small_tile"
set pre_check_option="check_all"

echo:
echo Start run: %date% %time%
echo:
call python main.py --source_base_path=%source_base_path% --target_base_path=%target_base_path% --city_folder_name=%city_folder_name% --pre_check_option=%pre_check_option%
echo:
echo Finish run: %date% %time%
