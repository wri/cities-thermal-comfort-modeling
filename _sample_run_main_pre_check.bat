@echo off
set source_base_path=".\sample_cities"
set target_base_path=".\test\resources"

echo:
echo Start run: %date% %time%
echo:
call python main.py --source_base_path=%source_base_path% --target_base_path=%target_base_path% --pre_check_option=check_all
echo:
echo Finish run: %date% %time%
