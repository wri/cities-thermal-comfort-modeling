@echo off
set source_base_path=".\sample_cities"
set target_base_path=".\test\resources"

echo:
echo Start run: Sat 10/12/2024 10:05:12.06
echo:
call python main.py --source_base_path=%source_base_path% --target_base_path=%target_base_path%
echo:
echo Finish run: Sat 10/12/2024 10:05:12.06
