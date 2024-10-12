@echo off
set source_base_path=".\sample_cities"
set target_base_path=".\test\resources"
call python main.py --source_base_path=%source_base_path% --target_base_path=%target_base_path%