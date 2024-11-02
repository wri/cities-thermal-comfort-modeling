@echo off
cls

SET path_to_main=C:\Users\Administrator\Github\cities-thermal-comfort-modeling\main.py
FOR %%A IN ("%~dp0.") DO SET parent_path=%%~dpA
FOR %%* IN (.) DO SET city_folder=%%~n*
SET processing_option=run_pipeline

echo:
echo **********************************************************************************
echo *** Processing data source in: %city_folder%
echo **********************************************************************************
call conda activate cities-thermal
echo Starting run: %date% %time%
call python %path_to_main% --source_base_path=%parent_path% --target_base_path=%parent_path% --city_folder_name=%city_folder% --processing_option=%processing_option%
echo:
echo Finished run: %date% %time%
echo **********************************************************************************
echo:
