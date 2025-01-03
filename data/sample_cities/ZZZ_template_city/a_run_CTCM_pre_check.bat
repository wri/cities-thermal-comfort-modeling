@echo off
cls

SET path_to_main=C:\Users\Administrator\Github\cities-thermal-comfort-modeling\main.py
REM FOR %%A IN ("%~dp0.") DO SET parent_path=%%~dpA
SET source_base_path=C:\CTCM_data_setup
FOR %%* IN (.) DO SET city_folder=%%~n*

SET processing_option=pre_check

echo:
echo ==================================================================================
echo === Pre-checking data source in: %city_folder%
echo ==================================================================================
call conda activate cities-thermal
echo Starting validation: %date% %time%
python %path_to_main% --source_base_path=%source_base_path% --target_base_path=%target_base_path% --city_folder_name=%city_folder% --processing_option=%processing_option%
echo:
echo Finished validation: %date% %time%
echo ==================================================================================
echo:

pause