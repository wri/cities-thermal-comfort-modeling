%echo off

echo Recreating cities-thermal environment
cmd "/c conda activate base && call conda remove -n cities-thermal --all --yes && conda env create --file environment.yml --yes"

echo Switching back to cities-thermal env
call conda activate cities-thermal

echo Adding plugins 
call environment_post_processing.bat
