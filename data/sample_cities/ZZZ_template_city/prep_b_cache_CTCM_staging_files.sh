#!/bin/bash

clear

# Set user variable
user="ubuntu"

# Set paths using user variable
path_to_main="/home/$user/github/cities-thermal-comfort-modeling/main.py"

# Get current directory name (equivalent to Windows FOR loop)
source_base_path="/home/$user/CTCM_data_setup"
city_folder=$(basename "$PWD")

processing_option="stage_ctcm_data"

echo
echo "=================================================================================="
echo "=== Caching CTCM staging files for city specified in: $city_folder"
echo "=================================================================================="

# Activate conda environment
eval "$(conda shell.bash hook)"
conda activate cities-thermal

echo "Starting caching: $(date)"

python "$path_to_main" --source_base_path="$source_base_path" --target_base_path="" --city_folder_name="$city_folder" --processing_option="$processing_option"

echo
echo "Finished caching: $(date)"
echo "=================================================================================="
echo

# Equivalent to pause - wait for user input
read -p "Press any key to continue..."
