#!/bin/bash

clear

# Set user variable
user="ubuntu"

# Set paths using user variable
path_to_main="/home/$user/github/cities-thermal-comfort-modeling/main.py"
source_base_path="/home/$user/CTCM_data_setup"
target_base_path="/home/$user/CTCM_outcome"

# Get current directory name (equivalent to Windows FOR loop)
city_folder=$(basename "$PWD")

processing_option="pre_check"

# Ensure the 'logs' subdirectory exists
mkdir -p logs

# Write to log file
timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
log_file="run_a_CTCM_pre_check_$timestamp.log"
exec > >(tee -a "logs/$log_file")
exec 2>&1

echo
echo "=================================================================================="
echo "=== Pre-checking data source in: $city_folder"
echo "=================================================================================="

# Activate conda environment
eval "$(conda shell.bash hook)"
conda activate cities-thermal

echo "Starting validation: $(date)"

PYTHONUNBUFFERED=1 python "$path_to_main" --source_base_path="$source_base_path" --target_base_path="$target_base_path" --city_folder_name="$city_folder" --processing_option="$processing_option"

echo
echo "Finished validation: $(date)"
echo "=================================================================================="
echo

# Equivalent to pause - wait for user input
read -p "Press any key to continue..."
