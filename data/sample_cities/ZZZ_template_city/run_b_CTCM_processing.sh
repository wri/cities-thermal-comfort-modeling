#!/bin/bash

clear

# Default value for the boolean parameter
for arg in "$@"; do
   case "$arg" in
      shutdown_on_completion=*) shutdown_on_completion="${arg#*=}" ;;
   esac
done

# Start timer
SECONDS=0

# Set user variable
user="ubuntu"

# Set paths using user variable
path_to_main="/home/$user/github/cities-thermal-comfort-modeling/main.py"
source_base_path="/home/$user/CTCM_data_setup"
target_base_path="/home/$user/CTCM_outcome"

# Get current directory name (equivalent to Windows FOR loop)
city_folder=$(basename "$PWD")

processing_option="run_pipeline"

# Ensure the 'logs' subdirectory exists
mkdir -p logs

# Write to log file
timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
log_file="run_b_CTCM_processing_$timestamp.log"
exec > >(tee -a "logs/$log_file")
exec 2>&1

echo
echo "=================================================================================="
echo "=== Pre-checking data source in: $city_folder"
echo "=================================================================================="

# Activate conda environment
eval "$(conda shell.bash hook)"
conda activate cities-thermal

echo "Starting run: $(date)"
PYTHONUNBUFFERED=1 python "$path_to_main" --source_base_path="$source_base_path" --target_base_path="$target_base_path" --city_folder_name="$city_folder" --processing_option="$processing_option"

echo
echo "Finished run: $(date)"
echo "=================================================================================="

# Calculate and print runtime
runtime=$SECONDS
hours=$((runtime / 3600))
minutes=$(((runtime % 3600) / 60))
echo "Total runtime: ${hours} hour(s) and ${minutes} minute(s)"
echo

# # Send Slack notification (if webhook URL is set)
# slack_mention="<@U031HB1JNRZ>"  # Replace with the Slack user. Default is Chris

# message="$slack_mention CTCM processing for $city_folder completed in ${hours} hour(s) and ${minutes} minute(s)."
# curl -X POST -H 'Content-type: application/json' --data "{\"text\": \"$message\"}" https://hooks.slack.com/services/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

# Shut down ec2 instance
if [ "$shutdown_on_completion" = true ] ; then
  echo "Shutting down machine."
  sudo shutdown -h now
fi

# Equivalent to pause - wait for user input
read -p "Press any key to continue..."
