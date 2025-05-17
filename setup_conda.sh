#!/bin/bash

echo "Recreating cities-thermal environment"
conda activate base
rm -r /home/ubuntu/miniconda3/envs/cities-thermal
conda env create --file environment_linux.yml --yes

echo "Switching back to cities-thermal env"
conda activate cities-thermal

