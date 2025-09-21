#!/bin/bash

echo "Creating conda environment for cities-thermal  scripts"
conda init
conda activate base
conda remove -n cities-thermal  --all --yes
conda env create --file environment.yml --yes

echo "Switching back to cities-thermal  Conda environment"
conda init
conda activate cities-thermal 

