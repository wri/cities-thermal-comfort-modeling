#!/bin/bash

echo "Recreating cities-thermal environment"
conda activate base
conda remove -n cities-thermal --all --yes
conda env create --file environment.yml --yes

echo "Switching back to cities-thermal env"
conda activate cities-thermal

