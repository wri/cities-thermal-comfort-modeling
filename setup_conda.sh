#!/bin/bash

echo "Recreating cities-thermal environment"
conda activate base
call conda remove -n cities-thermal --all --yes
conda env create --file environment_linux.yml --yes

echo "Switching back to cities-thermal env"
conda activate cities-thermal

