@echo off

call conda activate base

call conda remove -n cities-thermal --all --yes

call conda env create --file environment.yml --yes

call conda activate cities-thermal
