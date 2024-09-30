# cities-thermal-comfort-modeling processing system

Use `SOLWEIG-inputs.ipynb` to generate the input files for [SOLWEIG](https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/IntroductionToSolweig.html).

## Setup
1. Use Conda with the environment.yml file to configure the Conda environment.
~~~
'conda env create -f environment.yml`
or
`conda env update -f environment.yml`
~~~
2. Determine paths to both QGIS and QGIS plugins and specify in the config.ini file
1. Run the conda-develop as specified in the environment_post_processing.bat file

## Execution
1. Create input dataset based on the ZZZ_template_city folder, providing source data, meteorological timeseries files, and configuration settings
1. Configure runs of city data using the umep_city_processing_registry.csv file


