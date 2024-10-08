# cities-thermal-comfort-modeling processing system

Use `SOLWEIG-inputs.ipynb` to generate the input files for [SOLWEIG](https://umep-docs.readthedocs.io/projects/tutorial/en/latest/Tutorials/IntroductionToSolweig.html).

## Setup
1. Install Miniconda3 and add to system path, such as "C:\ProgramData\miniconda3\Scripts"
2. Install QGIS (v3.34.11) standalone app and add "UMEP for Processing" plugin.
3. Install PyCharm and create batch script pointing to PyCharm.bat such as "C:\Program Files\JetBrains\PyCharm Community Edition 2024.2.1\bin\pycharm.bat"
4. Use Conda with the environment.yml file to configure the Conda environment.
~~~
'conda env create -f environment.yml`
or
`conda env update -f environment.yml`
~~~
5. Determine paths to both QGIS and QGIS plugins and modify existing config.ini file as follows:
 a. Open QGIS app, enter the following in the python console, and determine paths to both QGIS app and QGIS plugins:
 ~~~
import sys
print(sys.path)
 ~~~
6. environment_post_processing.bat file
 a. Activate the cities-thermal conda environment
 b. Update the environment_post_processing.bat file with the plugins path
 c. Execute the environment_post_processing.bat file

## Execution
1. Create input dataset based on the ZZZ_template_city folder, providing source data, meteorological timeseries files, and configuration settings
1. Configure runs of city data using the umep_city_processing_registry.csv file in the source folder located above the cities folders
1. In PyCharm, run main.py specifying source/target folders


