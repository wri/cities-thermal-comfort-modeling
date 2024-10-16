REM Execute the following conda-develop command, specifying the path to QGIS plugins, such as on Windows: 
REM 'C:\Users\<user_name>\AppData\Roaming\QGIS\QGIS3\profiles\default\python\plugins
REM The conda-develop commands creates the conda.pth file. (in <conda_env_path>\Lib\site-packages)
REM To remove use "conda-develop <path> --uninstall
REM See https://stackoverflow.com/questions/35622661/import-qgis-modules-into-python-anaconda

conda-develop C:\Users\<username>\AppData\Roaming\QGIS\QGIS3\profiles\default\python\plugins

