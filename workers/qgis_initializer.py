import sys

def qgis_app_init():
    from qgis.core import QgsApplication
    from worker_tools import get_configurations

    qgis_home_path, qgis_plugin_path = get_configurations()
    sys.path.append(qgis_plugin_path)

    QgsApplication.setPrefixPath(qgis_home_path, True)
    qgis_app = QgsApplication([], False)
    qgis_app.initQgis()

    return qgis_app

