from qgis.core import QgsApplication
from tools import get_configurations
import sys

def qgis_app_init():
    qgis_home_path, qgis_plugin_path = get_configurations()
    sys.path.append(qgis_plugin_path)

    QgsApplication.setPrefixPath(qgis_home_path, True)
    qgis_app = QgsApplication([], False)
    qgis_app.initQgis()

    return qgis_app


