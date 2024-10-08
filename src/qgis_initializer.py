import sys
from qgis.core import QgsApplication
from src import tools
from src.logger_tools import simple_info_message


class QgisHandler:
    def __init__(self, instance):
        self.instance = instance
        # Initiating a QGIS application
        print('Starting QGIS Application core...')
        simple_info_message('Starting QGIS Application core...')
        qgis_home_path, qgis_plugin_path = tools.get_configurations()
        sys.path.append(qgis_plugin_path)
        self.qgis_app = QgsApplication([], False)
        QgsApplication.setPrefixPath(qgis_home_path, True)

        self.qgis_app.initQgis()

    def qgis_app(self):
        return self.qgis_app
