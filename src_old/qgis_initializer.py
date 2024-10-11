from qgis.core import QgsApplication
from src_old import tools
from src_old.logger_tools import simple_info_message
import sys
from processing_umep.processing_umep import ProcessingUMEPProvider

class QgisHandler:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(QgisHandler, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        # self.instance = instance
        # Initiating a QGIS application
        print('Starting QGIS Application core...')
        simple_info_message('Starting QGIS Application core...')
        qgis_home_path, qgis_plugin_path = tools.get_configurations()
        sys.path.append(qgis_plugin_path)
        self.qgis_app = QgsApplication([], False)
        QgsApplication.setPrefixPath(qgis_home_path, True)
        # umep_provider = ProcessingUMEPProvider()
        # self.qgis_app.processingRegistry().addProvider(umep_provider)

        self.qgis_app.initQgis()
        b=2

    def qgis_app(self):
        return self.qgis_app

def create_new_instance():
    QgisHandler._instance = None
    return QgisHandler()

