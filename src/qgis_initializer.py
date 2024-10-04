import sys
from qgis.core import QgsApplication
from src import tools

class QgisHandler:
    def __init__(self, instance):
        self.instance = instance
        # Initiating a QGIS application
        print('Starting QGIS Application core...')
        qgis_home_path, qgis_plugin_path = tools.get_configurations()
        sys.path.append(qgis_plugin_path)
        self.qgis_app = QgsApplication([], False)
        QgsApplication.setPrefixPath(qgis_home_path, True)

        # from processing_umep.processing_umep import ProcessingUMEPProvider
        # self.umep_provider = ProcessingUMEPProvider()
        # self.qgis_app.processingRegistry().addProvider(self.umep_provider)

        self.qgis_app.initQgis()

        # TODO what is purpose of next loop?
        # for alg in QgsApplication.processingRegistry().algorithms():
        #     print(alg.id(), "->", alg.displayName())

    # def __del__(self):
    #     self.qgis_app.exit()

    def qgis_app(self):
        return self.qgis_app

    # def instance(self):
    #     return self.instance


