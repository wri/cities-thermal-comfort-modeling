import processing
import sys
from processing.core.Processing import Processing
from processing_umep.processing_umep import ProcessingUMEPProvider

from src_old.qgis_initializer import create_new_instance


# from qgis.core import QgsApplication

# from src_old.qgis_initializer import QgisHandler
# from src_old.tools import get_configurations

# qgis_home_path, qgis_plugin_path = get_configurations()
# sys.path.append(qgis_plugin_path)
# qgis_app = QgsApplication([], False)
# QgsApplication.setPrefixPath(qgis_home_path, True)
# qgis_app.initQgis()

# umep_provider = ProcessingUMEPProvider()
# QH = QgisHandler(0)
# qgis_app = QH.qgis_app
# qgis_app.processingRegistry().addProvider(umep_provider)
# Processing.initialize()


# def run_skyview_factor(parin):
#     Processing.initialize()
#     processing.run("umep:Urban Geometry: Sky View Factor", parin)
#
# def run_solweig(parin):
#     Processing.initialize()
#     processing.run("umep:Outdoor Thermal Comfort: SOLWEIG", parin)


# class UmepProcessingQgisPlugins:
#     _instance = None
#
#     def __new__(cls, *args, **kwargs):
#         if not cls._instance:
#             cls._instance = super(UmepProcessingQgisPlugins, cls).__new__(cls, *args, **kwargs)
#         return cls._instance
#
#     def __init__(self):
#         from src_old.qgis_initializer import QgisHandler
#         QH = QgisHandler
#
#         self.qgis_app = QH.qgis_app
#         # umep_provider = ProcessingUMEPProvider()
#         # self.qgis_app.processingRegistry().addProvider(umep_provider)
#         b = 2
#
#     def proc(self):
#         umep_provider = ProcessingUMEPProvider()
#         self.qgis_app.processingRegistry().addProvider(umep_provider)
#         return self.qgis_app
#         # pp = self.qgis_app.processingRegistry().providers()
#         # b = 2
#         # return pp
#
#     def __del__(self):
#         # Processing.deinitialize()
#         # self.qgis_app.exit()
#         self.qgis_app.exitQgis()
#
# def create_new_instance():
#     UmepProcessingQgisPlugins._instance = None
#     return UmepProcessingQgisPlugins()

def run_wall_height_aspect(parin):
    umep_provider = ProcessingUMEPProvider()
    UmepProcessingQgisPlugins = create_new_instance()
    qgis_app = UmepProcessingQgisPlugins.qgis_app
    qgis_app.processingRegistry().addProvider(umep_provider)
    processing.Processing.initialize()
    # processing.Processing.activateProvider(ProcessingUMEPProvider)
    processing.run("umep:Urban Geometry: Wall Height and Aspect", parin)

    # upp = UmepProcessingQgisPlugins()
    # try:
    #     UmepProcessingQgisPlugins = create_new_instance()
    #     qgis_app = UmepProcessingQgisPlugins.qgis_app
    #     umep_provider = ProcessingUMEPProvider()
    #     qgis_app.processingRegistry().addProvider(umep_provider)
    #     Processing.initialize()
    #     processing.run("umep:Urban Geometry: Wall Height and Aspect", parin)
    # except Exception as e_msg:
    #     print(e_msg)
    #     b = 2
    #
    #     b = 2
