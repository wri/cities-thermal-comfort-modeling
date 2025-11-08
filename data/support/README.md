# How to add a new layer to the template
1. Create a sample city that generate a VRT file for the new layer.
   1. For simplicity, use ZAF_Capetown_small_tile_upenn city folder
2. Import the layer VRT into QGIS and decide what will be the appropriate rendering symbology for the layer.
   1. Examine other layers for guidance.
3. Copy the VRT file to the local repo and place in the cities-thermal-comfort-modeling\data\support\.qgis_data folder.
4. If the UTM for the city is not EPSG:32734 (Capetown), then you will need to replace the UTM code in the VRT file with 32734.
5. Rename the file to "template_<layernamme>_0_0.vrt"
6. Use QGIS to open the template_viewer.qgs file located in cities-thermal-comfort-modeling\data\support
   1. Click on the "Keep Unavailable Layers" on the "Handle Unavailable Layers" popup window.
   1. Note that raster data will not be visible using the VRT.
7. Add the template vrt file into the Layers window at the appropriate location in the layer stack.
8. For the layer, open Properties>Symbology:
   1. Specify the desired rendering symbology.
9. Save the project file.
10. Modify the ancillary_files.py file to include the new layer.
11. Run tests and modify as needed.
Note: Each layer has its own peculiarities, so the above steps may need to be modified.
