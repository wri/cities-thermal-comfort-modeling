## This code is used to calculate the height and aspect and saved as new file
from osgeo import gdal
from osgeo.gdalconst import *
import os, os.path
import numpy as np

def findwalls(a, walllimit):
    # This function identifies walls based on a DSM and a wall-height limit
    # Walls are represented by outer pixels within building footprints
    #
    # Fredrik Lindberg, Goteborg Urban Climate Group
    # fredrikl@gvc.gu.se
    # 20150625
    
    col = a.shape[0]
    row = a.shape[1]
    walls = np.zeros((col, row))
    domain = np.array([[0, 1, 0], [1, 0, 1], [0, 1, 0]])
    for i in np.arange(1, row-1):
        for j in np.arange(1, col-1):
            dom = a[j-1:j+2, i-1:i+2]
            # walls[j, i] = np.min(dom[np.where(domain == 1)])  # new 20171006
            walls[j, i] = np.max(dom[np.where(domain == 1)])  # new 20171006

    # walls = a-walls  # new 20171006
    walls = np.copy(walls - a)  # new 20171006
    walls[(walls < walllimit)] = 0

    walls[0:walls .shape[0], 0] = 0
    walls[0:walls .shape[0], walls .shape[1] - 1] = 0
    walls[0, 0:walls .shape[0]] = 0
    walls[walls .shape[0] - 1, 0:walls .shape[1]] = 0

    return walls


def get_ders(dsm, scale):
    # dem,_,_=read_dem_grid(dem_file)
    dx = 1/scale
    # dx=0.5
    fy, fx = np.gradient(dsm, dx, dx)
    asp, grad = cart2pol(fy, fx, 'rad')
    grad = np.arctan(grad)
    asp = asp * -1
    asp = asp + (asp < 0) * (np.pi * 2)
    return grad, asp


def saverasternd(gdal_data, filename, raster):
    rows = gdal_data.RasterYSize
    cols = gdal_data.RasterXSize
    
    outDs = gdal.GetDriverByName("GTiff").Create(filename, cols, rows, int(1), GDT_Float32)
    outBand = outDs.GetRasterBand(1)
    
    # write the data
    outBand.WriteArray(raster, 0, 0)
    # flush data to disk, set the NoData value and calculate stats
    outBand.FlushCache()
    outBand.SetNoDataValue(-9999)
    
    # georeference the image and set the projection
    outDs.SetGeoTransform(gdal_data.GetGeoTransform())
    outDs.SetProjection(gdal_data.GetProjection())


def cart2pol(x, y, units='deg'):
    radius = np.sqrt(x**2 + y**2)
    theta = np.arctan2(y, x)
    if units in ['deg', 'degs']:
        theta = theta * 180 / np.pi
    return theta, radius
    

## calculate the aspect
def aspect(dsm, walls, scale):
    # from builtins import range
    # import traceback
    # import numpy as np
    # import scipy.ndimage.interpolation as sc
    from scipy.ndimage import rotate

    import math


    ret = None
    # try:
    a = dsm
    # scale = self.scale
    # walls = self.walls
    # dlg = self.dlg

    # def filter1Goodwin_as_aspect_v3(walls, scale, a):

    row = a.shape[0]
    col = a.shape[1]

    filtersize = np.floor((scale + 0.0000000001) * 9)
    if filtersize <= 2:
        filtersize = 3
    else:
        if filtersize != 9:
            if filtersize % 2 == 0:
                filtersize = filtersize + 1

    filthalveceil = int(np.ceil(filtersize / 2.))
    filthalvefloor = int(np.floor(filtersize / 2.))

    filtmatrix = np.zeros((int(filtersize), int(filtersize)))
    buildfilt = np.zeros((int(filtersize), int(filtersize)))

    filtmatrix[:, filthalveceil - 1] = 1
    buildfilt[filthalveceil - 1, 0:filthalvefloor] = 1
    buildfilt[filthalveceil - 1, filthalveceil: int(filtersize)] = 2

    y = np.zeros((row, col))  # final direction
    z = np.zeros((row, col))  # temporary direction
    x = np.zeros((row, col))  # building side
    walls[walls > 0] = 1

    for h in range(0, 180):  # =0:1:180 #%increased resolution to 1 deg 20140911
    #     if self.killed is True:
    #             break
        # print h
        # filtmatrix1temp = sc.imrotate(filtmatrix, h, 'bilinear')
        # filtmatrix1 = np.round(filtmatrix1temp / 255.)
        # filtmatrixbuildtemp = sc.imrotate(buildfilt, h, 'nearest')
        # filtmatrixbuild = np.round(filtmatrixbuildtemp / 127.)
        filtmatrix1temp = rotate(filtmatrix, h, order=1, reshape=False, mode='nearest')  # bilinear
        filtmatrix1 = np.round(filtmatrix1temp)
        filtmatrixbuildtemp = rotate(buildfilt, h, order=0, reshape=False, mode='nearest')  # Nearest neighbor
        filtmatrixbuild = np.round(filtmatrixbuildtemp)
        index = 270-h
        if h == 150:
            filtmatrixbuild[:, filtmatrix.shape[0] - 1] = 0
        if h == 30:
            filtmatrixbuild[:, filtmatrix.shape[0] - 1] = 0
        if index == 225:
            n = filtmatrix.shape[0] - 1
            filtmatrix1[0, 0] = 1
            filtmatrix1[n, n] = 1
        if index == 135:
            n = filtmatrix.shape[0] - 1
            filtmatrix1[0, n] = 1
            filtmatrix1[n, 0] = 1
        
        for i in range(int(filthalveceil)-1, row - int(filthalveceil) - 1):  #i=filthalveceil:sizey-filthalveceil
            for j in range(int(filthalveceil)-1, col - int(filthalveceil) - 1):  #(j=filthalveceil:sizex-filthalveceil
                if walls[i, j] == 1:
                    wallscut = walls[i-filthalvefloor:i+filthalvefloor+1, j-filthalvefloor:j+filthalvefloor+1] * filtmatrix1
                    dsmcut = a[i-filthalvefloor:i+filthalvefloor+1, j-filthalvefloor:j+filthalvefloor+1]
                    if z[i, j] < wallscut.sum():  #sum(sum(wallscut))
                        z[i, j] = wallscut.sum()  #sum(sum(wallscut));
                        if np.sum(dsmcut[filtmatrixbuild == 1]) > np.sum(dsmcut[filtmatrixbuild == 2]):
                            x[i, j] = 1
                        else:
                            x[i, j] = 2

                        y[i, j] = index

    #     self.progress.emit()  # move progressbar forward

    y[(x == 1)] = y[(x == 1)] - 180
    y[(y < 0)] = y[(y < 0)] + 360

    grad, asp = get_ders(a, scale)

    y = y + ((walls == 1) * 1) * ((y == 0) * 1) * (asp / (math.pi / 180.))

    dirwalls = y

    wallresult = {'dirwalls': dirwalls}

    # if self.killed is False:
    #     self.progress.emit()
    #     ret = wallresult


    # except Exception:
    #     errorstring = self.print_exception()
    #     self.error.emit(errorstring)

    return wallresult


def run_wall_calculations(method_params):
    dsmfile = method_params['INPUT']
    walllimit = method_params['INPUT_LIMIT']
    wallheightFile = method_params['OUTPUT_HEIGHT']
    aspectFile = method_params['OUTPUT_ASPECT']

    # read primary source data
    gdal_dsm = gdal.Open(dsmfile)
    dsm = gdal_dsm.ReadAsArray().astype(float)  # dsm
    geotransform = gdal_dsm.GetGeoTransform()
    scale = 1 / geotransform[1]

    # calculate the wall height and save to file
    walls = findwalls(dsm, walllimit)
    saverasternd(gdal_dsm, wallheightFile, walls)

    # calculate the aspect and save to a file
    wallresult = aspect(dsm, walls, scale)
    dirwalls = wallresult["dirwalls"]
    saverasternd(gdal_dsm, aspectFile, dirwalls)
