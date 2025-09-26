## This script is used to calculate different types of SVFs based on DSM
## modified based on UMEP without GUI
## Last modified May 9, 2020, by Xiaojiang Li, Temple University, Glenside, PA
import gc
import os, os.path
import numpy as np
import rasterio
import zipfile
import shutil

from osgeo import gdal
from osgeo.gdalconst import *

def annulus_weight(altitude, aziinterval):
    '''assign different weight to different annulus'''
    n = 90.
    steprad = (360./aziinterval) * (np.pi/180.)
    annulus = 91.-altitude
    w = (1./(2.*np.pi)) * np.sin(np.pi / (2.*n)) * np.sin((np.pi * (2. * annulus - 1.)) / (2. * n))
    weight = steprad * w
    
    return weight


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


# def shadowingfunctionglobalradiation(a, azimuth, altitude, scale, dlg, forsvf):
def shadowingfunctionglobalradiation(a, azimuth, altitude, scale, forsvf):
    
    #%This m.file calculates shadows on a DEM
    #% conversion
    degrees = np.pi/180.
    if azimuth == 0.0:
        azimuth = 0.000000000001
    azimuth = np.dot(azimuth, degrees)
    altitude = np.dot(altitude, degrees)
    #% measure the size of the image
    sizex = a.shape[0]
    sizey = a.shape[1]
    if forsvf == 0:
        barstep = np.max([sizex, sizey])
#         dlg.progressBar.setRange(0, barstep)
    #% initialise parameters
    f = a
    dx = 0.
    dy = 0.
    dz = 0.
    temp = np.zeros((sizex, sizey))
    index = 1.
    #% other loop parameters
    amaxvalue = a.max()
    pibyfour = np.pi/4.
    threetimespibyfour = 3.*pibyfour
    fivetimespibyfour = 5.*pibyfour
    seventimespibyfour = 7.*pibyfour
    sinazimuth = np.sin(azimuth)
    cosazimuth = np.cos(azimuth)
    tanazimuth = np.tan(azimuth)
    signsinazimuth = np.sign(sinazimuth)
    signcosazimuth = np.sign(cosazimuth)
    dssin = np.abs((1./sinazimuth))
    dscos = np.abs((1./cosazimuth))
    tanaltitudebyscale = np.tan(altitude) / scale

    #% main loop
    while (amaxvalue >= dz and np.abs(dx) < sizex and np.abs(dy) < sizey):
#         if forsvf == 0:
#             dlg.progressBar.setValue(index)
    #while np.logical_and(np.logical_and(amaxvalue >= dz, np.abs(dx) <= sizex), np.abs(dy) <= sizey):(np.logical_and(amaxvalue >= dz, np.abs(dx) <= sizex), np.abs(dy) <= sizey):
        #if np.logical_or(np.logical_and(pibyfour <= azimuth, azimuth < threetimespibyfour), np.logical_and(fivetimespibyfour <= azimuth, azimuth < seventimespibyfour)):
        if (pibyfour <= azimuth and azimuth < threetimespibyfour or fivetimespibyfour <= azimuth and azimuth < seventimespibyfour):
            dy = signsinazimuth * index
            dx = -1. * signcosazimuth * np.abs(np.round(index / tanazimuth))
            ds = dssin
        else:
            dy = signsinazimuth * np.abs(np.round(index * tanazimuth))
            dx = -1. * signcosazimuth * index
            ds = dscos

        #% note: dx and dy represent absolute values while ds is an incremental value
        dz = ds *index * tanaltitudebyscale
        temp[0:sizex, 0:sizey] = 0.
        absdx = np.abs(dx)
        absdy = np.abs(dy)
        xc1 = (dx+absdx)/2.+1.
        xc2 = sizex+(dx-absdx)/2.
        yc1 = (dy+absdy)/2.+1.
        yc2 = sizey+(dy-absdy)/2.
        xp1 = -((dx-absdx)/2.)+1.
        xp2 = sizex-(dx+absdx)/2.
        yp1 = -((dy-absdy)/2.)+1.
        yp2 = sizey-(dy+absdy)/2.
        temp[int(xp1)-1:int(xp2), int(yp1)-1:int(yp2)] = a[int(xc1)-1:int(xc2), int(yc1)-1:int(yc2)]-dz
        # f = np.maximum(f, temp)  # bad performance in python3. Replaced with fmax
        f = np.fmax(f, temp)
        index += 1.
    
    f = f-a
    f = np.logical_not(f)
    sh = np.double(f)

    del f
    del a
    
    return sh
    

# def shadowingfunction_20(a, vegdem, vegdem2, azimuth, altitude, scale, amaxvalue, bush, dlg, forsvf):
def shadowingfunction_20(a, vegdem, vegdem2, azimuth, altitude, scale, amaxvalue, bush, forsvf):
    #% This function casts shadows on buildings and vegetation units
    #% conversion
    
    degrees = np.pi/180.
    if azimuth == 0.0:
        azimuth = 0.000000000001
    azimuth = np.dot(azimuth, degrees)
    altitude = np.dot(altitude, degrees)
    #% measure the size of the image
    sizex = a.shape[0]
    sizey = a.shape[1]
    #% initialise parameters
    if forsvf == 0:
        barstep = np.max([sizex, sizey])
    
    dx = 0.
    dy = 0.
    dz = 0.
    temp = np.zeros((sizex, sizey))
    tempvegdem = np.zeros((sizex, sizey))
    tempvegdem2 = np.zeros((sizex, sizey))
    sh = np.zeros((sizex, sizey))
    vbshvegsh = np.zeros((sizex, sizey))
    vegsh = np.zeros((sizex, sizey))
    tempbush = np.zeros((sizex, sizey))
    f = a
    g = np.zeros((sizex, sizey))
    bushplant = bush > 1.
    pibyfour = np.pi/4.
    threetimespibyfour = 3.*pibyfour
    fivetimespibyfour = 5.*pibyfour
    seventimespibyfour = 7.*pibyfour
    sinazimuth = np.sin(azimuth)
    cosazimuth = np.cos(azimuth)
    tanazimuth = np.tan(azimuth)
    signsinazimuth = np.sign(sinazimuth)
    signcosazimuth = np.sign(cosazimuth)
    dssin = np.abs((1./sinazimuth))
    dscos = np.abs((1./cosazimuth))
    tanaltitudebyscale = np.tan(altitude) / scale
    index = 1
    
    #% main loop
    while (amaxvalue >= dz and np.abs(dx) < sizex and np.abs(dy) < sizey):
#         if forsvf == 0:
#             dlg.progressBar.setValue(index)
        if (pibyfour <= azimuth and azimuth < threetimespibyfour or fivetimespibyfour <= azimuth and azimuth < seventimespibyfour):
            dy = signsinazimuth * index
            dx = -1. * signcosazimuth * np.abs(np.round(index / tanazimuth))
            ds = dssin
        else:
            dy = signsinazimuth * np.abs(np.round(index * tanazimuth))
            dx = -1. * signcosazimuth * index
            ds = dscos
        
        #% note: dx and dy represent absolute values while ds is an incremental value
        dz = np.dot(np.dot(ds, index), tanaltitudebyscale)
        tempvegdem[0:sizex, 0:sizey] = 0.
        tempvegdem2[0:sizex, 0:sizey] = 0.
        temp[0:sizex, 0:sizey] = 0.
        absdx = np.abs(dx)
        absdy = np.abs(dy)
        xc1 = (dx+absdx)/2.+1.
        xc2 = sizex+(dx-absdx)/2.
        yc1 = (dy+absdy)/2.+1.
        yc2 = sizey+(dy-absdy)/2.
        xp1 = -((dx-absdx)/2.)+1.
        xp2 = sizex-(dx+absdx)/2.
        yp1 = -((dy-absdy)/2.)+1.
        yp2 = sizey-(dy+absdy)/2.
        tempvegdem[int(xp1)-1:int(xp2), int(yp1)-1:int(yp2)] = vegdem[int(xc1)-1:int(xc2), int(yc1)-1:int(yc2)]-dz
        tempvegdem2[int(xp1)-1:int(xp2), int(yp1)-1:int(yp2)] = vegdem2[int(xc1)-1:int(xc2), int(yc1)-1:int(yc2)]-dz
        temp[int(xp1)-1:int(xp2), int(yp1)-1:int(yp2)] = a[int(xc1)-1:int(xc2), int(yc1)-1:int(yc2)]-dz
        # f = np.maximum(f, temp) # bad performance in python3. Replaced with fmax
        f = np.fmax(f, temp)
        sh[(f > a)] = 1.
        sh[(f <= a)] = 0.
        #%Moving building shadow
        fabovea = tempvegdem > a
        #%vegdem above DEM
        gabovea = tempvegdem2 > a
        #%vegdem2 above DEM
        # vegsh2 = np.float(fabovea)-np.float(gabovea)
        vegsh2 = np.subtract(fabovea, gabovea, dtype=float)
        # vegsh = np.maximum(vegsh, vegsh2) # bad performance in python3. Replaced with fmax
        vegsh = np.fmax(vegsh, vegsh2)
        vegsh[(vegsh*sh > 0.)] = 0.
        #% removing shadows 'behind' buildings
        vbshvegsh = vegsh+vbshvegsh
        #% vegsh at high sun altitudes
        if index == 1.:
            firstvegdem = tempvegdem-temp
            firstvegdem[(firstvegdem <= 0.)] = 1000.
            vegsh[(firstvegdem < dz)] = 1.
            vegsh = vegsh*(vegdem2 > a)
            vbshvegsh = np.zeros((sizex, sizey))

        #% Bush shadow on bush plant
        if np.logical_and(bush.max() > 0., np.max((fabovea*bush)) > 0.):
            tempbush[0:sizex, 0:sizey] = 0.
            tempbush[int(xp1)-1:int(xp2), int(yp1)-1:int(yp2)] = bush[int(xc1)-1:int(xc2),int(yc1)-1:int(yc2)]-dz
            # g = np.maximum(g, tempbush) # bad performance in python3. Replaced with fmax
            g = np.fmax(g, tempbush)
            g *= bushplant
        index += 1.
    
    sh = 1.-sh
    vbshvegsh[(vbshvegsh > 0.)] = 1.
    vbshvegsh = vbshvegsh-vegsh
    
    if bush.max() > 0.:
        g = g-bush
        g[(g > 0.)] = 1.
        g[(g < 0.)] = 0.
        vegsh = vegsh-bushplant+g
        vegsh[(vegsh<0.)] = 0.
    
    vegsh[(vegsh > 0.)] = 1.
    vegsh = 1.-vegsh
    vbshvegsh = 1.-vbshvegsh
    
    shadowresult = {'sh': sh, 'vegsh': vegsh, 'vbshvegsh': vbshvegsh}

    del bushplant
    del f
    del fabovea
    del firstvegdem
    del g
    del gabovea
    del sh
    del temp
    del tempbush
    del tempvegdem
    del tempvegdem2
    del vbshvegsh
    del vegsh
    del vegsh2

    return shadowresult


def run_skyview_calculations(method_params, tile_name):
    # Expand parameters into local variables
    INPUT_DSM = method_params['INPUT_DSM']
    INPUT_CDSM = method_params['INPUT_CDSM']
    # TRANS_VEG = method_params['TRANS_VEG']  # CTCM/UPenn does not use this parameter
    # INPUT_TDSM = method_params['INPUT_TDSM']  # CTCM/UPenn does not use this parameter
    # INPUT_THEIGHT = method_params['INPUT_THEIGHT'] # CTCM/UPenn does not use this parameter
    # ANISO = method_params['ANISO']  # CTCM/UPenn does not use this parameter
    # OUTPUT_DIR = method_params['OUTPUT_DIR']   # CTCM/UPenn does not use this parameter
    OUTPUT_FILE = method_params['OUTPUT_FILE']

    # Map UMEP parameters to UPenn variables
    dsmfile = INPUT_DSM
    chmfile = INPUT_CDSM
    svffolder = OUTPUT_FILE

    # the trunk data is not existe, therefore use 25% as default setting in UMEP
    trunkratio = 0.25 #25/100, from the svf_calculator.py in UMEP

    ## read the land use file, and judge if we need to process the tile
    # gdal_lu = gdal.Open(lufile)
    # lu_img = gdal_lu.ReadAsArray().astype(np.float)
    # if not np.min(lu_img) < 255: continue


    with rasterio.open(dsmfile) as dsmlayer:
        dsmimg = dsmlayer.read(1)

    gdal_dsm = gdal.Open(dsmfile)
    dsm = gdal_dsm.ReadAsArray().astype(float)#dsm
    geotransform = gdal_dsm.GetGeoTransform()
    scale = 1 / geotransform[1]

    # gdal_dem = gdal.Open(demfile) #dem
    # dem = gdal_dem.ReadAsArray().astype(np.float)

    nd = gdal_dsm.GetRasterBand(1).GetNoDataValue()
    dsm[dsm == nd] = 0.
    if dsm.min() < 0:
        dsm = dsm + np.abs(dsm.min())

    # read the vegetation canopy height model
    dataSet = gdal.Open(chmfile)
    vegdem = dataSet.ReadAsArray().astype(float)

    vegdem2 = vegdem * trunkratio

    rows = dsm.shape[0]
    cols = dsm.shape[1]
    svf = np.zeros([rows, cols])
    svfE = svf
    svfS = svf
    svfW = svf
    svfN = svf
    svfveg = np.zeros((rows, cols))
    svfEveg = np.zeros((rows, cols))
    svfSveg = np.zeros((rows, cols))
    svfWveg = np.zeros((rows, cols))
    svfNveg = np.zeros((rows, cols))
    svfaveg = np.zeros((rows, cols))
    svfEaveg = np.zeros((rows, cols))
    svfSaveg = np.zeros((rows, cols))
    svfWaveg = np.zeros((rows, cols))
    svfNaveg = np.zeros((rows, cols))

    # % amaxvalue
    vegmax = vegdem.max()
    amaxvalue = dsmimg.max()
    amaxvalue = np.maximum(amaxvalue, vegmax)

    # % Elevation vegdems if buildingDEM inclused ground heights
    vegdem = vegdem + dsmimg
    vegdem[vegdem == dsmimg] = 0

    vegdem2 = vegdem2 + dsmimg
    vegdem2[vegdem2 == dsmimg] = 0
    # % Bush separation
    bush = np.logical_not((vegdem2 * vegdem)) * vegdem

    shmat = np.zeros((rows, cols, 145))
    vegshmat = np.zeros((rows, cols, 145))

    index = int(0)
    iangle = np.array([6, 18, 30, 42, 54, 66, 78, 90])
    skyvaultaziint = np.array([12, 12, 15, 15, 20, 30, 60, 360])
    aziinterval = np.array([30, 30, 24, 24, 18, 12, 6, 1])
    azistart = np.array([0, 4, 2, 5, 8, 0, 10, 0])
    annulino = np.array([0, 12, 24, 36, 48, 60, 72, 84, 90])
    iazimuth = np.hstack(np.zeros((1, 145)))
    skyvaultaltint = np.array([6, 18, 30, 42, 54, 66, 78, 90])

    for j in range(0, 8):
        for k in range(0, int(360 / skyvaultaziint[j])):
            iazimuth[index] = k * skyvaultaziint[j] + azistart[j]
            if iazimuth[index] > 360.:
                iazimuth[index] = iazimuth[index] - 360.
            index = index + 1


    aziintervalaniso = np.ceil(aziinterval / 2.0)
    index = int(0)

    #for i in np.arange(0, iangle.shape[0]-1):
    for i in range(0, skyvaultaltint.shape[0]):
        for j in np.arange(0, (aziinterval[int(i)])):
            altitude = iangle[int(i)]
            azimuth = iazimuth[int(index)-1]

            # print status for azimuth 0
            if (index+1)%20 == 0:
                print(f"tile: {tile_name}, skyview-step: {index+1}/{len(iazimuth)}, altitude: {altitude}/{iangle.max()}, azimuth: {azimuth}")

            # Casting shadow, when the vegetation dsm is considered
            # if self.usevegdem == 1:
            shadowresult = shadowingfunction_20(dsmimg, vegdem, vegdem2, azimuth, altitude,
                                                       scale, amaxvalue, bush, 1)

            vegsh = shadowresult["vegsh"]
            vbshvegsh = shadowresult["vbshvegsh"]
            vegshmat[:, :, index] = vegsh

            sh = shadowingfunctionglobalradiation(dsmimg, azimuth, altitude, scale, 1)
            shmat[:, :, index] = sh


            # Calculate svfs
            k_start = annulino[int(i)]+1
            k_end = (annulino[int(i+1.)])+1
            for k in np.arange(k_start, k_end):
                weight = annulus_weight(k, aziinterval[i])*sh
                svf = svf + weight
                weight = annulus_weight(k, aziintervalaniso[i]) * sh
                if (azimuth >= 0) and (azimuth < 180):
                    # weight = self.annulus_weight(k, aziintervalaniso[i])*sh
                    svfE = svfE + weight
                if (azimuth >= 90) and (azimuth < 270):
                    # weight = self.annulus_weight(k, aziintervalaniso[i])*sh
                    svfS = svfS + weight
                if (azimuth >= 180) and (azimuth < 360):
                    # weight = self.annulus_weight(k, aziintervalaniso[i])*sh
                    svfW = svfW + weight
                if (azimuth >= 270) or (azimuth < 90):
                    # weight = self.annulus_weight(k, aziintervalaniso[i])*sh
                    svfN = svfN + weight


            # WRI Note: Switch to inline variable assignment to reduce memory requirements
            # if self.usevegdem == 1:
            for k in np.arange(k_start, k_end):
                # % changed to include 90
                weight = annulus_weight(k, aziinterval[i])
                svfveg += weight * vegsh
                svfaveg += weight * vbshvegsh
                weight = annulus_weight(k, aziintervalaniso[i])
                if (azimuth >= 0) and (azimuth < 180):
                    svfEveg += weight * vegsh
                    svfEaveg += weight * vbshvegsh
                if (azimuth >= 90) and (azimuth < 270):
                    svfSveg += weight * vegsh
                    svfSaveg += weight * vbshvegsh
                if (azimuth >= 180) and (azimuth < 360):
                    svfWveg += weight * vegsh
                    svfWaveg += weight * vbshvegsh
                if (azimuth >= 270) or (azimuth < 90):
                    svfNveg += weight * vegsh
                    svfNaveg += weight * vbshvegsh

            # WRI Note: Added deletion of unneeded objects to reduce memory pressure
            del altitude
            del azimuth
            del vegsh
            del vbshvegsh
            del sh

            index += 1

    svfS = svfS + 3.0459e-004
    svfW = svfW + 3.0459e-004
    # % Last azimuth is 90. Hence, manual add of last annuli for svfS and SVFW
    # %Forcing svf not be greater than 1 (some MATLAB crazyness)
    svf[(svf > 1.)] = 1.
    svfE[(svfE > 1.)] = 1.
    svfS[(svfS > 1.)] = 1.
    svfW[(svfW > 1.)] = 1.
    svfN[(svfN > 1.)] = 1.


    # use the vegetation DSM
    # if self.usevegdem == 1:
    last = np.zeros((rows, cols))
    last[(vegdem2 == 0.)] = 3.0459e-004
    svfSveg = svfSveg + last
    svfWveg = svfWveg + last
    svfSaveg = svfSaveg + last
    svfWaveg = svfWaveg + last
    # %Forcing svf not be greater than 1 (some MATLAB crazyness)
    svfveg[(svfveg > 1.)] = 1.
    svfEveg[(svfEveg > 1.)] = 1.
    svfSveg[(svfSveg > 1.)] = 1.
    svfWveg[(svfWveg > 1.)] = 1.
    svfNveg[(svfNveg > 1.)] = 1.
    svfaveg[(svfaveg > 1.)] = 1.
    svfEaveg[(svfEaveg > 1.)] = 1.
    svfSaveg[(svfSaveg > 1.)] = 1.
    svfWaveg[(svfWaveg > 1.)] = 1.
    svfNaveg[(svfNaveg > 1.)] = 1.


    svfresult = {'svf': svf, 'svfE': svfE, 'svfS': svfS, 'svfW': svfW, 'svfN': svfN,
                 'svfveg': svfveg, 'svfEveg': svfEveg, 'svfSveg': svfSveg, 'svfWveg': svfWveg,
                 'svfNveg': svfNveg, 'svfaveg': svfaveg, 'svfEaveg': svfEaveg, 'svfSaveg': svfSaveg,
                 'svfWaveg': svfWaveg, 'svfNaveg': svfNaveg, 'shmat': shmat,'vegshmat': vegshmat}


    ## output the svfs into a zip file
    svfs = ['svf', 'svfE', 'svfS', 'svfW', 'svfN', 'svfveg', \
            'svfEveg', 'svfSveg', 'svfWveg', 'svfNveg', 'svfaveg', \
            'svfEaveg', 'svfSaveg', 'svfWaveg', 'svfNaveg']

    for svf in svfs:
        svffile = os.path.join(svffolder, svf + '.tif')
        saverasternd(gdal_dsm, svffile, svfresult[svf])
        #os.remove(svffile)

    del svf
    del svfaveg
    del svfE
    del svfEaveg
    del svfEveg
    del svfN
    del svfNaveg
    del svfNveg
    del svfS
    del svfSaveg
    del svfSveg
    del svfveg
    del svfW
    del svfWaveg
    del svfWveg

    gc.collect()
