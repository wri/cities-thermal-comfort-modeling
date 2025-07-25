# -*- coding: utf-8 -*-
## by Xiaojiang Li, Temple University, we can folder the function
## to make the script cleaner.
## updated and cleaned o July 12, 2023

## update by Xiaojiang Li, UPenn, June 6, 2025, 
## now the script is able to take input of scalar and np array met data

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os, os.path
import numpy as np
from osgeo import gdal
from osgeo import osr, ogr
from osgeo.gdalconst import *
import datetime
import calendar
import math


# def stackRasters(dsm_gdal, lu_gdal, tilesize):
def stackRasters(dsm_gdal, lu_gdal):
    '''This function is used to stack raster with different dimensions
    The function will create a new raster based on the larger raster and 
    fill the gap as zero
    
    Parameters:
        dsm_gdal: gdal object of the larger raster file
        lu_gdal: the gdal object of the smaller raster file

    by Xiaojiang Li, Temple University
    '''

    lu_transform = lu_gdal.GetGeoTransform()
    dsm_transform = dsm_gdal.GetGeoTransform()

    tilewidth = dsm_gdal.RasterXSize 
    tileheight = dsm_gdal.RasterYSize
    # print('the width and height are:', tilewidth, tileheight)
    # this code assume that the dem and dsm are the largest tiles
    # tilesize = 2000 #1000
    outDSMimg = np.zeros((tileheight, tilewidth), dtype=float) #2000
    outLUimg = np.zeros((tileheight, tilewidth), dtype=int)


    # the right and bottom limit of the land use raster and dsm rasters
    luMaxX = lu_transform[0] + (lu_transform[1] * lu_gdal.RasterXSize)
    luMinY = lu_transform[3] + (lu_transform[5] * lu_gdal.RasterYSize)
    
    dsmMaxX = dsm_transform[0] + (dsm_transform[1] * dsm_gdal.RasterXSize)
    dsmMinY = dsm_transform[3] + (dsm_transform[5] * dsm_gdal.RasterYSize)
    
    # by comparing different boundaries and find overlaped area on each raster
    xstart = max(lu_transform[0], dsm_transform[0])
    xend = min(luMaxX, dsmMaxX)
    ydown = max(luMinY, dsmMinY)
    ytop= min(lu_transform[3], dsm_transform[3])
    
    # print(xstart, xend, ydown, ytop)
    # print(lu_transform)
    
    # get the xoff, yoff for two rasters
    dsm_xoff = int((xstart - dsm_transform[0])/dsm_transform[1])
    dsm_yoff = int((ytop - dsm_transform[3])/dsm_transform[5])
    lu_xoff = int((xstart - lu_transform[0])/lu_transform[1])
    lu_yoff = int((ytop - lu_transform[3])/lu_transform[5])

    # the number of cols and rows of the overlaped area in pixels
    cols = int((xend - xstart)/lu_transform[1])
    rows = int((ydown - ytop)/lu_transform[5])

    # based on the offs and row, cols to read pixel information
    print('lu_xoff, luyoff, cols, rows', lu_xoff, lu_yoff, cols, rows)
    lu_img = lu_gdal.ReadAsArray(lu_xoff, lu_yoff, cols, rows) # read the larger raster
    print('The shape of luimg and outLUimg is:', lu_img.shape, outLUimg.shape)

    if dsm_yoff > 1:
        dsm_yoff = dsm_yoff+1
    if dsm_xoff > 1:
        dsm_xoff = dsm_xoff + 1

    # print('dsm_yoff, dsm_xoff:', dsm_yoff, dsm_xoff)
    end_row = dsm_yoff+rows
    end_col = dsm_xoff+cols
    if end_row > rows - 1: end_row = rows - 1
    if end_col > cols - 1: end_col = cols - 1

    # print('The shape of lu_img is:', lu_img.shape)
    # print('The shape of the outLUimg is:', outLUimg.shape)
    # print(dsm_yoff, end_row, dsm_xoff, end_col)
    
    outLUimg[dsm_yoff: end_row, dsm_xoff: end_col] = lu_img[0: end_row - dsm_yoff, 0: end_col - dsm_xoff]
    
    return outLUimg

def Lvikt_veg(svf,svfveg,svfaveg,vikttot):

    # Least
    viktonlywall=(vikttot-(63.227*svf**6-161.51*svf**5+156.91*svf**4-70.424*svf**3+16.773*svf**2-0.4863*svf))/vikttot

    viktaveg=(vikttot-(63.227*svfaveg**6-161.51*svfaveg**5+156.91*svfaveg**4-70.424*svfaveg**3+16.773*svfaveg**2-0.4863*svfaveg))/vikttot

    viktwall=viktonlywall-viktaveg

    svfvegbu=(svfveg+svf-1)  # Vegetation plus buildings
    viktsky=(63.227*svfvegbu**6-161.51*svfvegbu**5+156.91*svfvegbu**4-70.424*svfvegbu**3+16.773*svfvegbu**2-0.4863*svfvegbu)/vikttot
    viktrefl=(vikttot-(63.227*svfvegbu**6-161.51*svfvegbu**5+156.91*svfvegbu**4-70.424*svfvegbu**3+16.773*svfvegbu**2-0.4863*svfvegbu))/vikttot
    viktveg=(vikttot-(63.227*svfvegbu**6-161.51*svfvegbu**5+156.91*svfvegbu**4-70.424*svfvegbu**3+16.773*svfvegbu**2-0.4863*svfvegbu))/vikttot
    viktveg=viktveg-viktwall

    return viktveg,viktwall,viktsky,viktrefl

def Kvikt_veg(svf,svfveg,vikttot):

    # Least
    viktwall=(vikttot-(63.227*svf**6-161.51*svf**5+156.91*svf**4-70.424*svf**3+16.773*svf**2-0.4863*svf))/vikttot
    
    svfvegbu=(svfveg+svf-1)  # Vegetation plus buildings
    viktveg=(vikttot-(63.227*svfvegbu**6-161.51*svfvegbu**5+156.91*svfvegbu**4-70.424*svfvegbu**3+16.773*svfvegbu**2-0.4863*svfvegbu))/vikttot
    viktveg=viktveg-viktwall
    
    return viktveg,viktwall

def Kside_veg_v2019a(radI,radD,radG,shadow,svfS,svfW,svfN,svfE,svfEveg,svfSveg,svfWveg,svfNveg,azimuth,altitude,psi,t,albedo,F_sh,KupE,KupS,KupW,KupN,cyl,lv,ani,diffsh,rows,cols):

    # New reflection equation 2012-05-25
    vikttot=4.4897
    aziE=azimuth+t
    aziS=azimuth-90+t
    aziW=azimuth-180+t
    aziN=azimuth-270+t
    deg2rad=np.pi/180
    KsideD = np.zeros((rows, cols))

    ### Direct radiation ###
    if cyl == 1: ### Kside with cylinder ###
        KsideI=shadow*radI*np.cos(altitude*deg2rad)
        KeastI=0;KsouthI=0;KwestI=0;KnorthI=0
    else: ### Kside with weights ###
        if azimuth > (360-t)  or  azimuth <= (180-t):
            KeastI=radI*shadow*np.cos(altitude*deg2rad)*np.sin(aziE*deg2rad)
        else:
            KeastI=0
        if azimuth > (90-t)  and  azimuth <= (270-t):
            KsouthI=radI*shadow*np.cos(altitude*deg2rad)*np.sin(aziS*deg2rad)
        else:
            KsouthI=0
        if azimuth > (180-t)  and  azimuth <= (360-t):
            KwestI=radI*shadow*np.cos(altitude*deg2rad)*np.sin(aziW*deg2rad)
        else:
            KwestI=0
        if azimuth <= (90-t)  or  azimuth > (270-t):
            KnorthI=radI*shadow*np.cos(altitude*deg2rad)*np.sin(aziN*deg2rad)
        else:
            KnorthI=0

        KsideI=shadow*0
    
    ### Diffuse and reflected radiation ###
    [viktveg,viktwall]=Kvikt_veg(svfE,svfEveg,vikttot)
    svfviktbuvegE=(viktwall+(viktveg)*(1-psi))

    [viktveg,viktwall]=Kvikt_veg(svfS,svfSveg,vikttot)
    svfviktbuvegS=(viktwall+(viktveg)*(1-psi))

    [viktveg,viktwall]=Kvikt_veg(svfW,svfWveg,vikttot)
    svfviktbuvegW=(viktwall+(viktveg)*(1-psi))

    [viktveg,viktwall]=Kvikt_veg(svfN,svfNveg,vikttot)
    svfviktbuvegN=(viktwall+(viktveg)*(1-psi))

    ### Anisotropic Diffuse Radiation after Perez et al. 1993 ###
    if ani == 1:

        aniAlt = lv[0][:, 0]
        aniAzi = lv[0][:, 1]
        aniLum = lv[0][:, 2]

        phiVar = np.zeros((145, 1))

        radTot = np.zeros(1)

        for ix in range(0, 145):    # Azimuth delta
            if ix < 60:
                aziDel = 12
            elif ix >= 60 and ix < 108:
                aziDel = 15
            elif ix >= 108 and ix < 126:
                aziDel = 20
            elif ix >= 126 and ix < 138:
                aziDel = 30
            elif ix >= 138 and ix < 144:
                aziDel = 60
            elif ix == 144:
                aziDel = 360

            phiVar[ix] = (aziDel * deg2rad) * (np.sin((aniAlt[ix] + 6) * deg2rad) - np.sin((aniAlt[ix] - 6) * deg2rad)) # Solid angle / Steradian

            radTot = radTot + (aniLum[ix] * phiVar[ix] * np.sin(aniAlt[ix] * deg2rad)) # Radiance fraction normalization

        lumChi = (aniLum * radD) / radTot # Radiance fraction normalization

        if cyl == 1:
            for idx in range(0, 145):
                anglIncC = np.cos(aniAlt[idx] * deg2rad) * np.cos(0) * np.sin(np.pi / 2) + np.sin(
                    aniAlt[idx] * deg2rad) * np.cos(np.pi / 2)                                    # Angle of incidence, np.cos(0) because cylinder - always perpendicular
                KsideD = KsideD + diffsh[:, :, idx] * lumChi[idx] * anglIncC * phiVar[idx]        # Diffuse vertical radiation
            Keast  = (albedo * (svfviktbuvegE * (radG * (1 - F_sh) + radD * F_sh)) + KupE) * 0.5
            Ksouth = (albedo * (svfviktbuvegS * (radG * (1 - F_sh) + radD * F_sh)) + KupS) * 0.5
            Kwest  = (albedo * (svfviktbuvegW * (radG * (1 - F_sh) + radD * F_sh)) + KupW) * 0.5
            Knorth = (albedo * (svfviktbuvegN * (radG * (1 - F_sh) + radD * F_sh)) + KupN) * 0.5
        else: # Box
            diffRadE = np.zeros((rows, cols)); diffRadS = np.zeros((rows, cols)); diffRadW = np.zeros((rows, cols)); diffRadN = np.zeros((rows, cols))

            for idx in range(0, 145):
                if aniAzi[idx] <= (180):
                    anglIncE = np.cos(aniAlt[idx] * deg2rad) * np.cos((90 - aniAzi[idx]) * deg2rad) * np.sin(
                        np.pi / 2) + np.sin(
                        aniAlt[idx] * deg2rad) * np.cos(np.pi / 2)
                    diffRadE = diffRadE + diffsh[:, :, idx] * lumChi[idx] * anglIncE * phiVar[idx] #* 0.5

                if aniAzi[idx] > (90) and aniAzi[idx] <= (270):
                    anglIncS = np.cos(aniAlt[idx] * deg2rad) * np.cos((180 - aniAzi[idx]) * deg2rad) * np.sin(
                        np.pi / 2) + np.sin(
                        aniAlt[idx] * deg2rad) * np.cos(np.pi / 2)
                    diffRadS = diffRadS + diffsh[:, :, idx] * lumChi[idx] * anglIncS * phiVar[idx] #* 0.5

                if aniAzi[idx] > (180) and aniAzi[idx] <= (360):
                    anglIncW = np.cos(aniAlt[idx] * deg2rad) * np.cos((270 - aniAzi[idx]) * deg2rad) * np.sin(
                        np.pi / 2) + np.sin(
                        aniAlt[idx] * deg2rad) * np.cos(np.pi / 2)
                    diffRadW = diffRadW + diffsh[:, :, idx] * lumChi[idx] * anglIncW * phiVar[idx] #* 0.5

                if aniAzi[idx] > (270) or aniAzi[idx] <= (90):
                    anglIncN = np.cos(aniAlt[idx] * deg2rad) * np.cos((0 - aniAzi[idx]) * deg2rad) * np.sin(
                        np.pi / 2) + np.sin(
                        aniAlt[idx] * deg2rad) * np.cos(np.pi / 2)
                    diffRadN = diffRadN + diffsh[:, :, idx] * lumChi[idx] * anglIncN * phiVar[idx] #* 0.5

            KeastDG = diffRadE + (albedo * (svfviktbuvegE * (radG * (1 - F_sh) + radD * F_sh)) + KupE) * 0.5
            Keast = KeastI + KeastDG

            KsouthDG = diffRadS + (albedo * (svfviktbuvegS * (radG * (1 - F_sh) + radD * F_sh)) + KupS) * 0.5
            Ksouth = KsouthI + KsouthDG

            KwestDG = diffRadW + (albedo * (svfviktbuvegW * (radG * (1 - F_sh) + radD * F_sh)) + KupW) * 0.5
            Kwest = KwestI + KwestDG

            KnorthDG = diffRadN + (albedo * (svfviktbuvegN * (radG * (1 - F_sh) + radD * F_sh)) + KupN) * 0.5
            Knorth = KnorthI + KnorthDG

    else:
        KeastDG = (radD * (1 - svfviktbuvegE) + albedo * (
        svfviktbuvegE * (radG * (1 - F_sh) + radD * F_sh)) + KupE) * 0.5
        Keast = KeastI + KeastDG

        KsouthDG = (radD * (1 - svfviktbuvegS) + albedo * (
        svfviktbuvegS * (radG * (1 - F_sh) + radD * F_sh)) + KupS) * 0.5
        Ksouth = KsouthI + KsouthDG

        KwestDG = (radD * (1 - svfviktbuvegW) + albedo * (
        svfviktbuvegW * (radG * (1 - F_sh) + radD * F_sh)) + KupW) * 0.5
        Kwest = KwestI + KwestDG

        KnorthDG = (radD * (1 - svfviktbuvegN) + albedo * (
        svfviktbuvegN * (radG * (1 - F_sh) + radD * F_sh)) + KupN) * 0.5
        Knorth = KnorthI + KnorthDG

    return Keast,Ksouth,Kwest,Knorth,KsideI,KsideD

def Kup_veg_2015a(radI,radD,radG,altitude,svfbuveg,albedo_b,F_sh,gvfalb,gvfalbE,gvfalbS,gvfalbW,gvfalbN,gvfalbnosh,gvfalbnoshE,gvfalbnoshS,gvfalbnoshW,gvfalbnoshN):

    Kup=(gvfalb*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnosh

    KupE=(gvfalbE*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshE

    KupS=(gvfalbS*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshS

    KupW=(gvfalbW*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshW

    KupN=(gvfalbN*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshN

    return Kup, KupE, KupS, KupW, KupN

def TsWaveDelay_2015a(gvfLup, firstdaytime, timeadd, timestepdec, Tgmap1):
    Tgmap0 = gvfLup  # current timestep
    if firstdaytime == 1:  # "first in morning"
        Tgmap1 = Tgmap0

    if timeadd >= (59/1440):  # more or equal to 59 min
        weight1 = np.exp(-33.27 * timeadd)  # surface temperature delay function - 1 step
        Tgmap1 = Tgmap0 * (1 - weight1) + Tgmap1 * weight1
        Lup = Tgmap1
        if timestepdec > (59/1440):
            timeadd = timestepdec
        else:
            timeadd = 0
    else:
        timeadd = timeadd + timestepdec
        weight1 = np.exp(-33.27 * timeadd)  # surface temperature delay function - 1 step
        Lup = (Tgmap0 * (1 - weight1) + Tgmap1 * weight1)

    return Lup, timeadd, Tgmap1

def sunonsurface_2018a(azimuthA, scale, buildings, shadow, sunwall, first, second, aspect, walls, Tg, Tgwall, Ta,
                       emis_grid, ewall, alb_grid, SBC, albedo_b, Twater, lc_grid, landcover):
    # This version of sunonsurfce goes with SOLWEIG 2015a. It also simulates
    # Lup and albedo based on landcover information and shadow patterns.
    # Fredrik Lindberg, fredrikl@gvc.gu.se
    
    sizex = np.shape(walls)[0]
    sizey = np.shape(walls)[1]

    # sizex=size(buildings,1);sizey=size(buildings,2);
    wallbol = (walls > 0) * 1
    sunwall[sunwall > 0] = 1  # test 20160910
    
    # conversion into radians
    azimuth = azimuthA * (np.pi / 180)

    # loop parameters
    index = 0
    f = buildings
    Lup = SBC * emis_grid * (Tg * shadow + Ta + 273.15) ** 4 - SBC * emis_grid * (Ta + 273.15) ** 4  # +Ta
    if landcover == 1:
        # Use mean(Ta) if it's an array, otherwise use it as-is
        Ta_val = np.mean(Ta) if not np.isscalar(Ta) else Ta
        Tg[lc_grid == 3] = Twater - Ta_val  # Setting water temperature

    Lwall = SBC * ewall * (Tgwall + Ta + 273.15) ** 4 - SBC * ewall * (Ta + 273.15) ** 4  # +Ta
    albshadow = alb_grid * shadow
    alb = alb_grid
    # sh(sh<=0.1)=0;
    # sh=sh-(1-vegsh)*(1-psi);
    # shadow=sh-(1-vegsh)*(1-psi);
    # dx=0;
    # dy=0;
    # ds=0; ##ok<NASGU>
    
    tempsh = np.zeros((sizex, sizey))
    tempbu = np.zeros((sizex, sizey))
    tempbub = np.zeros((sizex, sizey))
    tempbubwall = np.zeros((sizex, sizey))
    tempwallsun = np.zeros((sizex, sizey))
    weightsumsh = np.zeros((sizex, sizey))
    weightsumwall = np.zeros((sizex, sizey))
    first = np.round(first * scale)
    if first < 1:
        first = 1
    second = np.round(second * scale)
    # tempTgsh=tempsh;
    weightsumLupsh = np.zeros((sizex, sizey))
    weightsumLwall = np.zeros((sizex, sizey))
    weightsumalbsh = np.zeros((sizex, sizey))
    weightsumalbwall = np.zeros((sizex, sizey))
    weightsumalbnosh = np.zeros((sizex, sizey))
    weightsumalbwallnosh = np.zeros((sizex, sizey))
    tempLupsh = np.zeros((sizex, sizey))
    tempalbsh = np.zeros((sizex, sizey))
    tempalbnosh = np.zeros((sizex, sizey))

    # other loop parameters
    pibyfour = np.pi / 4
    threetimespibyfour = 3 * pibyfour
    fivetimespibyfour = 5 * pibyfour
    seventimespibyfour = 7 * pibyfour
    sinazimuth = np.sin(azimuth)
    cosazimuth = np.cos(azimuth)
    tanazimuth = np.tan(azimuth)
    signsinazimuth = np.sign(sinazimuth)
    signcosazimuth = np.sign(cosazimuth)

    ## The Shadow casting algoritm
    for n in np.arange(0, second):
        if (pibyfour <= azimuth and azimuth < threetimespibyfour) or (
                fivetimespibyfour <= azimuth and azimuth < seventimespibyfour):
            dy = signsinazimuth * index
            dx = -1 * signcosazimuth * np.abs(np.round(index / tanazimuth))
            # ds = dssin
        else:
            dy = signsinazimuth * abs(round(index * tanazimuth))
            dx = -1 * signcosazimuth * index
            # ds = dscos

        absdx = np.abs(dx)
        absdy = np.abs(dy)

        xc1 = ((dx + absdx) / 2)
        xc2 = (sizex + (dx - absdx) / 2)
        yc1 = ((dy + absdy) / 2)
        yc2 = (sizey + (dy - absdy) / 2)

        xp1 = -((dx - absdx) / 2)
        xp2 = (sizex - (dx + absdx) / 2)
        yp1 = -((dy - absdy) / 2)
        yp2 = (sizey - (dy + absdy) / 2)
        
        tempbu[int(xp1):int(xp2), int(yp1):int(yp2)] = buildings[int(xc1):int(xc2),
                                                       int(yc1):int(yc2)]  # moving building
        tempsh[int(xp1):int(xp2), int(yp1):int(yp2)] = shadow[int(xc1):int(xc2), int(yc1):int(yc2)]  # moving shadow
        tempLupsh[int(xp1):int(xp2), int(yp1):int(yp2)] = Lup[int(xc1):int(xc2), int(yc1):int(yc2)]  # moving Lup/shadow
        tempalbsh[int(xp1):int(xp2), int(yp1):int(yp2)] = albshadow[int(xc1):int(xc2),
                                                          int(yc1):int(yc2)]  # moving Albedo/shadow
        tempalbnosh[int(xp1):int(xp2), int(yp1):int(yp2)] = alb[int(xc1):int(xc2), int(yc1):int(yc2)]  # moving Albedo
        f = np.min([f, tempbu], axis=0)  # utsmetning av buildings

        shadow2 = tempsh * f
        weightsumsh = weightsumsh + shadow2

        Lupsh = tempLupsh * f
        weightsumLupsh = weightsumLupsh + Lupsh

        albsh = tempalbsh * f
        weightsumalbsh = weightsumalbsh + albsh

        albnosh = tempalbnosh * f
        weightsumalbnosh = weightsumalbnosh + albnosh

        tempwallsun[int(xp1):int(xp2), int(yp1):int(yp2)] = sunwall[int(xc1):int(xc2),
                                                            int(yc1):int(yc2)]  # moving buildingwall insun image
        tempb = tempwallsun * f
        tempbwall = f * -1 + 1
        tempbub = ((tempb + tempbub) > 0) * 1
        tempbubwall = ((tempbwall + tempbubwall) > 0) * 1
        weightsumLwall = weightsumLwall + tempbub * Lwall
        weightsumalbwall = weightsumalbwall + tempbub * albedo_b
        weightsumwall = weightsumwall + tempbub
        weightsumalbwallnosh = weightsumalbwallnosh + tempbubwall * albedo_b

        ind = 1
        if (n + 1) <= first:
            weightsumwall_first = weightsumwall / ind
            weightsumsh_first = weightsumsh / ind
            wallsuninfluence_first = weightsumwall_first > 0
            weightsumLwall_first = (weightsumLwall) / ind  # *Lwall
            weightsumLupsh_first = weightsumLupsh / ind

            weightsumalbwall_first = weightsumalbwall / ind  # *albedo_b
            weightsumalbsh_first = weightsumalbsh / ind
            weightsumalbwallnosh_first = weightsumalbwallnosh / ind  # *albedo_b
            weightsumalbnosh_first = weightsumalbnosh / ind
            wallinfluence_first = weightsumalbwallnosh_first > 0
            #         gvf1=(weightsumwall+weightsumsh)/first;
            #         gvf1(gvf1>1)=1;
            ind += 1
        index += 1

    wallsuninfluence_second = weightsumwall > 0
    wallinfluence_second = weightsumalbwallnosh > 0
    # gvf2(gvf2>1)=1;
    
    
    # Removing walls in shadow due to selfshadowing
    azilow = azimuth - np.pi / 2
    azihigh = azimuth + np.pi / 2
    if azilow >= 0 and azihigh < 2 * np.pi:  # 90 to 270  (SHADOW)
        facesh = (np.logical_or(aspect < azilow, aspect >= azihigh).astype(float) - wallbol + 1)
    elif azilow < 0 and azihigh <= 2 * np.pi:  # 0 to 90
        azilow = azilow + 2 * np.pi
        facesh = np.logical_or(aspect > azilow, aspect <= azihigh) * -1 + 1  # (SHADOW)    # check for the -1
    elif azilow > 0 and azihigh >= 2 * np.pi:  # 270 to 360
        azihigh = azihigh - 2 * np.pi
        facesh = np.logical_or(aspect > azilow, aspect <= azihigh) * -1 + 1  # (SHADOW)

    # removing walls in self shadoing
    keep = (weightsumwall == second) - facesh
    keep[keep == -1] = 0

    # gvf from shadow only
    gvf1 = ((weightsumwall_first + weightsumsh_first) / (first + 1)) * wallsuninfluence_first + \
           (weightsumsh_first) / (first) * (wallsuninfluence_first * -1 + 1)
    weightsumwall[keep == 1] = 0
    gvf2 = ((weightsumwall + weightsumsh) / (second + 1)) * wallsuninfluence_second + \
           (weightsumsh) / (second) * (wallsuninfluence_second * -1 + 1)

    gvf2[gvf2 > 1.] = 1.

    # gvf from shadow and Lup
    gvfLup1 = ((weightsumLwall_first + weightsumLupsh_first) / (first + 1)) * wallsuninfluence_first + \
              (weightsumLupsh_first) / (first) * (wallsuninfluence_first * -1 + 1)
    weightsumLwall[keep == 1] = 0
    gvfLup2 = ((weightsumLwall + weightsumLupsh) / (second + 1)) * wallsuninfluence_second + \
              (weightsumLupsh) / (second) * (wallsuninfluence_second * -1 + 1)

    # gvf from shadow and albedo
    gvfalb1 = ((weightsumalbwall_first + weightsumalbsh_first) / (first + 1)) * wallsuninfluence_first + \
              (weightsumalbsh_first) / (first) * (wallsuninfluence_first * -1 + 1)
    weightsumalbwall[keep == 1] = 0
    gvfalb2 = ((weightsumalbwall + weightsumalbsh) / (second + 1)) * wallsuninfluence_second + \
              (weightsumalbsh) / (second) * (wallsuninfluence_second * -1 + 1)

    # gvf from albedo only
    gvfalbnosh1 = ((weightsumalbwallnosh_first + weightsumalbnosh_first) / (first + 1)) * wallinfluence_first + \
                  (weightsumalbnosh_first) / (first) * (wallinfluence_first * -1 + 1)  #
    gvfalbnosh2 = ((weightsumalbwallnosh + weightsumalbnosh) / (second)) * wallinfluence_second + \
                  (weightsumalbnosh) / (second) * (wallinfluence_second * -1 + 1)

    # Weighting
    gvf = (gvf1 * 0.5 + gvf2 * 0.4) / 0.9
    gvfLup = (gvfLup1 * 0.5 + gvfLup2 * 0.4) / 0.9
    gvfLup = gvfLup + ((SBC * emis_grid * (Tg * shadow + Ta + 273.15) ** 4) - SBC * emis_grid * (Ta + 273.15) ** 4) * (
                buildings * -1 + 1)  # +Ta
    gvfalb = (gvfalb1 * 0.5 + gvfalb2 * 0.4) / 0.9
    gvfalb = gvfalb + alb_grid * (buildings * -1 + 1) * shadow
    gvfalbnosh = (gvfalbnosh1 * 0.5 + gvfalbnosh2 * 0.4) / 0.9
    gvfalbnosh = gvfalbnosh * buildings + alb_grid * (buildings * -1 + 1)

    return gvf, gvfLup, gvfalb, gvfalbnosh, gvf2

def diffusefraction(radG,altitude,Kt,Ta,RH):
    """
    This function estimates diffuse and directbeam radiation according to
    Reindl et al (1990), Solar Energy 45:1

    :param radG:
    :param altitude:
    :param Kt:
    :param Ta:
    :param RH:
    :return:
    """

    alfa = altitude*(np.pi/180)

    ## update the script, make sure it works even Ta and RH are np array or scalar
    # if Ta <= -999.00 or RH <= -999.00 or np.isnan(Ta) or np.isnan(RH):
    if np.any(Ta <= -999.0) or np.any(RH <= -999.0) or np.any(np.isnan(Ta)) or np.any(np.isnan(RH)):
        if Kt <= 0.3:
            radD = radG*(1.020-0.248*Kt)
        elif Kt > 0.3 and Kt < 0.78:
            radD = radG*(1.45-1.67*Kt)
        else:
            radD = radG*0.147
    else:
        RH = RH/100
        if Kt <= 0.3:
            radD = radG*(1 - 0.232 * Kt + 0.0239 * np.sin(alfa) - 0.000682 * Ta + 0.0195 * RH)
        elif Kt > 0.3 and Kt < 0.78:
            radD = radG*(1.329- 1.716 * Kt + 0.267 * np.sin(alfa) - 0.00357 * Ta + 0.106 * RH)
        else:
            radD = radG*(0.426 * Kt - 0.256 * np.sin(alfa) + 0.00349 * Ta + 0.0734 * RH)

    radI = (radG - radD)/(np.sin(alfa))


    ## Corrections for low sun altitudes (20130307)
    # if radI < 0:
    #     radI = 0
    # if altitude < 1 and radI > radG:
    #     radI=radG
    # if radD > radG:
    #     radD = radG

    ## update the scripts and make sure they are fine for both array and scalar
    def ensure_array(x):
        return np.asarray(x), np.isscalar(x)

    # Convert inputs and record if they were scalar
    radI, is_scalar_I = ensure_array(radI)
    radG, is_scalar_G = ensure_array(radG)
    radD, is_scalar_D = ensure_array(radD)
    altitude, is_scalar_alt = ensure_array(altitude)

    # Apply logic with vectorized operations
    radI = np.where(radI < 0, 0, radI)

    cond2 = (altitude < 1) & (radI > radG)
    radI = np.where(cond2, radG, radI)

    radD = np.where(radD > radG, radG, radD)

    # Convert back to scalar if original input was scalar
    if is_scalar_I:
        radI = radI.item()
    if is_scalar_D:
        radD = radD.item()

    return radI, radD


def shadowingfunction_wallheight_23(a, vegdem, vegdem2, azimuth, altitude, scale, amaxvalue, bush, walls, aspect):
    """
    This function calculates shadows on a DSM and shadow height on building
    walls including both buildings and vegetion units.
    
    INPUTS:
    a = DSM
    vegdem = Vegetation canopy DSM (magl)
    vegdem2 = Trunkzone DSM (magl)
    azimuth and altitude = sun position
    scale= scale of DSM (1 meter pixels=1, 2 meter pixels=0.5)
    walls= pixel row 'outside' buildings. will be calculated if empty
    aspect=normal aspect of walls
    
    OUTPUT:
    sh=ground and roof shadow

    wallsh = height of wall that is in shadow
    wallsun = hieght of wall that is in sun

    original Matlab code:
    Fredrik Lindberg 2013-08-14
    fredrikl@gvc.gu.se

    :param a:
    :param vegdem:
    :param vegdem2:
    :param azimuth:
    :param altitude:
    :param scale:
    :param amaxvalue:
    :param bush:
    :param walls:
    :param aspect:
    :return:
    """

    if not walls.size:
        """ needs to be checked
        walls=ordfilt2(a,4,[0 1 0; 1 0 1; 0 1 0]);
        walls=walls-a;
        walls(walls<3)=0;
        sizex=size(a,1);%might be wrong
        sizey=size(a,2);
        dirwalls = filter1Goodwin_as_aspect_v3(walls,sizex,sizey,scale,a);
        aspect=dirwalls*pi/180;
        """

    # conversion
    degrees = np.pi/180
    azimuth *= degrees
    altitude *= degrees
    
    # measure the size of the image

    sizex = np.shape(a)[0]
    sizey = np.shape(a)[1]
    
    # initialise parameters
    dx = 0
    dy = 0
    dz = 0
    
    sh = np.zeros((sizex, sizey))
    vbshvegsh = np.copy(sh)
    vegsh = np.copy(sh)
    f = np.copy(a)
    shvoveg = np.copy(vegdem)    # for vegetation shadowvolume
    g = np.copy(sh)
    bushplant = bush > 1
    #wallbol = np.array([np.float(boolean) for row in walls > 0 for boolean in row])
    # wallbol = np.copy(sh)
    # wallbol[walls > 0] = 1.
    wallbol = (walls > 0).astype(float)
    wallbol[wallbol == 0] = np.nan

    pibyfour = np.pi/4
    threetimespibyfour = 3*pibyfour
    fivetimespibyfour = 5*pibyfour
    seventimespibyfour = 7*pibyfour
    sinazimuth = np.sin(azimuth)
    cosazimuth = np.cos(azimuth)
    tanazimuth = np.tan(azimuth)
    signsinazimuth = np.sign(sinazimuth)
    signcosazimuth = np.sign(cosazimuth)
    dssin = np.abs(1/sinazimuth)
    dscos = np.abs(1/cosazimuth)
    tanaltitudebyscale = np.tan(altitude)/scale

    tempvegdem = np.zeros((sizex, sizey))
    tempvegdem2 = np.zeros((sizex, sizey))
    temp = np.zeros((sizex, sizey))

    index = 0

    # main loop
    while (amaxvalue>=dz) and (np.abs(dx)<sizex) and (np.abs(dy)<sizey):
        if ((pibyfour <= azimuth) and (azimuth < threetimespibyfour)) or \
                ((fivetimespibyfour <= azimuth) and (azimuth < seventimespibyfour)):
            dy = signsinazimuth*(index+1)
            dx = -1*signcosazimuth*np.abs(np.round((index+1)/tanazimuth))
            ds = dssin
        else:
            dy = signsinazimuth*np.abs(np.round((index+1)*tanazimuth))
            dx = -1*signcosazimuth*(index+1)
            ds = dscos

        # note: dx and dy represent absolute values while ds is an incremental value
        dz = ds*(index+1)*tanaltitudebyscale
        tempvegdem[0:sizex, 0:sizey] = 0
        tempvegdem2[0:sizex, 0:sizey] = 0
        temp[0:sizex, 0:sizey] = 0
    
        absdx = np.abs(dx)
        absdy = np.abs(dy)

        xc1 = int((dx+absdx)/2)
        xc2 = int(sizex+(dx-absdx)/2)
        yc1 = int((dy+absdy)/2)
        yc2 = int(sizey+(dy-absdy)/2)

        xp1 = -int((dx-absdx)/2)
        xp2 = int(sizex-(dx+absdx)/2)
        yp1 = -int((dy-absdy)/2)
        yp2 = int(sizey-(dy+absdy)/2)

        tempvegdem[int(xp1):int(xp2), int(yp1):int(yp2)] = vegdem[int(xc1):int(xc2), int(yc1):int(yc2)] - dz
        tempvegdem2[int(xp1):int(xp2), int(yp1):int(yp2)] = vegdem2[int(xc1):int(xc2), int(yc1):int(yc2)] - dz
        temp[int(xp1):int(xp2), int(yp1):int(yp2)] = a[int(xc1):int(xc2), int(yc1):int(yc2)] - dz

        f = np.max([f, temp], axis=0)
        #f = np.array([np.max(val) for val in zip(f, temp)])
        shvoveg = np.max([shvoveg, tempvegdem], axis=0)
        sh[f > a] = 1
        sh[f <= a] = 0   #Moving building shadow
        fabovea = (tempvegdem > a).astype(int)   #vegdem above DEM
        gabovea = (tempvegdem2 > a).astype(int)   #vegdem2 above DEM
        vegsh2 = fabovea - gabovea
        vegsh = np.max([vegsh, vegsh2], axis=0)
        vegsh[vegsh*sh > 0] = 0    # removing shadows 'behind' buildings
        vbshvegsh = np.copy(vegsh) + vbshvegsh

        # vegsh at high sun altitudes
        if index == 0:
            firstvegdem = np.copy(tempvegdem) - np.copy(temp)
            firstvegdem[firstvegdem <= 0] = 1000
            vegsh[firstvegdem < dz] = 1
            vegsh *= (vegdem2 > a)
            vbshvegsh = np.zeros((sizex, sizey))

        # Bush shadow on bush plant
        if np.max(bush) > 0 and np.max(fabovea*bush) > 0:
            tempbush = np.zeros((sizex, sizey))
            tempbush[int(xp1):int(xp2), int(yp1):int(yp2)] = bush[int(xc1):int(xc2), int(yc1):int(yc2)] - dz
            g = np.max([g, tempbush], axis=0)
            g = bushplant * g
    
        #     if index<3 #removing shadowed walls 1
        #         tempfirst(1:sizex,1:sizey)=0;
        #         tempfirst(xp1:xp2,yp1:yp2)= a(xc1:xc2,yc1:yc2);
        #         if index==1 # removing shadowed walls 2
        #             tempwalls(1:sizex,1:sizey)=0;
        #             tempwalls(xp1:xp2,yp1:yp2)= wallbol(xc1:xc2,yc1:yc2);
        #             wallfirst=((tempwalls+wallbol).*wallbol)==2;
        #             wallfirstaspect=aspect.*wallbol.*wallfirst;
        #             wallfirstaspect(wallfirstaspect==0)=NaN;
        #             wallfirstsun=(wallfirstaspect>azimuth-pi/2 & wallfirstaspect<azimuth+pi/2);
        #             wallfirstshade=wallfirst-wallfirstsun;
        #         end
        #     end
    
        index += 1
        #     imagesc(h),axis image,colorbar
        # Stopping loop if all shadows reached the ground
        #     stopbuild=stopbuild==f;
        #      imagesc(stopbuild),axis image,pause(0.3)
        #     fin=find(stopbuild==0, 1);
        #     stopbuild=f;
        #     stopveg=stopveg==vegsh;
        #     finveg=find(stopveg==0, 1);
        #     stopveg=vegsh;
        #     if isempty(fin) && isempty(finveg)
        #         dz=amaxvalue+9999;
        #     end

    # Removing walls in shadow due to selfshadowing
    azilow = azimuth - np.pi/2
    azihigh = azimuth + np.pi/2
    if azilow >= 0 and azihigh < 2*np.pi:    # 90 to 270  (SHADOW)
        facesh = np.logical_or(aspect < azilow, aspect >= azihigh).astype(float) - wallbol + 1    # TODO check
    elif azilow < 0 and azihigh <= 2*np.pi:    # 0 to 90
        azilow = azilow + 2*np.pi
        facesh = np.logical_or(aspect > azilow, aspect <= azihigh) * -1 + 1    # (SHADOW)
    elif azilow > 0 and azihigh >= 2*np.pi:    # 270 to 360
        azihigh -= 2 * np.pi
        facesh = np.logical_or(aspect > azilow, aspect <= azihigh)*-1 + 1    # (SHADOW)

    sh = 1-sh
    vbshvegsh[vbshvegsh > 0] = 1
    vbshvegsh = vbshvegsh-vegsh
    
    if np.max(bush) > 0:
        g = g-bush
        g[g > 0] = 1
        g[g < 0] = 0
        vegsh = vegsh-bushplant+g
        vegsh[vegsh < 0] = 0

    vegsh[vegsh > 0] = 1
    shvoveg = (shvoveg-a) * vegsh    #Vegetation shadow volume
    vegsh = 1-vegsh
    vbshvegsh = 1-vbshvegsh

    #removing walls in shadow
    # tempfirst=tempfirst-a;
    # tempfirst(tempfirst<2)=1;
    # tempfirst(tempfirst>=2)=0;
    
    shvo = f - a   # building shadow volume

    facesun = np.logical_and(facesh + (walls > 0).astype(float) == 1, walls > 0).astype(float)
    #facesun = np.reshape(np.array([np.float(boolean) for row in facesun for boolean in row]), facesun.shape)

    wallsun = np.copy(walls-shvo)
    wallsun[wallsun < 0] = 0
    wallsun[facesh == 1] = 0    # Removing walls in "self"-shadow
    # wallsun(tempfirst =  = 0) = 0# Removing walls in shadow 1
    # wallsun(wallfirstshade =  = 1) = 0# Removing walls in shadow 2
    wallsh = np.copy(walls-wallsun)
    # wallsh(wallfirstshade =  = 1) = 0
    # wallsh = wallsh+(wallfirstshade.*walls)
    #wallbol = np.reshape(np.array([np.float(boolean) for row in walls > 0 for boolean in row]), walls.shape)
    wallbol = (walls > 0).astype(float)

    wallshve = shvoveg * wallbol
    wallshve = wallshve - wallsh
    wallshve[wallshve < 0] = 0
    id = np.where(wallshve > walls)
    wallshve[id] = walls[id]
    wallsun = wallsun-wallshve    # problem with wallshve only
    id = np.where(wallsun < 0)
    wallshve[id] = 0
    wallsun[id] = 0
    
    # subplot(2,2,1),imagesc(facesh),axis image ,colorbar,title('facesh')#
    # subplot(2,2,2),imagesc(wallsun,[0 20]),axis image, colorbar,title('Wallsun')#
    # subplot(2,2,3),imagesc(sh-vegsh*0.8), colorbar,axis image,title('Groundsh')#
    # subplot(2, 2, 4), imagesc(wallshve, [0 20]), axis image,  colorbar, title('Wallshve')#
    return vegsh, sh, vbshvegsh, wallsh, wallsun, wallshve, facesh, facesun


def shadowingfunction_wallheight_13(a, azimuth, altitude, scale, walls, aspect):
    """
    This m.file calculates shadows on a DSM and shadow height on building
    walls.
    
    INPUTS:
    a = DSM
    azimuth and altitude = sun position
    scale= scale of DSM (1 meter pixels=1, 2 meter pixels=0.5)
    walls= pixel row 'outside' buildings. will be calculated if empty
    aspect = normal aspect of buildings walls
    
    OUTPUT:
    sh=ground and roof shadow
    wallsh = height of wall that is in shadow
    wallsun = hieght of wall that is in sun
    
    Fredrik Lindberg 2012-03-19
    fredrikl@gvc.gu.se
    
     Utdate 2013-03-13 - bugfix for walls alinged with sun azimuths

    :param a:
    :param azimuth:
    :param altitude:
    :param scale:
    :param walls:
    :param aspect:
    :return:
    """

    if not walls.size:
        """
        walls = ordfilt2(a,4,[0 1 0; 1 0 1; 0 1 0])
        walls = walls-a
        walls[walls < 3]=0
        sizex = np.shape(a)[0]    #might be wrong
        sizey = np.shape(a)[1]
        dirwalls = filter1Goodwin_as_aspect_v3(walls,sizex,sizey,scale,a);
        aspect = dirwalls*np.pi/180
        """

    # conversion
    degrees = np.pi/180
    azimuth = math.radians(azimuth)
    altitude = math.radians(altitude)

    # measure the size of the image

    sizex = np.shape(a)[0]
    sizey = np.shape(a)[1]

    # initialise parameters
    f = np.copy(a)
    dx = 0
    dy = 0
    dz = 0
    temp = np.zeros((sizex, sizey))
    index = 1
    wallbol = (walls > 0).astype(float)
    #wallbol[wallbol == 0] = np.nan
    # np.savetxt("wallbol.txt",wallbol)
    # other loop parameters
    amaxvalue = np.max(a)
    pibyfour = np.pi/4
    threetimespibyfour = 3 * pibyfour
    fivetimespibyfour = 5 * pibyfour
    seventimespibyfour = 7 * pibyfour
    sinazimuth = np.sin(azimuth)
    cosazimuth = np.cos(azimuth)
    tanazimuth = np.tan(azimuth)
    signsinazimuth = np.sign(sinazimuth)
    signcosazimuth = np.sign(cosazimuth)
    dssin = np.abs(1/sinazimuth)
    dscos = np.abs(1/cosazimuth)
    tanaltitudebyscale = np.tan(altitude)/scale

    # main loop
    while (amaxvalue >= dz) and (np.abs(dx) < sizex) and (np.abs(dy) < sizey):

        if (pibyfour <= azimuth and azimuth < threetimespibyfour) or \
                (fivetimespibyfour <= azimuth and azimuth < seventimespibyfour):
            dy = signsinazimuth*index
            dx = -1*signcosazimuth*np.abs(np.round(index/tanazimuth))
            ds = dssin
        else:
            dy = signsinazimuth*abs(round(index*tanazimuth))
            dx = -1*signcosazimuth*index
            ds = dscos

        # note: dx and dy represent absolute values while ds is an incremental value
        dz = ds*index*tanaltitudebyscale
        temp[0:sizex, 0:sizey] = 0

        absdx = np.abs(dx)
        absdy = np.abs(dy)

        xc1 = ((dx+absdx)/2)
        xc2 = (sizex+(dx-absdx)/2)
        yc1 = ((dy+absdy)/2)
        yc2 = (sizey+(dy-absdy)/2)

        xp1 = -((dx-absdx)/2)
        xp2 = (sizex-(dx+absdx)/2)
        yp1 = -((dy-absdy)/2)
        yp2 = (sizey-(dy+absdy)/2)

        temp[int(xp1):int(xp2), int(yp1):int(yp2)] = a[int(xc1):int(xc2), int(yc1):int(yc2)] - dz

        f = np.max([f, temp], axis=0)

        index = index+1

    # Removing walls in shadow due to selfshadowing
    azilow = azimuth-np.pi/2
    azihigh = azimuth+np.pi/2

    if azilow >= 0 and azihigh < 2*np.pi:    # 90 to 270  (SHADOW)
        facesh = (np.logical_or(aspect < azilow, aspect >= azihigh).astype(float)-wallbol+1)
    elif azilow < 0 and azihigh <= 2*np.pi:    # 0 to 90
        azilow = azilow + 2*np.pi
        facesh = np.logical_or(aspect > azilow, aspect <= azihigh) * -1 + 1    # (SHADOW)    # check for the -1
    elif azilow > 0 and azihigh >= 2*np.pi:    # 270 to 360
        azihigh = azihigh-2*np.pi
        facesh = np.logical_or(aspect > azilow, aspect <= azihigh)*-1 + 1    # (SHADOW)

    sh = np.copy(f-a)    # shadow volume
    facesun = np.logical_and(facesh + (walls > 0).astype(float) == 1, walls > 0).astype(float)
    wallsun = np.copy(walls-sh)
    wallsun[wallsun < 0] = 0
    wallsun[facesh == 1] = 0    # Removing walls in "self"-shadow
    wallsh = np.copy(walls-wallsun)

    sh = np.logical_not(np.logical_not(sh)).astype(float)
    sh = sh*-1 + 1

    # subplot(2,2,1),imagesc(facesh),axis image ,colorbar,title('facesh')#
    # subplot(2,2,2),imagesc(wallsh,[0 20]),axis image, colorbar,title('Wallsh')#
    # subplot(2,2,3),imagesc(sh), colorbar,axis image,title('Groundsh')#
    # subplot(2,2,4),imagesc(wallsun,[0 20]),axis image, colorbar,title('Wallsun')#

    ## old stuff
    #     if index==0 #removing shadowed walls at first iteration
    #         tempfirst(1:sizex,1:sizey)=0;
    #         tempfirst(xp1:xp2,yp1:yp2)= a(xc1:xc2,yc1:yc2);
    #         tempfirst=tempfirst-a;
    #         tempfirst(tempfirst<2)=1;# 2 is smallest wall height. Should be variable
    #         tempfirst(tempfirst>=2)=0;# walls in shadow at first movment (iteration)
    #         tempwalls(1:sizex,1:sizey)=0;
    #         tempwalls(xp1:xp2,yp1:yp2)= wallbol(xc1:xc2,yc1:yc2);
    #         wallfirst=tempwalls.*wallbol;#wallpixels potentially shaded by adjacent wall pixels
    #         wallfirstaspect=aspect.*wallfirst;
    #         azinormal=azimuth-pi/2;
    #         if azinormal<=0,azinormal=azinormal+2*pi;end
    #         facesh=double(wallfirstaspect<azinormal|tempfirst<=0);
    #         facesun=double((facesh+double(walls>0))==1);
    #     end


    # Removing walls in shadow due to selfshadowing (This only works on
    # regular arrays)
    #     if dy~=0 && firsty==0
    #         if yp1>1
    #             yp1f=2;yp2f=sizey;
    #             yc1f=1;yc2f=sizey-1;
    #         else
    #             yp1f=1;yp2f=sizey-1;
    #             yc1f=2;yc2f=sizey;
    #         end
    #         firsty=1;
    #     end
    #     if dx~=0 && firstx==0
    #         if xp1>1
    #             xp1f=2;xp2f=sizex;
    #             xc1f=1;xc2f=sizex-1;
    #         else
    #             xp1f=1;xp2f=sizex-1;
    #             xc1f=2;xc2f=sizex;
    #         end
    #         firstx=1;
    #     end
    #     if firsty==1 && firstx==1
    #         facesh(xp1f:xp2f,yp1f:yp2f)= a(xc1f:xc2f,yc1f:yc2f);
    #         facesh=facesh-a;
    #         facesh(facesh<=0)=0;
    #         facesh(facesh>0)=1;
    #     end


    #     if index<3 #removing shadowed walls 1
    #         tempfirst(1:sizex,1:sizey)=0;
    #         tempfirst(xp1:xp2,yp1:yp2)= a(xc1:xc2,yc1:yc2);
    #         #removing walls in shadow
    #         tempfirst=tempfirst-a;
    #         tempfirst(tempfirst<2)=1;# 2 is smallest wall height. Should be variable
    #         tempfirst(tempfirst>=2)=0;
    #         if index==1 # removing shadowed walls 2
    #             tempwalls(1:sizex,1:sizey)=0;
    #             tempwalls(xp1:xp2,yp1:yp2)= wallbol(xc1:xc2,yc1:yc2);
    #             #             wallfirst=((tempwalls+wallbol).*wallbol)==2;
    #             wallfirst=tempwalls.*wallbol;
    #             wallfirstaspect=aspect.*wallfirst;#.*wallbol
    #             #             wallfirstaspect(wallfirstaspect==0)=NaN;
    #             wallfirstsun=wallfirstaspect>(azimuth-pi/2);
    #             #             wallfirstsun=(wallfirstaspect>=azimuth-pi/2 & wallfirstaspect<=azimuth+pi/2);###H�R�RJAG
    #             wallfirstshade=wallfirst-wallfirstsun;
    #         end
    #     end
    return sh, wallsh, wallsun, facesh, facesun

## this is the updated cpu version script to calculate the Tmrt, the input is going to be the
## raster met data, like air temperature, humidity
def Solweig_2019a_calc(i, dsm, scale, rows, cols, svf, svfN, svfW, svfE, svfS, svfveg, svfNveg, svfEveg, svfSveg,
                       svfWveg, svfaveg, svfEaveg, svfSaveg, svfWaveg, svfNaveg, vegdem, vegdem2, albedo_b, absK, absL,
                       ewall, Fside, Fup, Fcyl, altitude, azimuth, zen, jday, usevegdem, onlyglobal, buildings, location, psi,
                       landcover, lc_grid, dectime, altmax, dirwalls, walls, cyl, elvis, Ta, RH, radG, radD, radI, P,
                       amaxvalue, bush, Twater, TgK, Tstart, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST,
                       TmaxLST_wall, first, second, svfalfa, svfbuveg, firstdaytime, timeadd, timeaddE, timeaddS,
                       timeaddW, timeaddN, timestepdec, Tgmap1, Tgmap1E, Tgmap1S, Tgmap1W, Tgmap1N, CI, TgOut1, diffsh, ani):
    
    # This is the core function of the SOLWEIG model
    # 2016-Aug-28
    # Fredrik Lindberg, fredrikl@gvc.gu.se
    # Goteborg Urban Climate Group
    # Gothenburg University
    # 
    ## update by Xiaojiang Li, UPenn, now it is able to take both scalar and 
    ## np array input of air temperature, June 6, 2025

    # Input variables:
    # dsm = digital surface model
    # scale = height to pixel size (2m pixel gives scale = 0.5)
    # header = ESRI Ascii Grid header
    # sizey,sizex = no. of pixels in x and y
    # svf,svfN,svfW,svfE,svfS = SVFs for building and ground
    # svfveg,svfNveg,svfEveg,svfSveg,svfWveg = Veg SVFs blocking sky
    # svfaveg,svfEaveg,svfSaveg,svfWaveg,svfNaveg = Veg SVFs blocking buildings
    # vegdem = Vegetation canopy DSM
    # vegdem2 = Vegetation trunk zone DSM
    # albedo_b = buildings
    # absK = human absorption coefficient for shortwave radiation
    # absL = human absorption coefficient for longwave radiation
    # ewall = Emissivity of building walls
    # Fside = The angular factors between a person and the surrounding surfaces
    # Fup = The angular factors between a person and the surrounding surfaces
    # altitude = Sun altitude (degree)
    # azimuth = Sun azimuth (degree)
    # zen = Sun zenith angle (radians)
    # jday = day of year
    # usevegdem = use vegetation scheme
    # onlyglobal = calculate dir and diff from global
    # buildings = Boolena grid to identify building pixels
    # location = geographic location
    # height = height of measurements point
    # psi = 1 - Transmissivity of shortwave through vegetation
    # output = output settings
    # fileformat = fileformat of output grids
    # landcover = use landcover scheme !!!NEW IN 2015a!!!
    # sensorheight = Sensorheight of wind sensor
    # lc_grid = grid with landcoverclasses
    # lc_class = table with landcover properties
    # dectime = decimal time
    # altmax = maximum sun altitude
    # dirwalls = aspect of walls
    # walls = one pixel row outside building footprints
    # cyl = consider man as cylinder instead of cude

    # # # Core program start # # #
    # for i in np.arange(1., (length(altitude))+1): ## loop move outside

    # Instrument offset in degrees
    t = 0.

    # Stefan Bolzmans Constant
    SBC = 5.67051e-8

    # Find sunrise decimal hour - new from 2014a
    _, _, _, SNUP = daylen(jday, location['latitude'])

    # Vapor pressure, update make it handle the Ta as scalar and np array
    Ta_val = np.mean(Ta) if not np.isscalar(Ta) else Ta
    ea = 6.107 * 10 ** ((7.5 * Ta_val) / (237.3 + Ta_val)) * (RH / 100.)

    # Determination of clear - sky emissivity from Prata (1996)
    msteg = 46.5 * (ea / (Ta_val + 273.15))
    esky = (1 - (1 + msteg) * np.exp(-((1.2 + 3.0 * msteg) ** 0.5))) + elvis  # -0.04 old error from Jonsson et al.2006

    if altitude > 0: # # # # # # DAYTIME # # # # # #
        # Clearness Index on Earth's surface after Crawford and Dunchon (1999) with a correction
        #  factor for low sun elevations after Lindberg et al.(2008)
        I0, CI, Kt, I0et, CIuncorr = clearnessindex_2013b(zen, jday, Ta, RH / 100., radG, location, P)
        if (CI > 1) or (CI == np.inf):
            CI = 1
            
        # Estimation of radD and radI if not measured after Reindl et al.(1990)
        if onlyglobal == 1:
            I0, CI, Kt, I0et, CIuncorr = clearnessindex_2013b(zen, jday, Ta, RH / 100., radG, location, P)
            if (CI > 1) or (CI == np.inf):
                CI = 1

            radI, radD = diffusefraction(radG, altitude, Kt, Ta, RH)

        # Diffuse Radiation
        # Anisotropic Diffuse Radiation after Perez et al. 1993
        if ani == 1:
            patchchoice = 1
            zenDeg = zen*(180/np.pi)
            # TODO (WRI) Where is the Parez_V3 function? Do we every need to specify ani ==1?
            # lv = Perez_v3(zenDeg, azimuth, radD, radI, jday, patchchoice)   # Relative luminance
            lv = 0 # TODO (WRI) This values is zerod out for testing since function is not available
            
            deg2rad = np.pi/180

            aniLum = np.zeros((rows, cols))
            for idx in range(0,145):
                aniLum = aniLum + diffsh[:,:,idx] * lv[0][idx][2]     # Total relative luminance from sky into each cell

            dRad = aniLum * radD   # Total diffuse radiation from sky into each cell

        else:
            dRad = radD * svfbuveg
            lv = 0

        # Shadow  images
        if usevegdem == 1:
            vegsh, sh, _, wallsh, wallsun, wallshve, _, facesun = shadowingfunction_wallheight_23(dsm, vegdem, vegdem2,
                                        azimuth, altitude, scale, amaxvalue, bush, walls, dirwalls * np.pi / 180.)
            shadow = sh - (1 - vegsh) * (1 - psi)
        else:
            sh, wallsh, wallsun, facesh, facesun = shadowingfunction_wallheight_13(dsm, azimuth, altitude, scale,
                                                                                   walls, dirwalls * np.pi / 180.)
            shadow = sh

        # # # Surface temperature parameterisation during daytime # # # #
        # new using max sun alt.instead of  dfm
        Tgamp = (TgK * altmax - Tstart) + Tstart
        Tgampwall = (TgK_wall * altmax - (Tstart_wall)) + (Tstart_wall)
        Tg = Tgamp * np.sin((((dectime - np.floor(dectime)) - SNUP / 24) / (TmaxLST / 24 - SNUP / 24)) * np.pi / 2) + Tstart # 2015 a, based on max sun altitude
        Tgwall = Tgampwall * np.sin((((dectime - np.floor(dectime)) - SNUP / 24) / (TmaxLST_wall / 24 - SNUP / 24)) * np.pi / 2) + (Tstart_wall) # 2015a, based on max sun altitude

        if Tgwall < 0:  # temporary for removing low Tg during morning 20130205
            # Tg = 0
            Tgwall = 0

        # New estimation of Tg reduction for non - clear situation based on Reindl et al.1990
        radI0, _ = diffusefraction(I0, altitude, 1., Ta, RH)
        corr = 0.1473 * np.log(90 - (zen / np.pi * 180)) + 0.3454  # 20070329 correction of lat, Lindberg et al. 2008
        CI_Tg = (radI / radI0) + (1 - corr)

        #### here we update the script to make sure it can handle both scalar and np array
        # if (CI_Tg > 1) or (CI_Tg == np.inf):
        #     CI_Tg = 1
        CI_Tg = np.asarray(CI_Tg, dtype=float)
        # Clip values greater than 1 or infinite
        CI_Tg = np.where((CI_Tg > 1) | (CI_Tg == np.inf), 1.0, CI_Tg)
        # Optional: convert back to scalar if the input was scalar
        if np.isscalar(CI_Tg) or np.ndim(CI_Tg) == 0:
            CI_Tg = CI_Tg.item()

        Tg = Tg * CI_Tg  # new estimation
        Tgwall = Tgwall * CI_Tg
        if landcover == 1:
            Tg[Tg < 0] = 0  # temporary for removing low Tg during morning 20130205

        # Tw = Tg

        # # # # Ground View Factors # # # #
        gvfLup, gvfalb, gvfalbnosh, gvfLupE, gvfalbE, gvfalbnoshE, gvfLupS, gvfalbS, gvfalbnoshS, gvfLupW, gvfalbW,\
        gvfalbnoshW, gvfLupN, gvfalbN, gvfalbnoshN, gvfSum, gvfNorm = gvf_2018a(wallsun, walls, buildings, scale, shadow, first,
                second, dirwalls, Tg, Tgwall, Ta, emis_grid, ewall, alb_grid, SBC, albedo_b, rows, cols,
                                                                 Twater, lc_grid, landcover)

        # # # # Lup, daytime # # # #
        # Surface temperature wave delay - new as from 2014a
        Lup, timeadd, Tgmap1 = TsWaveDelay_2015a(gvfLup, firstdaytime, timeadd, timestepdec, Tgmap1)
        LupE, timeaddE, Tgmap1E = TsWaveDelay_2015a(gvfLupE, firstdaytime, timeaddE, timestepdec, Tgmap1E)
        LupS, timeaddS, Tgmap1S = TsWaveDelay_2015a(gvfLupS, firstdaytime, timeaddS, timestepdec, Tgmap1S)
        LupW, timeaddW, Tgmap1W = TsWaveDelay_2015a(gvfLupW, firstdaytime, timeaddW, timestepdec, Tgmap1W)
        LupN, timeaddN, Tgmap1N = TsWaveDelay_2015a(gvfLupN, firstdaytime, timeaddN, timestepdec, Tgmap1N)

        # # For Tg output in POIs
        TgTemp = Tg * shadow + Ta
        TgOut, timeadd, TgOut1 = TsWaveDelay_2015a(TgTemp, firstdaytime, timeadd, timestepdec, TgOut1)

        # Building height angle from svf
        F_sh = cylindric_wedge(zen, svfalfa, rows, cols)  # Fraction shadow on building walls based on sun alt and svf
        F_sh[np.isnan(F_sh)] = 0.5

        # # # # # # # Calculation of shortwave daytime radiative fluxes # # # # # # #
        Kdown = radI * shadow * np.sin(altitude * (np.pi / 180)) + dRad + albedo_b * (1 - svfbuveg) * \
                            (radG * (1 - F_sh) + radD * F_sh) # *sin(altitude(i) * (pi / 180))

        #Kdown = radI * shadow * np.sin(altitude * (np.pi / 180)) + radD * svfbuveg + albedo_b * (1 - svfbuveg) * \
                            #(radG * (1 - F_sh) + radD * F_sh) # *sin(altitude(i) * (pi / 180))

        Kup, KupE, KupS, KupW, KupN = Kup_veg_2015a(radI, radD, radG, altitude, svfbuveg, albedo_b, F_sh, gvfalb,
                    gvfalbE, gvfalbS, gvfalbW, gvfalbN, gvfalbnosh, gvfalbnoshE, gvfalbnoshS, gvfalbnoshW, gvfalbnoshN)

        Keast, Ksouth, Kwest, Knorth, KsideI, KsideD = Kside_veg_v2019a(radI, radD, radG, shadow, svfS, svfW, svfN, svfE,
                    svfEveg, svfSveg, svfWveg, svfNveg, azimuth, altitude, psi, t, albedo_b, F_sh, KupE, KupS, KupW,
                    KupN, cyl, lv, ani, diffsh, rows, cols)

        firstdaytime = 0

    else:  # # # # # # # NIGHTTIME # # # # # # # #

        Tgwall = 0
        # CI_Tg = -999  # F_sh = []

        # Nocturnal K fluxes set to 0
        Knight = np.zeros((rows, cols))
        Kdown = np.zeros((rows, cols))
        Kwest = np.zeros((rows, cols))
        Kup = np.zeros((rows, cols))
        Keast = np.zeros((rows, cols))
        Ksouth = np.zeros((rows, cols))
        Knorth = np.zeros((rows, cols))
        KsideI = np.zeros((rows, cols))
        KsideD = np.zeros((rows, cols))
        F_sh = np.zeros((rows, cols))
        Tg = np.zeros((rows, cols))
        shadow = np.zeros((rows, cols))

        # # # # Lup # # # #
        Lup = SBC * emis_grid * ((Knight + Ta + Tg + 273.15) ** 4)
        if landcover == 1:
            Lup[lc_grid == 3] = SBC * 0.98 * (Twater + 273.15) ** 4  # nocturnal Water temp

        LupE = Lup
        LupS = Lup
        LupW = Lup
        LupN = Lup

        # # For Tg output in POIs
        TgOut = Ta + Tg

        I0 = 0
        timeadd = 0
        firstdaytime = 1

    # # # # Ldown # # # #
    Ldown = (svf + svfveg - 1) * esky * SBC * ((Ta + 273.15) ** 4) + (2 - svfveg - svfaveg) * ewall * SBC * \
                    ((Ta + 273.15) ** 4) + (svfaveg - svf) * ewall * SBC * ((Ta + 273.15 + Tgwall) ** 4) + \
                    (2 - svf - svfveg) * (1 - ewall) * esky * SBC * ((Ta + 273.15) ** 4)  # Jonsson et al.(2006)
    # Ldown = Ldown - 25 # Shown by Jonsson et al.(2006) and Duarte et al.(2006)

    if CI < 0.95:  # non - clear conditions
        c = 1 - CI
        Ldown = Ldown * (1 - c) + c * ((svf + svfveg - 1) * SBC * ((Ta + 273.15) ** 4) + (2 - svfveg - svfaveg) *
                ewall * SBC * ((Ta + 273.15) ** 4) + (svfaveg - svf) * ewall * SBC * ((Ta + 273.15 + Tgwall) ** 4) +
                (2 - svf - svfveg) * (1 - ewall) * SBC * ((Ta + 273.15) ** 4))  # NOT REALLY TESTED!!! BUT MORE CORRECT?

    # # # # Lside # # # #
    Least, Lsouth, Lwest, Lnorth = Lside_veg_v2015a(svfS, svfW, svfN, svfE, svfEveg, svfSveg, svfWveg, svfNveg,
                    svfEaveg, svfSaveg, svfWaveg, svfNaveg, azimuth, altitude, Ta, Tgwall, SBC, ewall, Ldown,
                                                      esky, t, F_sh, CI, LupE, LupS, LupW, LupN)

    # # # # Calculation of radiant flux density and Tmrt # # # #
    if cyl == 1 and ani == 1:  # Human body considered as a cylinder with Perez et al. (1993)
        Sstr = absK * ((KsideI + KsideD) * Fcyl + (Kdown + Kup) * Fup + (Knorth + Keast + Ksouth + Kwest) * Fside) + absL * \
                        (Ldown * Fup + Lup * Fup + Lnorth * Fside + Least * Fside + Lsouth * Fside + Lwest * Fside)
    elif cyl == 1 and ani == 0: # Human body considered as a cylinder with isotropic all-sky diffuse
        Sstr = absK * (KsideI * Fcyl + (Kdown + Kup) * Fup + (Knorth + Keast + Ksouth + Kwest) * Fside) + absL * \
                        (Ldown * Fup + Lup * Fup + Lnorth * Fside + Least * Fside + Lsouth * Fside + Lwest * Fside)
    # Knorth = nan Ksouth = nan Kwest = nan Keast = nan
    else: # Human body considered as a standing cube
        Sstr = absK * ((Kdown + Kup) * Fup + (Knorth + Keast + Ksouth + Kwest) * Fside) +absL * \
                        (Ldown * Fup + Lup * Fup + Lnorth * Fside + Least * Fside + Lsouth * Fside + Lwest * Fside)
    # Sstr = absK * (Kdown * Fup + Kup * Fup + Knorth * Fside + Keast * Fside + Ksouth * Fside + Kwest * Fside)...
    # +absL * (Ldown * Fup + Lup * Fup + Lnorth * Fside + Least * Fside + Lsouth * Fside + Lwest * Fside)
    # KsideI = nan

    Tmrt = np.sqrt(np.sqrt((Sstr / (absL * SBC)))) - 273.2
    # Sstr = absK * (Kdown * Fup + Kup * Fup + Knorth * Fside + Keast * Fside + Ksouth * Fside + Kwest * Fside)...
    # +absL * (Ldown * Fup + Lup * Fup + Lnorth * Fside + Least * Fside + Lsouth * Fside + Lwest * Fside)
    # Tmrt = sqrt(sqrt((Sstr / (absL * SBC)))) - 273.2

    return Tmrt, Kdown, Kup, Ldown, Lup, Tg, ea, esky, I0, CI, shadow, firstdaytime, timestepdec, \
           timeadd, Tgmap1, timeaddE, Tgmap1E, timeaddS, Tgmap1S, timeaddW, Tgmap1W, timeaddN, Tgmap1N, \
           Keast, Ksouth, Kwest, Knorth, Least, Lsouth, Lwest, Lnorth, KsideI, TgOut1, TgOut, radI, radD

    # return Tmrt, Kdown, Kup, Ldown, Lup, Tg, ea, esky, I0, CI, shadow, firstdaytime, timestepdec, \
    #        timeadd, Tgmap1, timeaddE, Tgmap1E, timeaddS, Tgmap1S, timeaddW, Tgmap1W, timeaddN, Tgmap1N, \
    #        Keast, Ksouth, Kwest, Knorth, Least, Lsouth, Lwest, Lnorth, KsideI, KsideD, dRad, Sstr, lv, radD, aniLum


def sun_distance(jday):
    """

    #% Calculatesrelative earth sun distance
    #% with day of year as input.
    #% Partridge and Platt, 1975
    """
    b = 2.*np.pi*jday/365.
    D = np.sqrt((1.00011+np.dot(0.034221, np.cos(b))+np.dot(0.001280, np.sin(b))+np.dot(0.000719,
                                        np.cos((2.*b)))+np.dot(0.000077, np.sin((2.*b)))))
    return D

def clearnessindex_2013b(zen, jday, Ta, RH, radG, location, P):

    """ Clearness Index at the Earth's surface calculated from Crawford and Duchon 1999

    :param zen: zenith angle in radians
    :param jday: day of year
    :param Ta: air temperature
    :param RH: relative humidity
    :param radG: global shortwave radiation
    :param location: distionary including lat, lon and alt
    :param P: pressure
    :return:
    """
    import numpy as np

    ## update the handle the Ta is array case, if Ta is array, then just use the mean value
    if not np.isscalar(Ta): Ta = np.mean(Ta)

    if P == -999.0:
        p = 1013.  # Pressure in millibars
    else:
        p = P*10.  # Convert from hPa to millibars

    Itoa = 1370.0  # Effective solar constant
#     D = sun_distance.sun_distance(jday)  # irradiance differences due to Sun-Earth distances
    D = sun_distance(jday)  # irradiance differences due to Sun-Earth distances
    m = 35. * np.cos(zen) * ((1224. * (np.cos(zen)**2) + 1) ** (-1/2.))     # optical air mass at p=1013
    Trpg = 1.021-0.084*(m*(0.000949*p+0.051))**0.5  # Transmission coefficient for Rayliegh scattering and permanent gases

    # empirical constant depending on latitude
    if location['latitude'] < 10.:
        G = [3.37, 2.85, 2.80, 2.64]
    elif location['latitude'] >= 10. and location['latitude'] < 20.:
        G = [2.99, 3.02, 2.70, 2.93]
    elif location['latitude'] >= 20. and location['latitude'] <30.:
        G = [3.60, 3.00, 2.98, 2.93]
    elif location['latitude'] >= 30. and location['latitude'] <40.:
        G = [3.04, 3.11, 2.92, 2.94]
    elif location['latitude'] >= 40. and location['latitude'] <50.:
        G = [2.70, 2.95, 2.77, 2.71]
    elif location['latitude'] >= 50. and location['latitude'] <60.:
        G = [2.52, 3.07, 2.67, 2.93]
    elif location['latitude'] >= 60. and location['latitude'] <70.:
        G = [1.76, 2.69, 2.61, 2.61]
    elif location['latitude'] >= 70. and location['latitude'] <80.:
        G = [1.60, 1.67, 2.24, 2.63]
    elif location['latitude'] >= 80. and location['latitude'] <90.:
        G = [1.11, 1.44, 1.94, 2.02]

    if jday > 335 or jday <= 60:
        G = G[0]
    elif jday > 60 and jday <= 152:
        G = G[1]
    elif jday > 152 and jday <= 244:
        G = G[2]
    elif jday > 244 and jday <= 335:
        G = G[3]

    # dewpoint calculation
    a2 = 17.27
    b2 = 237.7
    Td = (b2*(((a2*Ta)/(b2+Ta))+np.log(RH)))/(a2-(((a2*Ta)/(b2+Ta))+np.log(RH)))
    Td = (Td*1.8)+32  # Dewpoint (F)
    u = np.exp(0.1133-np.log(G+1)+0.0393*Td)  # Precipitable water
    Tw = 1-0.077*((u*m)**0.3)  # Transmission coefficient for water vapor
    Tar = 0.935**m  # Transmission coefficient for aerosols

    I0=Itoa*np.cos(zen)*Trpg*Tw*D*Tar
    if abs(zen)>np.pi/2:
        I0 = 0
    # b=I0==abs(zen)>np.pi/2
    # I0(b==1)=0
    # clear b;
    if not(np.isreal(I0)):
        I0 = 0

    corr=0.1473*np.log(90-(zen/np.pi*180))+0.3454  # 20070329

    CIuncorr = radG / I0
    CI = CIuncorr + (1-corr)
    I0et = Itoa*np.cos(zen)*D  # extra terrestial solar radiation
    Kt = radG / I0et
    if math.isnan(CI):
        CI = float('Inf')

    return I0, CI, Kt, I0et, CIuncorr

def daylen(DOY, XLAT):
    # Calculation of declination of sun (Eqn. 16). Amplitude= +/-23.45
    # deg. Minimum = DOY 355 (DEC 21), maximum = DOY 172.5 (JUN 21/22).
    # Sun angles.  SOC limited for latitudes above polar circles.
    # Calculate daylength, sunrise and sunset (Eqn. 17)
    
    RAD=np.pi/180.0

    DEC = -23.45 * np.cos(2.0*np.pi*(DOY+10.0)/365.0)

    SOC = np.tan(RAD*DEC) * np.tan(RAD*XLAT)
    SOC = min(max(SOC,-1.0),1.0)
    # SOC=alt

    DAYL = 12.0 + 24.0*np.arcsin(SOC)/np.pi
    SNUP = 12.0 - DAYL/2.0
    SNDN = 12.0 + DAYL/2.0

    return DAYL, DEC, SNDN, SNUP

def sun_position(time, location):
    """
    % sun = sun_position(time, location)
    %
    % This function compute the sun position (zenith and azimuth angle at the observer
    % location) as a function of the observer local time and position.
    %
    % It is an implementation of the algorithm presented by Reda et Andreas in:
    %   Reda, I., Andreas, A. (2003) Solar position algorithm for solar
    %   radiation application. National Renewable Energy Laboratory (NREL)
    %   Technical report NREL/TP-560-34302.
    % This document is avalaible at www.osti.gov/bridge
    %
    % This algorithm is based on numerical approximation of the exact equations.
    % The authors of the original paper state that this algorithm should be
    % precise at +/- 0.0003 degrees. I have compared it to NOAA solar table
    % (http://www.srrb.noaa.gov/highlights/sunrise/azel.html) and to USNO solar
    % table (http://aa.usno.navy.mil/data/docs/AltAz.html) and found very good
    % correspondance (up to the precision of those tables), except for large
    % zenith angle, where the refraction by the atmosphere is significant
    % (difference of about 1 degree). Note that in this code the correction
    % for refraction in the atmosphere as been implemented for a temperature
    % of 10C (283 kelvins) and a pressure of 1010 mbar. See the subfunction
    % �sun_topocentric_zenith_angle_calculation� for a possible modification
    % to explicitely model the effect of temperature and pressure as describe
    % in Reda & Andreas (2003).
    %
    % Input parameters:
    %   time: a structure that specify the time when the sun position is
    %   calculated.
    %       time.year: year. Valid for [-2000, 6000]
    %       time.month: month [1-12]
    %       time.day: calendar day [1-31]
    %       time.hour: local hour [0-23]
    %       time.min: minute [0-59]
    %       time.sec: second [0-59]
    %       time.UTC: offset hour from UTC. Local time = Greenwich time + time.UTC
    %   This input can also be passed using the Matlab time format ('dd-mmm-yyyy HH:MM:SS').
    %   In that case, the time has to be specified as UTC time (time.UTC = 0)
    %
    %   location: a structure that specify the location of the observer
    %       location.latitude: latitude (in degrees, north of equator is
    %       positive)
    %       location.longitude: longitude (in degrees, positive for east of
    %       Greenwich)
    %       location.altitude: altitude above mean sea level (in meters)
    %
    % Output parameters
    %   sun: a structure with the calculated sun position
    %       sun.zenith = zenith angle in degrees (angle from the vertical)
    %       sun.azimuth = azimuth angle in degrees, eastward from the north.
    % Only the sun zenith and azimuth angles are returned as output, but a lot
    % of other parameters are calculated that could also extracted as output of
    % this function.
    %
    % Exemple of use
    %
    % location.longitude = -105.1786;
    % location.latitude = 39.742476;
    % location.altitude = 1830.14;
    % time.year = 2005;
    % time.month = 10;
    % time.day = 17;
    % time.hour = 6;
    % time.min = 30;
    % time.sec = 30;
    % time.UTC = -7;
    % %
    % location.longitude = 11.94;
    % location.latitude = 57.70;
    % location.altitude = 3.0;
    % time.UTC = 1;
    % sun = sun_position(time, location);
    %
    % sun =
    %
    %      zenith: 50.1080438859849
    %      azimuth: 194.341174010338
    %
    % History
    %   09/03/2004  Original creation by Vincent Roy (vincent.roy@drdc-rddc.gc.ca)
    %   10/03/2004  Fixed a bug in julian_calculation subfunction (was
    %               incorrect for year 1582 only), Vincent Roy
    %   18/03/2004  Correction to the header (help display) only. No changes to
    %               the code. (changed the �elevation� field in �location� structure
    %               information to �altitude�), Vincent Roy
    %   13/04/2004  Following a suggestion from Jody Klymak (jklymak@ucsd.edu),
    %               allowed the 'time' input to be passed as a Matlab time string.
    %   22/08/2005  Following a bug report from Bruce Bowler
    %               (bbowler@bigelow.org), modified the julian_calculation function. Bug
    %               was 'MATLAB has allowed structure assignment  to a non-empty non-structure
    %               to overwrite the previous value.  This behavior will continue in this release,
    %               but will be an error in a  future version of MATLAB.  For advice on how to
    %               write code that  will both avoid this warning and work in future versions of
    %               MATLAB,  see R14SP2 Release Notes'. Script should now be
    %               compliant with futher release of Matlab...
    """

    # 1. Calculate the Julian Day, and Century. Julian Ephemeris day, century
    # and millenium are calculated using a mean delta_t of 33.184 seconds.
    julian = julian_calculation(time)
    #print(julian)
    
    # 2. Calculate the Earth heliocentric longitude, latitude, and radius
    # vector (L, B, and R)
    earth_heliocentric_position = earth_heliocentric_position_calculation(julian)

    # 3. Calculate the geocentric longitude and latitude
    sun_geocentric_position = sun_geocentric_position_calculation(earth_heliocentric_position)

    # 4. Calculate the nutation in longitude and obliquity (in degrees).
    nutation = nutation_calculation(julian)

    # 5. Calculate the true obliquity of the ecliptic (in degrees).
    true_obliquity = true_obliquity_calculation(julian, nutation)

    # 6. Calculate the aberration correction (in degrees)
    aberration_correction = abberation_correction_calculation(earth_heliocentric_position)

    # 7. Calculate the apparent sun longitude in degrees)
    apparent_sun_longitude = apparent_sun_longitude_calculation(sun_geocentric_position, nutation, aberration_correction)

    # 8. Calculate the apparent sideral time at Greenwich (in degrees)
    apparent_stime_at_greenwich = apparent_stime_at_greenwich_calculation(julian, nutation, true_obliquity)

    # 9. Calculate the sun rigth ascension (in degrees)
    sun_rigth_ascension = sun_rigth_ascension_calculation(apparent_sun_longitude, true_obliquity, sun_geocentric_position)

    # 10. Calculate the geocentric sun declination (in degrees). Positive or
    # negative if the sun is north or south of the celestial equator.
    sun_geocentric_declination = sun_geocentric_declination_calculation(apparent_sun_longitude, true_obliquity,
                                                                        sun_geocentric_position)

    # 11. Calculate the observer local hour angle (in degrees, westward from south).
    observer_local_hour = observer_local_hour_calculation(apparent_stime_at_greenwich, location, sun_rigth_ascension)

    # 12. Calculate the topocentric sun position (rigth ascension, declination and
    # rigth ascension parallax in degrees)
    topocentric_sun_position = topocentric_sun_position_calculate(earth_heliocentric_position, location,
                                                                  observer_local_hour, sun_rigth_ascension,
                                                                  sun_geocentric_declination)

    # 13. Calculate the topocentric local hour angle (in degrees)
    topocentric_local_hour = topocentric_local_hour_calculate(observer_local_hour, topocentric_sun_position)

    # 14. Calculate the topocentric zenith and azimuth angle (in degrees)
    sun = sun_topocentric_zenith_angle_calculate(location, topocentric_sun_position, topocentric_local_hour)

    return sun

def julian_calculation(t_input):
    """
    % This function compute the julian day and julian century from the local
    % time and timezone information. Ephemeris are calculated with a delta_t=0
    % seconds.

    % If time input is a Matlab time string, extract the information from
    % this string and create the structure as defined in the main header of
    % this script.
    """
    if not isinstance(t_input, dict):
        # tt = datetime.datetime.strptime(t_input, "%Y-%m-%d %H:%M:%S.%f")    # if t_input is a string of this format
        # t_input should be a datetime object
        time = dict()
        time['UTC'] = 0
        time['year'] = t_input.year
        time['month'] = t_input.month
        time['day'] = t_input.day
        time['hour'] = t_input.hour
        time['min'] = t_input.minute
        time['sec'] = t_input.second
    else:
        time = t_input

    if time['month'] == 1 or time['month'] == 2:
        Y = time['year'] - 1
        M = time['month'] + 12
    else:
        Y = time['year']
        M = time['month']
    
    ut_time = ((time['hour'] - time['UTC'])/24) + (time['min']/(60*24)) + (time['sec']/(60*60*24))   # time of day in UT time.
    D = time['day'] + ut_time   # Day of month in decimal time, ex. 2sd day of month at 12:30:30UT, D=2.521180556

    # In 1582, the gregorian calendar was adopted
    if time['year'] == 1582:
        if time['month'] == 10:
            if time['day'] <= 4:   # The Julian calendar ended on October 4, 1582
                B = (0)
            elif time['day'] >= 15:   # The Gregorian calendar started on October 15, 1582
                A = np.floor(Y/100)
                B = 2 - A + np.floor(A/4)
            else:
                print('This date never existed!. Date automatically set to October 4, 1582')
                time['month'] = 10
                time['day'] = 4
                B = 0
        elif time['month'] < 10:   # Julian calendar
            B = 0
        else: # Gregorian calendar
            A = np.floor(Y/100)
            B = 2 - A + np.floor(A/4)
    elif time['year'] < 1582:   # Julian calendar
        B = 0
    else:
        A = np.floor(Y/100)    # Gregorian calendar
        B = 2 - A + np.floor(A/4)

    julian = dict()
    julian['day'] = D + B + np.floor(365.25*(Y+4716)) + np.floor(30.6001*(M+1)) - 1524.5

    delta_t = 0   # 33.184;
    julian['ephemeris_day'] = (julian['day']) + (delta_t/86400)
    julian['century'] = (julian['day'] - 2451545) / 36525
    julian['ephemeris_century'] = (julian['ephemeris_day'] - 2451545) / 36525
    julian['ephemeris_millenium'] = (julian['ephemeris_century']) / 10

    return julian

def earth_heliocentric_position_calculation(julian):
    """
    % This function compute the earth position relative to the sun, using
    % tabulated values. 
    
    % Tabulated values for the longitude calculation
    % L terms  from the original code.
    """
    # Tabulated values for the longitude calculation
    # L terms  from the original code. 
    L0_terms = np.array([[175347046.0, 0, 0],
                        [3341656.0, 4.6692568, 6283.07585],
                        [34894.0, 4.6261, 12566.1517],
                        [3497.0, 2.7441, 5753.3849],
                        [3418.0, 2.8289, 3.5231],
                        [3136.0, 3.6277, 77713.7715],
                        [2676.0, 4.4181, 7860.4194],
                        [2343.0, 6.1352, 3930.2097],
                        [1324.0, 0.7425, 11506.7698],
                        [1273.0, 2.0371, 529.691],
                        [1199.0, 1.1096, 1577.3435],
                        [990, 5.233, 5884.927],
                        [902, 2.045, 26.298],
                        [857, 3.508, 398.149],
                        [780, 1.179, 5223.694],
                        [753, 2.533, 5507.553],
                        [505, 4.583, 18849.228],
                        [492, 4.205, 775.523],
                        [357, 2.92, 0.067],
                        [317, 5.849, 11790.629],
                        [284, 1.899, 796.298],
                        [271, 0.315, 10977.079],
                        [243, 0.345, 5486.778],
                        [206, 4.806, 2544.314],
                        [205, 1.869, 5573.143],
                        [202, 2.4458, 6069.777],
                        [156, 0.833, 213.299],
                        [132, 3.411, 2942.463],
                        [126, 1.083, 20.775],
                        [115, 0.645, 0.98],
                        [103, 0.636, 4694.003],
                        [102, 0.976, 15720.839],
                        [102, 4.267, 7.114],
                        [99, 6.21, 2146.17],
                        [98, 0.68, 155.42],
                        [86, 5.98, 161000.69],
                        [85, 1.3, 6275.96],
                        [85, 3.67, 71430.7],
                        [80, 1.81, 17260.15],
                        [79, 3.04, 12036.46],
                        [71, 1.76, 5088.63],
                        [74, 3.5, 3154.69],
                        [74, 4.68, 801.82],
                        [70, 0.83, 9437.76],
                        [62, 3.98, 8827.39],
                        [61, 1.82, 7084.9],
                        [57, 2.78, 6286.6],
                        [56, 4.39, 14143.5],
                        [56, 3.47, 6279.55],
                        [52, 0.19, 12139.55],
                        [52, 1.33, 1748.02],
                        [51, 0.28, 5856.48],
                        [49, 0.49, 1194.45],
                        [41, 5.37, 8429.24],
                        [41, 2.4, 19651.05],
                        [39, 6.17, 10447.39],
                        [37, 6.04, 10213.29],
                        [37, 2.57, 1059.38],
                        [36, 1.71, 2352.87],
                        [36, 1.78, 6812.77],
                        [33, 0.59, 17789.85],
                        [30, 0.44, 83996.85],
                        [30, 2.74, 1349.87],
                        [25, 3.16, 4690.48]])

    L1_terms = np.array([[628331966747.0, 0, 0],
                        [206059.0, 2.678235, 6283.07585],
                        [4303.0, 2.6351, 12566.1517],
                        [425.0, 1.59, 3.523],
                        [119.0, 5.796, 26.298],
                        [109.0, 2.966, 1577.344],
                        [93, 2.59, 18849.23],
                        [72, 1.14, 529.69],
                        [68, 1.87, 398.15],
                        [67, 4.41, 5507.55],
                        [59, 2.89, 5223.69],
                        [56, 2.17, 155.42],
                        [45, 0.4, 796.3],
                        [36, 0.47, 775.52],
                        [29, 2.65, 7.11],
                        [21, 5.34, 0.98],
                        [19, 1.85, 5486.78],
                        [19, 4.97, 213.3],
                        [17, 2.99, 6275.96],
                        [16, 0.03, 2544.31],
                        [16, 1.43, 2146.17],
                        [15, 1.21, 10977.08],
                        [12, 2.83, 1748.02],
                        [12, 3.26, 5088.63],
                        [12, 5.27, 1194.45],
                        [12, 2.08, 4694],
                        [11, 0.77, 553.57],
                        [10, 1.3, 3286.6],
                        [10, 4.24, 1349.87],
                        [9, 2.7, 242.73],
                        [9, 5.64, 951.72],
                        [8, 5.3, 2352.87],
                        [6, 2.65, 9437.76],
                        [6, 4.67, 4690.48]])

    L2_terms = np.array([[52919.0, 0, 0],
                        [8720.0, 1.0721, 6283.0758],
                        [309.0, 0.867, 12566.152],
                        [27, 0.05, 3.52],
                        [16, 5.19, 26.3],
                        [16, 3.68, 155.42],
                        [10, 0.76, 18849.23],
                        [9, 2.06, 77713.77],
                        [7, 0.83, 775.52],
                        [5, 4.66, 1577.34],
                        [4, 1.03, 7.11],
                        [4, 3.44, 5573.14],
                        [3, 5.14, 796.3],
                        [3, 6.05, 5507.55],
                        [3, 1.19, 242.73],
                        [3, 6.12, 529.69],
                        [3, 0.31, 398.15],
                        [3, 2.28, 553.57],
                        [2, 4.38, 5223.69],
                        [2, 3.75, 0.98]])

    L3_terms = np.array([[289.0, 5.844, 6283.076],
                        [35, 0, 0],
                        [17, 5.49, 12566.15],
                        [3, 5.2, 155.42],
                        [1, 4.72, 3.52],
                        [1, 5.3, 18849.23],
                        [1, 5.97, 242.73]])
    L4_terms = np.array([[114.0, 3.142, 0],
                        [8, 4.13, 6283.08],
                        [1, 3.84, 12566.15]])

    L5_terms = np.array([1, 3.14, 0])
    L5_terms = np.atleast_2d(L5_terms)    # since L5_terms is 1D, we have to convert it to 2D to avoid indexErrors

    A0 = L0_terms[:, 0]
    B0 = L0_terms[:, 1]
    C0 = L0_terms[:, 2]

    A1 = L1_terms[:, 0]
    B1 = L1_terms[:, 1]
    C1 = L1_terms[:, 2]

    A2 = L2_terms[:, 0]
    B2 = L2_terms[:, 1]
    C2 = L2_terms[:, 2]

    A3 = L3_terms[:, 0]
    B3 = L3_terms[:, 1]
    C3 = L3_terms[:, 2]

    A4 = L4_terms[:, 0]
    B4 = L4_terms[:, 1]
    C4 = L4_terms[:, 2]

    A5 = L5_terms[:, 0]
    B5 = L5_terms[:, 1]
    C5 = L5_terms[:, 2]

    JME = julian['ephemeris_millenium']

    # Compute the Earth Heliochentric longitude from the tabulated values.
    L0 = np.sum(A0 * np.cos(B0 + (C0 * JME)))
    L1 = np.sum(A1 * np.cos(B1 + (C1 * JME)))
    L2 = np.sum(A2 * np.cos(B2 + (C2 * JME)))
    L3 = np.sum(A3 * np.cos(B3 + (C3 * JME)))
    L4 = np.sum(A4 * np.cos(B4 + (C4 * JME)))
    L5 = A5 * np.cos(B5 + (C5 * JME))
    
    earth_heliocentric_position = dict()
    earth_heliocentric_position['longitude'] = (L0 + (L1 * JME) + (L2 * np.power(JME, 2)) +
                                                          (L3 * np.power(JME, 3)) +
                                                          (L4 * np.power(JME, 4)) +
                                                          (L5 * np.power(JME, 5))) / 1e8
    # Convert the longitude to degrees.
    earth_heliocentric_position['longitude'] = earth_heliocentric_position['longitude'] * 180/np.pi

    # Limit the range to [0,360]
    earth_heliocentric_position['longitude'] = set_to_range(earth_heliocentric_position['longitude'], 0, 360)

    # Tabulated values for the earth heliocentric latitude. 
    # B terms  from the original code. 
    B0_terms = np.array([[280.0, 3.199, 84334.662],
                        [102.0, 5.422, 5507.553],
                        [80, 3.88, 5223.69],
                        [44, 3.7, 2352.87],
                        [32, 4, 1577.34]])

    B1_terms = np.array([[9, 3.9, 5507.55],
                         [6, 1.73, 5223.69]])

    A0 = B0_terms[:, 0]
    B0 = B0_terms[:, 1]
    C0 = B0_terms[:, 2]
    
    A1 = B1_terms[:, 0]
    B1 = B1_terms[:, 1]
    C1 = B1_terms[:, 2]
    
    L0 = np.sum(A0 * np.cos(B0 + (C0 * JME)))
    L1 = np.sum(A1 * np.cos(B1 + (C1 * JME)))

    earth_heliocentric_position['latitude'] = (L0 + (L1 * JME)) / 1e8

    # Convert the latitude to degrees. 
    earth_heliocentric_position['latitude'] = earth_heliocentric_position['latitude'] * 180/np.pi

    # Limit the range to [0,360];
    earth_heliocentric_position['latitude'] = set_to_range(earth_heliocentric_position['latitude'], 0, 360)

    # Tabulated values for radius vector. 
    # R terms from the original code
    R0_terms = np.array([[100013989.0, 0, 0],
                        [1670700.0, 3.0984635, 6283.07585],
                        [13956.0, 3.05525, 12566.1517],
                        [3084.0, 5.1985, 77713.7715],
                        [1628.0, 1.1739, 5753.3849],
                        [1576.0, 2.8469, 7860.4194],
                        [925.0, 5.453, 11506.77],
                        [542.0, 4.564, 3930.21],
                        [472.0, 3.661, 5884.927],
                        [346.0, 0.964, 5507.553],
                        [329.0, 5.9, 5223.694],
                        [307.0, 0.299, 5573.143],
                        [243.0, 4.273, 11790.629],
                        [212.0, 5.847, 1577.344],
                        [186.0, 5.022, 10977.079],
                        [175.0, 3.012, 18849.228],
                        [110.0, 5.055, 5486.778],
                        [98, 0.89, 6069.78],
                        [86, 5.69, 15720.84],
                        [86, 1.27, 161000.69],
                        [85, 0.27, 17260.15],
                        [63, 0.92, 529.69],
                        [57, 2.01, 83996.85],
                        [56, 5.24, 71430.7],
                        [49, 3.25, 2544.31],
                        [47, 2.58, 775.52],
                        [45, 5.54, 9437.76],
                        [43, 6.01, 6275.96],
                        [39, 5.36, 4694],
                        [38, 2.39, 8827.39],
                        [37, 0.83, 19651.05],
                        [37, 4.9, 12139.55],
                        [36, 1.67, 12036.46],
                        [35, 1.84, 2942.46],
                        [33, 0.24, 7084.9],
                        [32, 0.18, 5088.63],
                        [32, 1.78, 398.15],
                        [28, 1.21, 6286.6],
                        [28, 1.9, 6279.55],
                        [26, 4.59, 10447.39]])

    R1_terms = np.array([[103019.0, 1.10749, 6283.07585],
                        [1721.0, 1.0644, 12566.1517],
                        [702.0, 3.142, 0],
                        [32, 1.02, 18849.23],
                        [31, 2.84, 5507.55],
                        [25, 1.32, 5223.69],
                        [18, 1.42, 1577.34],
                        [10, 5.91, 10977.08],
                        [9, 1.42, 6275.96],
                        [9, 0.27, 5486.78]])

    R2_terms = np.array([[4359.0, 5.7846, 6283.0758],
                        [124.0, 5.579, 12566.152],
                        [12, 3.14, 0],
                        [9, 3.63, 77713.77],
                        [6, 1.87, 5573.14],
                        [3, 5.47, 18849]])

    R3_terms = np.array([[145.0, 4.273, 6283.076],
                        [7, 3.92, 12566.15]])
    
    R4_terms = [4, 2.56, 6283.08]
    R4_terms = np.atleast_2d(R4_terms)    # since L5_terms is 1D, we have to convert it to 2D to avoid indexErrors

    A0 = R0_terms[:, 0]
    B0 = R0_terms[:, 1]
    C0 = R0_terms[:, 2]
    
    A1 = R1_terms[:, 0]
    B1 = R1_terms[:, 1]
    C1 = R1_terms[:, 2]
    
    A2 = R2_terms[:, 0]
    B2 = R2_terms[:, 1]
    C2 = R2_terms[:, 2]
    
    A3 = R3_terms[:, 0]
    B3 = R3_terms[:, 1]
    C3 = R3_terms[:, 2]
    
    A4 = R4_terms[:, 0]
    B4 = R4_terms[:, 1]
    C4 = R4_terms[:, 2]

    # Compute the Earth heliocentric radius vector
    L0 = np.sum(A0 * np.cos(B0 + (C0 * JME)))
    L1 = np.sum(A1 * np.cos(B1 + (C1 * JME)))
    L2 = np.sum(A2 * np.cos(B2 + (C2 * JME)))
    L3 = np.sum(A3 * np.cos(B3 + (C3 * JME)))
    L4 = A4 * np.cos(B4 + (C4 * JME))

    # Units are in AU
    earth_heliocentric_position['radius'] = (L0 + (L1 * JME) + (L2 * np.power(JME, 2)) +
                                             (L3 * np.power(JME, 3)) +
                                             (L4 * np.power(JME, 4))) / 1e8

    return earth_heliocentric_position

def sun_geocentric_position_calculation(earth_heliocentric_position):
    """
    % This function compute the sun position relative to the earth.
    """
    sun_geocentric_position = dict()
    sun_geocentric_position['longitude'] = earth_heliocentric_position['longitude'] + 180
    # Limit the range to [0,360];
    sun_geocentric_position['longitude'] = set_to_range(sun_geocentric_position['longitude'], 0, 360)

    sun_geocentric_position['latitude'] = -earth_heliocentric_position['latitude']
    # Limit the range to [0,360]
    sun_geocentric_position['latitude'] = set_to_range(sun_geocentric_position['latitude'], 0, 360)
    return sun_geocentric_position

def nutation_calculation(julian):
    """
    % This function compute the nutation in longtitude and in obliquity, in
    % degrees.
    :param julian:
    :return: nutation
    """

    # All Xi are in degrees.
    JCE = julian['ephemeris_century']

    # 1. Mean elongation of the moon from the sun
    p = np.atleast_2d([(1/189474), -0.0019142, 445267.11148, 297.85036])

    # X0 = polyval(p, JCE);
    X0 = p[0, 0] * np.power(JCE, 3) + p[0, 1] * np.power(JCE, 2) + p[0, 2] * JCE + p[0, 3]   # This is faster than polyval...

    # 2. Mean anomaly of the sun (earth)
    p = np.atleast_2d([-(1/300000), -0.0001603, 35999.05034, 357.52772])

    # X1 = polyval(p, JCE)
    X1 = p[0, 0] * np.power(JCE, 3) + p[0, 1] * np.power(JCE, 2) + p[0, 2] * JCE + p[0, 3]

    # 3. Mean anomaly of the moon
    p = np.atleast_2d([(1/56250), 0.0086972, 477198.867398, 134.96298])
    
    # X2 = polyval(p, JCE);
    X2 = p[0, 0] * np.power(JCE, 3) + p[0, 1] * np.power(JCE, 2) + p[0, 2] * JCE + p[0, 3]

    # 4. Moon argument of latitude
    p = np.atleast_2d([(1/327270), -0.0036825, 483202.017538, 93.27191])

    # X3 = polyval(p, JCE)
    X3 = p[0, 0] * np.power(JCE, 3) + p[0, 1] * np.power(JCE, 2) + p[0, 2] * JCE + p[0, 3]

    # 5. Longitude of the ascending node of the moon's mean orbit on the
    # ecliptic, measured from the mean equinox of the date
    p = np.atleast_2d([(1/450000), 0.0020708, -1934.136261, 125.04452])

    # X4 = polyval(p, JCE);
    X4 = p[0, 0] * np.power(JCE, 3) + p[0, 1] * np.power(JCE, 2) + p[0, 2] * JCE + p[0, 3]

    # Y tabulated terms from the original code
    Y_terms = np.array([[0, 0, 0, 0, 1],
                        [-2, 0, 0, 2, 2],
                        [0, 0, 0, 2, 2],
                        [0, 0, 0, 0, 2],
                        [0, 1, 0, 0, 0],
                        [0, 0, 1, 0, 0],
                        [-2, 1, 0, 2, 2],
                        [0, 0, 0, 2, 1],
                        [0, 0, 1, 2, 2],
                        [-2, -1, 0, 2, 2],
                        [-2, 0, 1, 0, 0],
                        [-2, 0, 0, 2, 1],
                        [0, 0, -1, 2, 2],
                        [2, 0, 0, 0, 0],
                        [0, 0, 1, 0, 1],
                        [2, 0, -1, 2, 2],
                        [0, 0, -1, 0, 1],
                        [0, 0, 1, 2, 1],
                        [-2, 0, 2, 0, 0],
                        [0, 0, -2, 2, 1],
                        [2, 0, 0, 2, 2],
                        [0, 0, 2, 2, 2],
                        [0, 0, 2, 0, 0],
                        [-2, 0, 1, 2, 2],
                        [0, 0, 0, 2, 0],
                        [-2, 0, 0, 2, 0],
                        [0, 0, -1, 2, 1],
                        [0, 2, 0, 0, 0],
                        [2, 0, -1, 0, 1],
                        [-2, 2, 0, 2, 2],
                        [0, 1, 0, 0, 1],
                        [-2, 0, 1, 0, 1],
                        [0, -1, 0, 0, 1],
                        [0, 0, 2, -2, 0],
                        [2, 0, -1, 2, 1],
                        [2, 0, 1, 2, 2],
                        [0, 1, 0, 2, 2],
                        [-2, 1, 1, 0, 0],
                        [0, -1, 0, 2, 2],
                        [2, 0, 0, 2, 1],
                        [2, 0, 1, 0, 0],
                        [-2, 0, 2, 2, 2],
                        [-2, 0, 1, 2, 1],
                        [2, 0, -2, 0, 1],
                        [2, 0, 0, 0, 1],
                        [0, -1, 1, 0, 0],
                        [-2, -1, 0, 2, 1],
                        [-2, 0, 0, 0, 1],
                        [0, 0, 2, 2, 1],
                        [-2, 0, 2, 0, 1],
                        [-2, 1, 0, 2, 1],
                        [0, 0, 1, -2, 0],
                        [-1, 0, 1, 0, 0],
                        [-2, 1, 0, 0, 0],
                        [1, 0, 0, 0, 0],
                        [0, 0, 1, 2, 0],
                        [0, 0, -2, 2, 2],
                        [-1, -1, 1, 0, 0],
                        [0, 1, 1, 0, 0],
                        [0, -1, 1, 2, 2],
                        [2, -1, -1, 2, 2],
                        [0, 0, 3, 2, 2],
                        [2, -1, 0, 2, 2]])

    nutation_terms = np.array([[-171996, -174.2, 92025, 8.9],
                                [-13187, -1.6, 5736, -3.1],
                                [-2274, -0.2, 977, -0.5],
                                [2062, 0.2, -895, 0.5],
                                [1426, -3.4, 54, -0.1],
                                [712, 0.1, -7, 0],
                                [-517, 1.2, 224, -0.6],
                                [-386, -0.4, 200, 0],
                                [-301, 0, 129, -0.1],
                                [217, -0.5, -95, 0.3],
                                [-158, 0, 0, 0],
                                [129, 0.1, -70, 0],
                                [123, 0, -53, 0],
                                [63, 0, 0, 0],
                                [63, 0.1, -33, 0],
                                [-59, 0, 26, 0],
                                [-58, -0.1, 32, 0],
                                [-51, 0, 27, 0],
                                [48, 0, 0, 0],
                                [46, 0, -24, 0],
                                [-38, 0, 16, 0],
                                [-31, 0, 13, 0],
                                [29, 0, 0, 0],
                                [29, 0, -12, 0],
                                [26, 0, 0, 0],
                                [-22, 0, 0, 0],
                                [21, 0, -10, 0],
                                [17, -0.1, 0, 0],
                                [16, 0, -8, 0],
                                [-16, 0.1, 7, 0],
                                [-15, 0, 9, 0],
                                [-13, 0, 7, 0],
                                [-12, 0, 6, 0],
                                [11, 0, 0, 0],
                                [-10, 0, 5, 0],
                                [-8, 0, 3, 0],
                                [7, 0, -3, 0],
                                [-7, 0, 0, 0],
                                [-7, 0, 3, 0],
                                [-7, 0, 3, 0],
                                [6, 0, 0, 0],
                                [6, 0, -3, 0],
                                [6, 0, -3, 0],
                                [-6, 0, 3, 0],
                                [-6, 0, 3, 0],
                                [5, 0, 0, 0],
                                [-5, 0, 3, 0],
                                [-5, 0, 3, 0],
                                [-5, 0, 3, 0],
                                [4, 0, 0, 0],
                                [4, 0, 0, 0],
                                [4, 0, 0, 0],
                                [-4, 0, 0, 0],
                                [-4, 0, 0, 0],
                                [-4, 0, 0, 0],
                                [3, 0, 0, 0],
                                [-3, 0, 0, 0],
                                [-3, 0, 0, 0],
                                [-3, 0, 0, 0],
                                [-3, 0, 0, 0],
                                [-3, 0, 0, 0],
                                [-3, 0, 0, 0],
                                [-3, 0, 0, 0]])

    # Using the tabulated values, compute the delta_longitude and
    # delta_obliquity.
    Xi = np.array([X0, X1, X2, X3, X4])    # a col mat in octave

    tabulated_argument = Y_terms.dot(np.transpose(Xi)) * (np.pi/180)

    delta_longitude = (nutation_terms[:, 0] + (nutation_terms[:, 1] * JCE)) * np.sin(tabulated_argument)
    delta_obliquity = (nutation_terms[:, 2] + (nutation_terms[:, 3] * JCE)) * np.cos(tabulated_argument)

    nutation = dict()    # init nutation dictionary
    # Nutation in longitude
    nutation['longitude'] = np.sum(delta_longitude) / 36000000

    # Nutation in obliquity
    nutation['obliquity'] = np.sum(delta_obliquity) / 36000000

    return nutation

def true_obliquity_calculation(julian, nutation):
    """
    This function compute the true obliquity of the ecliptic.

    :param julian:
    :param nutation:
    :return:
    """

    p = np.atleast_2d([2.45, 5.79, 27.87, 7.12, -39.05, -249.67, -51.38, 1999.25, -1.55, -4680.93, 84381.448])

    # mean_obliquity = polyval(p, julian.ephemeris_millenium/10);
    U = julian['ephemeris_millenium'] / 10
    mean_obliquity = p[0, 0] * np.power(U, 10) + p[0, 1] * np.power(U, 9) + \
                     p[0, 2] * np.power(U, 8) + p[0, 3] * np.power(U, 7) + \
                     p[0, 4] * np.power(U, 6) + p[0, 5] * np.power(U, 5) + \
                     p[0, 6] * np.power(U, 4) + p[0, 7] * np.power(U, 3) + \
                     p[0, 8] * np.power(U, 2) + p[0, 9] * U + p[0, 10]

    true_obliquity = (mean_obliquity/3600) + nutation['obliquity']

    return true_obliquity

def abberation_correction_calculation(earth_heliocentric_position):
    """
    This function compute the aberration_correction, as a function of the
    earth-sun distance.

    :param earth_heliocentric_position:
    :return:
    """
    aberration_correction = -20.4898/(3600*earth_heliocentric_position['radius'])
    return aberration_correction

def apparent_sun_longitude_calculation(sun_geocentric_position, nutation, aberration_correction):
    """
    This function compute the sun apparent longitude

    :param sun_geocentric_position:
    :param nutation:
    :param aberration_correction:
    :return:
    """
    apparent_sun_longitude = sun_geocentric_position['longitude'] + nutation['longitude'] + aberration_correction
    return apparent_sun_longitude

def apparent_stime_at_greenwich_calculation(julian, nutation, true_obliquity):
    """
    This function compute the apparent sideral time at Greenwich.

    :param julian:
    :param nutation:
    :param true_obliquity:
    :return:
    """

    JD = julian['day']
    JC = julian['century']

    # Mean sideral time, in degrees
    mean_stime = 280.46061837 + (360.98564736629*(JD-2451545)) + \
                 (0.000387933*np.power(JC, 2)) - \
                 (np.power(JC, 3)/38710000)

    # Limit the range to [0-360];
    mean_stime = set_to_range(mean_stime, 0, 360)

    apparent_stime_at_greenwich = mean_stime + (nutation['longitude'] * np.cos(true_obliquity * np.pi/180))
    return apparent_stime_at_greenwich

def sun_rigth_ascension_calculation(apparent_sun_longitude, true_obliquity, sun_geocentric_position):
    """
    This function compute the sun rigth ascension.
    :param apparent_sun_longitude:
    :param true_obliquity:
    :param sun_geocentric_position:
    :return:
    """

    argument_numerator = (np.sin(apparent_sun_longitude * np.pi/180) * np.cos(true_obliquity * np.pi/180)) - \
        (np.tan(sun_geocentric_position['latitude'] * np.pi/180) * np.sin(true_obliquity * np.pi/180))
    argument_denominator = np.cos(apparent_sun_longitude * np.pi/180);

    sun_rigth_ascension = np.arctan2(argument_numerator, argument_denominator) * 180/np.pi
    # Limit the range to [0,360];
    sun_rigth_ascension = set_to_range(sun_rigth_ascension, 0, 360)
    return sun_rigth_ascension

def sun_geocentric_declination_calculation(apparent_sun_longitude, true_obliquity, sun_geocentric_position):
    """

    :param apparent_sun_longitude:
    :param true_obliquity:
    :param sun_geocentric_position:
    :return:
    """

    argument = (np.sin(sun_geocentric_position['latitude'] * np.pi/180) * np.cos(true_obliquity * np.pi/180)) + \
        (np.cos(sun_geocentric_position['latitude'] * np.pi/180) * np.sin(true_obliquity * np.pi/180) *
         np.sin(apparent_sun_longitude * np.pi/180))

    sun_geocentric_declination = np.arcsin(argument) * 180/np.pi
    return sun_geocentric_declination

def observer_local_hour_calculation(apparent_stime_at_greenwich, location, sun_rigth_ascension):
    """
    This function computes observer local hour.

    :param apparent_stime_at_greenwich:
    :param location:
    :param sun_rigth_ascension:
    :return:
    """

    observer_local_hour = apparent_stime_at_greenwich + location['longitude'] - sun_rigth_ascension
    # Set the range to [0-360]
    observer_local_hour = set_to_range(observer_local_hour, 0, 360)
    return observer_local_hour

def topocentric_sun_position_calculate(earth_heliocentric_position, location,
                                       observer_local_hour, sun_rigth_ascension, sun_geocentric_declination):
    """
    This function compute the sun position (rigth ascension and declination)
    with respect to the observer local position at the Earth surface.

    :param earth_heliocentric_position:
    :param location:
    :param observer_local_hour:
    :param sun_rigth_ascension:
    :param sun_geocentric_declination:
    :return:
    """

    # Equatorial horizontal parallax of the sun in degrees
    eq_horizontal_parallax = 8.794 / (3600 * earth_heliocentric_position['radius'])

    # Term u, used in the following calculations (in radians)
    u = np.arctan(0.99664719 * np.tan(location['latitude'] * np.pi/180))

    # Term x, used in the following calculations
    x = np.cos(u) + ((location['altitude']/6378140) * np.cos(location['latitude'] * np.pi/180))

    # Term y, used in the following calculations
    y = (0.99664719 * np.sin(u)) + ((location['altitude']/6378140) * np.sin(location['latitude'] * np.pi/180))

    # Parallax in the sun rigth ascension (in radians)
    nominator = -x * np.sin(eq_horizontal_parallax * np.pi/180) * np.sin(observer_local_hour * np.pi/180)
    denominator = np.cos(sun_geocentric_declination * np.pi/180) - (x * np.sin(eq_horizontal_parallax * np.pi/180) *
                                                                    np.cos(observer_local_hour * np.pi/180))
    sun_rigth_ascension_parallax = np.arctan2(nominator, denominator)
    # Conversion to degrees.
    topocentric_sun_position = dict()
    topocentric_sun_position['rigth_ascension_parallax'] = sun_rigth_ascension_parallax * 180/np.pi

    # Topocentric sun rigth ascension (in degrees)
    topocentric_sun_position['rigth_ascension'] = sun_rigth_ascension + (sun_rigth_ascension_parallax * 180/np.pi)

    # Topocentric sun declination (in degrees)
    nominator = (np.sin(sun_geocentric_declination * np.pi/180) - (y*np.sin(eq_horizontal_parallax * np.pi/180))) * \
                np.cos(sun_rigth_ascension_parallax)
    denominator = np.cos(sun_geocentric_declination * np.pi/180) - (y*np.sin(eq_horizontal_parallax * np.pi/180)) * \
                                                                   np.cos(observer_local_hour * np.pi/180)
    topocentric_sun_position['declination'] = np.arctan2(nominator, denominator) * 180/np.pi
    return topocentric_sun_position

def topocentric_local_hour_calculate(observer_local_hour, topocentric_sun_position):
    """
    This function compute the topocentric local jour angle in degrees

    :param observer_local_hour:
    :param topocentric_sun_position:
    :return:
    """

    topocentric_local_hour = observer_local_hour - topocentric_sun_position['rigth_ascension_parallax']
    return topocentric_local_hour

def sun_topocentric_zenith_angle_calculate(location, topocentric_sun_position, topocentric_local_hour):
    """
    This function compute the sun zenith angle, taking into account the
    atmospheric refraction. A default temperature of 283K and a
    default pressure of 1010 mbar are used.

    :param location:
    :param topocentric_sun_position:
    :param topocentric_local_hour:
    :return:
    """

    # Topocentric elevation, without atmospheric refraction
    argument = (np.sin(location['latitude'] * np.pi/180) * np.sin(topocentric_sun_position['declination'] * np.pi/180)) + \
    (np.cos(location['latitude'] * np.pi/180) * np.cos(topocentric_sun_position['declination'] * np.pi/180) *
     np.cos(topocentric_local_hour * np.pi/180))
    true_elevation = np.arcsin(argument) * 180/np.pi

    # Atmospheric refraction correction (in degrees)
    argument = true_elevation + (10.3/(true_elevation + 5.11))
    refraction_corr = 1.02 / (60 * np.tan(argument * np.pi/180))

    # For exact pressure and temperature correction, use this,
    # with P the pressure in mbar amd T the temperature in Kelvins:
    # refraction_corr = (P/1010) * (283/T) * 1.02 / (60 * tan(argument * pi/180));

    # Apparent elevation
    apparent_elevation = true_elevation + refraction_corr

    sun = dict()
    sun['zenith'] = 90 - apparent_elevation

    # Topocentric azimuth angle. The +180 conversion is to pass from astronomer
    # notation (westward from south) to navigation notation (eastward from
    # north);
    nominator = np.sin(topocentric_local_hour * np.pi/180)
    denominator = (np.cos(topocentric_local_hour * np.pi/180) * np.sin(location['latitude'] * np.pi/180)) - \
    (np.tan(topocentric_sun_position['declination'] * np.pi/180) * np.cos(location['latitude'] * np.pi/180))
    sun['azimuth'] = (np.arctan2(nominator, denominator) * 180/np.pi) + 180

    # Set the range to [0-360]
    sun['azimuth'] = set_to_range(sun['azimuth'], 0, 360)
    return sun

def set_to_range(var, min_interval, max_interval):
    """
    Sets a variable in range min_interval and max_interval

    :param var:
    :param min_interval:
    :param max_interval:
    :return:
    """
    var = var - max_interval * np.floor(var/max_interval)

    if var < min_interval:
        var = var + max_interval
    return var

def day_of_year(yyyy, month, day):
    if (yyyy % 4) == 0:
        if (yyyy % 100) == 0:
            if (yyyy % 400) == 0:
                leapyear = 1
            else:
                leapyear = 0
        else:
            leapyear = 1
    else:
        leapyear = 0
    
    if leapyear == 1:
        dayspermonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    else:
        dayspermonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    doy = np.sum(dayspermonth[0:month - 1]) + day
    
    return doy

def day_of_year(yyyy, month, day):
    if (yyyy % 4) == 0:
        if (yyyy % 100) == 0:
            if (yyyy % 400) == 0:
                leapyear = 1
            else:
                leapyear = 0
        else:
            leapyear = 1
    else:
        leapyear = 0
    
    if leapyear == 1:
        dayspermonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    else:
        dayspermonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    doy = np.sum(dayspermonth[0:month - 1]) + day
    
    return doy
    
def annulus_weight(altitude, aziinterval):
    '''assign different weight to different annulus'''
    n = 90.
    steprad = (360./aziinterval) * (np.pi/180.)
    annulus = 91.-altitude
    w = (1./(2.*np.pi)) * np.sin(np.pi / (2.*n)) * np.sin((np.pi * (2. * annulus - 1.)) / (2. * n))
    weight = steprad * w
    
    return weight
    
# def shadowingfunctionglobalradiation(a, azimuth, altitude, scale, dlg, forsvf):
def shadowingfunctionglobalradiation(a, azimuth, altitude, scale, forsvf):
    import numpy as np
    
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
    
    return sh

# def shadowingfunction_20(a, vegdem, vegdem2, azimuth, altitude, scale, amaxvalue, bush, dlg, forsvf):
def shadowingfunction_20(a, vegdem, vegdem2, azimuth, altitude, scale, amaxvalue, bush, forsvf):
    #% This function casts shadows on buildings and vegetation units
    #% conversion
    
    import numpy as np
    
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
#         dlg.progressBar.setRange(0, barstep)
#         dlg.progressBar.setValue(0)
    
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
        vegsh2 = np.subtract(fabovea, gabovea, dtype=np.float)
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
    
    return shadowresult

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
    # outBand.SetNoDataValue(-9999)
    
    # georeference the image and set the projection
    outDs.SetGeoTransform(gdal_data.GetGeoTransform())
    outDs.SetProjection(gdal_data.GetProjection())

def cart2pol(x, y, units='deg'):
    radius = np.sqrt(x**2 + y**2)
    theta = np.arctan2(y, x)
    if units in ['deg', 'degs']:
        theta = theta * 180 / np.pi
    return theta, radius

def saturated_vapor_pressure_hpa(db_temp):
    """Calculate saturated vapor pressure (hPa) at temperature (C).

    This equation of saturation vapor pressure is specific to the UTCI model.
    """
    g = (-2836.5744, -6028.076559, 19.54263612, -0.02737830188, 0.000016261698,
         7.0229056e-10, -1.8680009e-13)
    tk = db_temp + 273.15  # air temp in K
    es = 2.7150305 * math.log(tk)
    for i, x in enumerate(g):
        es = es + (x * (tk**(i - 2)))
    es = math.exp(es) * 0.01
    return es

def reclassifyLU2Albedo(lcgrid):
    '''the albedos of different land use types classification system
    is differet from the land use/cover system, therefore, change the
    land use/cover classification system to the albedo classification system

    The SOLWEIG classification sytem
        1 Paved Paved surfaces (e.g. roads, car parks)
        2 Buildings - Building surfaces
        3 Evergreen Trees - Evergreen trees and shrubs
        4 Deciduous Trees - Deciduous trees and shrubs
        5 Grass - Grass surfaces
        6 Bare soil - Bare soil surfaces and unmanaged land
        7 Water - Open water (e.g. lakes, ponds, rivers, fountain)

    In the solweig model, there should have no 3/4 in the land use classification system, 
    because there is no albedo paramters in the lookup table, only use the Ground cover 
    information (underneath canopy) is required. Therefore, for the tree canopy, we can 
    reclassify to other land use, for the deciduous, maybe use grass, for evergreen use bare soil.
    Here I only use the grass. 
    '''

    ## the 1 for pavement is missing here

    lcgrid = np.round(lcgrid)
    lc_grid = np.zeros(lcgrid.shape)
    lc_grid[np.logical_or(lcgrid == 5, lcgrid == 6)] = 2 #building and road are all asigned to building albeod
    lc_grid[lcgrid == 4] = 7 #water, only consider water, not fresh, ocean
    lc_grid[lcgrid == 3] = 6 #brown field
    lc_grid[(np.logical_or(lcgrid == 2, lcgrid == 1))] = 5 #grass and forest both to grass
    
    return lc_grid

def prepareAlbedo():
    lin = ['Name              Code Alb  Emis Ts_deg Tstart TmaxLST \n',
            'Roofs(buildings)   2   0.18 0.95 0.58   -9.78  15.0\n',
            'Dark_asphalt       1   0.18 0.95 0.58   -9.78  15.0\n',
            'Cobble_stone_2014a 0   0.20 0.95 0.37   -3.41  15.0\n',
            'Water              7   0.05 0.98 0.00    0.00  12.0\n',
            'Grass_unmanaged    5   0.16 0.94 0.21   -3.38  14.0\n',
            'bare_soil          6   0.25 0.94 0.33   -3.01  14.0\n',
            'Walls             99   0.20 0.90 0.58   -3.41  15.0']
    lc_class = np.zeros((lin.__len__() - 1, 6))
    for i in range(1, lin.__len__()):
        lines = lin[i].split()
        for j in np.arange(1, 7):
            lc_class[i - 1, j - 1] = float(lines[j])

    return lc_class

def transform_prep(epsgcode):
    '''
    based on the input of th epsgcode and prepare the geom
    transform
    '''
    
    # the input raster dsm of local pennsylvania, 2272
    srcSRS = osr.SpatialReference()
    # srcSRS.ImportFromEPSG(26918) #26918, 2272
    srcSRS.ImportFromEPSG(epsgcode)

    wgs84_wkt = """
        GEOGCS["WGS 84",
            DATUM["WGS_1984",
                SPHEROID["WGS 84",6378137,298.257223563,
                    AUTHORITY["EPSG","7030"]],
                AUTHORITY["EPSG","6326"]],
            PRIMEM["Greenwich",0,
                AUTHORITY["EPSG","8901"]],
            UNIT["degree",0.01745329251994328,
                AUTHORITY["EPSG","9122"]],
            AUTHORITY["EPSG","4326"]]"""

    new_cs = osr.SpatialReference()
    new_cs.ImportFromWkt(wgs84_wkt)

    transform = osr.CoordinateTransformation(srcSRS, new_cs)

    return transform

def prepareData(mrtfolder, svffolder, dsmfile, chmfile, wallfile, aspectfile, epsgcode):
    # if the folder already exist, then skip or create folder and start the computing
    # if os.path.exists(mrtfolder) and os.path.exists(utcifolder): 
    #     continue
    # else:

    if not os.path.exists(mrtfolder):
        os.mkdir(mrtfolder)

    # Read the dem and dsm file to np arrays and calculate the scale using gdal
    gdal_dsm = gdal.Open(dsmfile)
    dsm = gdal_dsm.ReadAsArray().astype(float)
    geotransform = gdal_dsm.GetGeoTransform()
    scale = 1 / geotransform[1]

    rows = dsm.shape[0]
    cols = dsm.shape[1]

    alt = np.median(dsm)
    if alt < 0:
        alt = 3

    # response to issue #85
    nd = gdal_dsm.GetRasterBand(1).GetNoDataValue()
    dsm[dsm == nd] = 0.
    if dsm.min() < 0:
        dsm = dsm + np.abs(dsm.min())

    ## get the transform
    transform = transform_prep(epsgcode)

    width = gdal_dsm.RasterXSize
    height = gdal_dsm.RasterYSize
    minx = geotransform[0]
    miny = geotransform[3] + width * geotransform[4] + height * geotransform[5]
    lonlat = transform.TransformPoint(minx, miny)

    print('The lon lat are:', lonlat)

    gdalver = float(gdal.__version__[0])
    if gdalver == 3.:
        lon = lonlat[1] #changed to gdal 3
        lat = lonlat[0] #changed to gdal 3
    else:
        lon = lonlat[0] #changed to gdal 2
        lat = lonlat[1] #changed to gdal 2
    
    ## use the vegetation canopy height model
    trans = 0.03
    dataSet = gdal.Open(chmfile)
    vegdsm = dataSet.ReadAsArray().astype(float)

    # use trunk ratio of 0.25 to deal with the trunk
    trunkratio = 25 / 100.0
    vegdsm2 = vegdsm * trunkratio

    # read the previously created svfs
    svfs = ['svf', 'svfN', 'svfS', 'svfE', 'svfW', 'svfveg', \
            'svfEveg', 'svfSveg', 'svfWveg', 'svfNveg', 'svfaveg', \
            'svfEaveg', 'svfSaveg', 'svfWaveg', 'svfNaveg']

    svf_imgs_dict = {}

    try:
        for svf in svfs:
            svffile = os.path.join(svffolder, svf + '.tif')
            dataSet = gdal.Open(svffile)
            svf_img = dataSet.ReadAsArray().astype(float)
            svf_imgs_dict[svf] = svf_img
    except:
        print('The svf folder %s is not existed'%(svffolder))
    #     continue

    svf = svf_imgs_dict['svf']
    svfN = svf_imgs_dict['svfN']
    svfS = svf_imgs_dict['svfS']
    svfE = svf_imgs_dict['svfE']
    svfW = svf_imgs_dict['svfW']

    svfveg = svf_imgs_dict['svfveg']
    svfNveg = svf_imgs_dict['svfNveg']
    svfSveg = svf_imgs_dict['svfSveg']
    svfEveg = svf_imgs_dict['svfEveg']
    svfWveg = svf_imgs_dict['svfWveg']

    svfaveg = svf_imgs_dict['svfaveg']
    svfNaveg = svf_imgs_dict['svfNaveg']
    svfSaveg = svf_imgs_dict['svfSaveg']
    svfEaveg = svf_imgs_dict['svfEaveg']
    svfWaveg = svf_imgs_dict['svfWaveg']
    
    tmp = svf + svfveg - 1.
    tmp[tmp < 0.] = 0.

    # %matlab crazyness around 0
    epsilon = 1e-10  # A small value to avoid log(0)
    tmp = np.clip(tmp, 0, 1 - epsilon)
    # svfalfa = np.arcsin(np.exp((np.log((1. - tmp + 0.00000001)) / 2.))) ##add + 0.0000001, you can remove it
    svfalfa = np.arcsin(np.exp((np.log((1. - tmp)) / 2.))) ##add + 0.0000001, you can remove it

    # read the wall height and aspect
    gdal_wall = gdal.Open(wallfile)
    wallheight = gdal_wall.ReadAsArray().astype(float)

    gdal_aspect = gdal.Open(aspectfile)
    wallaspect = gdal_aspect.ReadAsArray().astype(float)

    # amaxvalue
    vegmax = vegdsm.max()
    amaxvalue = dsm.max() - dsm.min()
    amaxvalue = np.maximum(amaxvalue, vegmax)

    # Elevation vegdsms if buildingDEM includes ground heights
    vegdsm = vegdsm + dsm
    vegdsm[vegdsm == dsm] = 0
    vegdsm2 = vegdsm2 + dsm
    vegdsm2[vegdsm2 == dsm] = 0

    # % Bush separation
    bush = np.logical_not((vegdsm2 * vegdsm)) * vegdsm
    svfbuveg = (svf - (1. - svfveg) * (1. - trans))  # % major bug fixed 20141203

    return lon, lat, scale, rows, cols, alt, dsm, svf, svfN, svfS, svfE, svfW, svfveg, svfNveg, \
        svfSveg, svfEveg, svfWveg, svfaveg, svfNaveg, svfSaveg, svfEaveg,\
        svfWaveg, svfalfa, wallheight, wallaspect, amaxvalue, vegdsm, \
        vegdsm2, bush, svfbuveg, gdal_dsm


def metdataParse(row):
    '''parse the metadata, make the code cleaner'''
    metdata = np.zeros((1, 24)) - 999.
    
    # date information
    year = int(row['Year'])
    month = int(row['Month'])
    day = int(row['Day'])
    hour = int(row['Hour'])
    minu = int(row['Minute'])
    doy = day_of_year(year, month, day)
    
    # meteorological data
    print('The year, month, day, hour, minute are:', year, month, day, hour, minu)
    Ta = float(row['Temperature']) # air temperature
    RH = float(row['Relative Humidity']) # relative humidity
    # radG = float(row['GHI']) # global radiation
    # radD = float(row['DHI']) #diffuse radiation, 'Clearsky DHI'
    # radI = float(row['DNI']) # direct radiation, 'Clearsky DNI'

    radG = float(row['Clearsky GHI']) # global radiation
    radD = float(row['Clearsky DHI']) #diffuse radiation, 'Clearsky DHI'
    radI = float(row['Clearsky DNI']) # direct radiation, 'Clearsky DNI'


    Ws = float(row['Wind Speed']) # wind speed
    
    metdata[0, 0] = year
    metdata[0, 1] = doy
    metdata[0, 2] = hour
    metdata[0, 3] = minu
    metdata[0, 11] = Ta
    metdata[0, 10] = RH
    metdata[0, 14] = radG
    metdata[0, 21] = radD
    metdata[0, 22] = radI
    metdata[0, 9] = Ws
    
    return metdata, year, month, day, hour, minu, doy

def otherParameters():
    ## other parameters
    absK = 0.70 # absorption of shortwave radiation
    absL = 0.95 # absorption of longwave radiation
    pos = 0 # posture of the body, standing-sitting

    # consider human as cylinder instead of box
    # if self.dlg.CheckBoxBox.isChecked():
    #     cyl = 1
    # else:
    #     cyl = 0
    cyl = 1

    if pos == 0:
        Fside = 0.22
        Fup = 0.06
        height = 1.1
        Fcyl = 0.28
    else:
        Fside = 0.166666
        Fup = 0.166666
        height = 0.75
        Fcyl = 0.2

    elvis = 0 

    timeadd = 0.
    timeaddE = 0.
    timeaddS = 0.
    timeaddW = 0.
    timeaddN = 0.
    firstdaytime = 1.

    return absK, absL, pos, cyl, Fside, Fup, height, Fcyl, elvis, \
        timeadd, timeaddE, timeaddS, timeaddW, timeaddN, firstdaytime

def landcoverAlbedo(root, base, Knight, albedo_g, eground, lc_class, landcover):
    '''
    landcover: 0: don't use the land cover data, 1: use the land cover data
    
    lc_class:
        lin = ['Name              Code Alb  Emis Ts_deg Tstart TmaxLST \n',
            'Roofs(buildings)   2   0.18 0.95 0.58   -9.78  15.0\n',
            'Dark_asphalt       1   0.18 0.95 0.58   -9.78  15.0\n',
            'Cobble_stone_2014a 0   0.20 0.95 0.37   -3.41  15.0\n',
            'Water              7   0.05 0.98 0.00    0.00  12.0\n',
            'Grass_unmanaged    5   0.16 0.94 0.21   -3.38  14.0\n',
            'bare_soil          6   0.25 0.94 0.33   -3.01  14.0\n',
            'Walls             99   0.20 0.90 0.58   -3.41  15.0']
    The albedo of the tree canopy is not considered. This is because there is no need to consider
    the albedo of trees, since they would block the solar radiation from reaching the ground. In 
    this case, the tree canopy should be reclassified as the grass, beneath the tree
    '''

    #### ------------land cover and albedo------------------
    # don't use the land cover
    # landcover = 0

    # demforbuild = 1
    # filePath_lc = None
    lufile = os.path.join(root, 'lcgrid/' + base + '.tif')
    albedo_file = os.path.join(root, 'albedo/' + base + '.tif')
    
    if os.path.exists(lufile) and landcover != 0:
        dataSet = gdal.Open(lufile)
        lc_grid = dataSet.ReadAsArray().astype(float)
        
        #this is the previous lu for Phily
        lcgrid = dataSet.ReadAsArray().astype(float)
        lc_grid = reclassifyLU2Albedo(lcgrid)

        # del lcgrid
        [TgK, Tstart, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall] = Tgmaps_v1(lc_grid, lc_class)
        
        ### here is weired, if the albedo of impervious surface large and set grass as 0.15, the 
        ### tmrt of the grass is even larger than the building roof. Need to be checked in future.
        # # just use the generated albedo database
        albedoDataset = gdal.Open(albedo_file)
        alb_grid = albedoDataset.ReadAsArray().astype(float)
        
    else:
        lc_grid = None
        ## when you don't consider the land cover, use this,skip this if lu is considered
        TgK = Knight + 0.37
        Tstart = Knight - 3.41
        alb_grid = Knight + albedo_g
        emis_grid = Knight + eground
        TgK_wall = 0.37
        Tstart_wall = -3.41
        TmaxLST = 15.
        TmaxLST_wall = 15.
    return TgK, Tstart, lc_grid, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall


def landcoverAlbedoNew(lufile, albedo_file, Knight, albedo_g, eground, lc_class, landcover):
    '''
    This is the new version, use the simple land use classification
    landcover: 0: don't use the land cover data, 1: use the land cover data
    
    lc_class:
        lin = ['Name              Code Alb  Emis Ts_deg Tstart TmaxLST \n',
            'Roofs(buildings)   2   0.18 0.95 0.58   -9.78  15.0\n',
            'Dark_asphalt       1   0.18 0.95 0.58   -9.78  15.0\n',
            'Cobble_stone_2014a 0   0.20 0.95 0.37   -3.41  15.0\n',
            'Water              7   0.05 0.98 0.00    0.00  12.0\n',
            'Grass_unmanaged    5   0.16 0.94 0.21   -3.38  14.0\n',
            'bare_soil          6   0.25 0.94 0.33   -3.01  14.0\n',
            'Walls             99   0.20 0.90 0.58   -3.41  15.0']
    The albedo of the tree canopy is not considered. This is because there is no need to consider
    the albedo of trees, since they would block the solar radiation from reaching the ground. In 
    this case, the tree canopy should be reclassified as the grass, beneath the tree
    '''

    #### ------------land cover and albedo------------------
    # don't use the land cover
    # landcover = 0
    
    # demforbuild = 1
    # filePath_lc = None
    # lufile = os.path.join(root, 'landcover_tiles', city, base + '.tif')
    # albedo_file = os.path.join(root, 'albedo_tiles', city, base + '.tif')
    
    if os.path.exists(lufile) and landcover != 0:
        dataSet = gdal.Open(lufile)
        lc_grid = dataSet.ReadAsArray().astype(float)
        
        # #this is the previous lu for Phily
        # lcgrid = dataSet.ReadAsArray().astype(float)
        # lc_grid = reclassifyLU2Albedo(lcgrid)

        # del lcgrid
        [TgK, Tstart, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall] = Tgmaps_v1(lc_grid, lc_class)
        
        ### here is weired, if the albedo of impervious surface large and set grass as 0.15, the 
        ### tmrt of the grass is even larger than the building roof. Need to be checked in future.
        # # just use the generated albedo database
        albedoDataset = gdal.Open(albedo_file)
        alb_grid = albedoDataset.ReadAsArray().astype(float)
        
    else:
        lc_grid = None
        ## when you don't consider the land cover, use this,skip this if lu is considered
        TgK = Knight + 0.37
        Tstart = Knight - 3.41
        alb_grid = Knight + albedo_g
        emis_grid = Knight + eground
        TgK_wall = 0.37
        Tstart_wall = -3.41
        TmaxLST = 15.
        TmaxLST_wall = 15.
    return TgK, Tstart, lc_grid, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall



def Tgmaps_v1(lc_grid, lc_class):
    #Tgmaps_v1 Populates grids with cooeficients for Tg wave
    #   Detailed explanation goes here
    # lc_grid: the land use/cover, a standard encoding of solweig model
    # lc_class: the albedo metrics of different land use

    id = np.unique(np.round(lc_grid))
    TgK = np.copy(lc_grid)
    Tstart = np.copy(lc_grid)
    alb_grid = np.copy(lc_grid)
    emis_grid = np.copy(lc_grid)
    TmaxLST = np.copy(lc_grid)

    for i in np.arange(0, id.__len__()):
        row = (lc_class[:, 0] == id[i])
        Tstart[Tstart == id[i]] = lc_class[row, 4]
        alb_grid[alb_grid == id[i]] = lc_class[row, 1]
        emis_grid[emis_grid == id[i]] = lc_class[row, 2]
        TmaxLST[TmaxLST == id[i]] = lc_class[row, 5]
        TgK[TgK == id[i]] = lc_class[row, 3]
    
    wall_pos = np.where(lc_class[:, 0] == 99)
    TgK_wall = lc_class[wall_pos, 3]
    Tstart_wall = lc_class[wall_pos, 4]
    TmaxLST_wall = lc_class[wall_pos, 5]
    
    return TgK, Tstart, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall

def universal_thermal_climate_index(ta, tr, vel, rh):
    """Calculate Universal Thermal Climate Index (UTCI) using a polynomial approximation.
    
    UTCI is an international standard for outdoor temperature sensation
    (aka. "feels-like" temperature) that attempts to fill the
    following requirements:
    
    1) Thermo-physiological significance in the whole range of heat
       exchange conditions of existing thermal environments
    2) Valid in all climates, seasons, and scales
    3) Useful for key applications in human biometeorology.

    This function here is a Python version of the original UTCI_approx
    application written in Fortran. Version a 0.002, October 2009
    The original Fortran code can be found at www.utci.org.
    
    Note:
        [1] Peter Bröde, Dusan Fiala, Krzysztof Blazejczyk, Yoram Epstein,
        Ingvar Holmér, Gerd Jendritzky, Bernhard Kampmann, Mark Richards,
        Hannu Rintamäki, Avraham Shitzer, George Havenith. 2009.
        Calculating UTCI Equivalent Temperature. In: JW Castellani & TL
        Endrusick, eds. Proceedings of the 13th International Conference
        on Environmental Ergonomics, USARIEM, Natick, MA.

    Args:
        ta: Air temperature [C]
        tr: Mean radiant temperature [C]
        vel: Wind speed 10 m above ground level [m/s].
            Note that this meteorological speed at 10 m is simply 1.5 times the
            speed felt at ground in the original Fiala model used to build UTCI.
        rh: Relative humidity [%]

    Returns:
        UTCI_approx -- The Universal Thermal Climate Index (UTCI) for the input
        conditions as approximated by a 4-D polynomial.
    
    
    Modified to calculte the UTCI distribution based on input MRT img
    The spatial distribution of wind speed may need CFD simulation, here use uniform
    The relative humidity is uniform as well, future version may considere the spatial
    variations of the relative humidity and wind speed.
    last modified by Xiaojiang Li, Temple University on Mar 13, 2021
    """
    
    # set upper and lower limits of air velocity according to Fiala model scenarios
    vel = 0.5 if vel < 0.5 else vel
    vel = 17 if vel > 17 else vel
    
    
    # metrics derived from the inputs used in the polynomial equation
    eh_pa = saturated_vapor_pressure_hpa(ta) * (rh / 100.0)  # partial vapor pressure
    pa_pr = eh_pa / 10.0  # convert vapour pressure to kPa
    d_tr = tr - ta  # difference between radiant and air temperature


    utci_approx = ta + \
        0.607562052 + \
        -0.0227712343 * ta + \
        8.06470249e-4 * ta * ta + \
        -1.54271372e-4 * ta * ta * ta + \
        -3.24651735e-6 * ta * ta * ta * ta + \
        7.32602852e-8 * ta * ta * ta * ta * ta + \
        1.35959073e-9 * ta * ta * ta * ta * ta * ta + \
        -2.25836520 * vel + \
        0.0880326035 * ta * vel + \
        0.00216844454 * ta * ta * vel + \
        -1.53347087e-5 * ta * ta * ta * vel + \
        -5.72983704e-7 * ta * ta * ta * ta * vel + \
        -2.55090145e-9 * ta * ta * ta * ta * ta * vel + \
        -0.751269505 * vel * vel + \
        -0.00408350271 * ta * vel * vel + \
        -5.21670675e-5 * ta * ta * vel * vel + \
        1.94544667e-6 * ta * ta * ta * vel * vel + \
        1.14099531e-8 * ta * ta * ta * ta * vel * vel + \
        0.158137256 * vel * vel * vel + \
        -6.57263143e-5 * ta * vel * vel * vel + \
        2.22697524e-7 * ta * ta * vel * vel * vel + \
        -4.16117031e-8 * ta * ta * ta * vel * vel * vel + \
        -0.0127762753 * vel * vel * vel * vel + \
        9.66891875e-6 * ta * vel * vel * vel * vel + \
        2.52785852e-9 * ta * ta * vel * vel * vel * vel + \
        4.56306672e-4 * vel * vel * vel * vel * vel + \
        -1.74202546e-7 * ta * vel * vel * vel * vel * vel + \
        -5.91491269e-6 * vel * vel * vel * vel * vel * vel + \
        0.398374029 * d_tr + \
        1.83945314e-4 * ta * d_tr + \
        -1.73754510e-4 * ta * ta * d_tr + \
        -7.60781159e-7 * ta * ta * ta * d_tr + \
        3.77830287e-8 * ta * ta * ta * ta * d_tr + \
        5.43079673e-10 * ta * ta * ta * ta * ta * d_tr + \
        -0.0200518269 * vel * d_tr + \
        8.92859837e-4 * ta * vel * d_tr + \
        3.45433048e-6 * ta * ta * vel * d_tr + \
        -3.77925774e-7 * ta * ta * ta * vel * d_tr + \
        -1.69699377e-9 * ta * ta * ta * ta * vel * d_tr + \
        1.69992415e-4 * vel * vel * d_tr + \
        -4.99204314e-5 * ta * vel * vel * d_tr + \
        2.47417178e-7 * ta * ta * vel * vel * d_tr + \
        1.07596466e-8 * ta * ta * ta * vel * vel * d_tr + \
        8.49242932e-5 * vel * vel * vel * d_tr + \
        1.35191328e-6 * ta * vel * vel * vel * d_tr + \
        -6.21531254e-9 * ta * ta * vel * vel * vel * d_tr + \
        -4.99410301e-6 * vel * vel * vel * vel * d_tr + \
        -1.89489258e-8 * ta * vel * vel * vel * vel * d_tr + \
        8.15300114e-8 * vel * vel * vel * vel * vel * d_tr + \
        7.55043090e-4 * d_tr * d_tr + \
        -5.65095215e-5 * ta * d_tr * d_tr + \
        -4.52166564e-7 * ta * ta * d_tr * d_tr + \
        2.46688878e-8 * ta * ta * ta * d_tr * d_tr + \
        2.42674348e-10 * ta * ta * ta * ta * d_tr * d_tr + \
        1.54547250e-4 * vel * d_tr * d_tr + \
        5.24110970e-6 * ta * vel * d_tr * d_tr + \
        -8.75874982e-8 * ta * ta * vel * d_tr * d_tr + \
        -1.50743064e-9 * ta * ta * ta * vel * d_tr * d_tr + \
        -1.56236307e-5 * vel * vel * d_tr * d_tr + \
        -1.33895614e-7 * ta * vel * vel * d_tr * d_tr + \
        2.49709824e-9 * ta * ta * vel * vel * d_tr * d_tr + \
        6.51711721e-7 * vel * vel * vel * d_tr * d_tr + \
        1.94960053e-9 * ta * vel * vel * vel * d_tr * d_tr + \
        -1.00361113e-8 * vel * vel * vel * vel * d_tr * d_tr + \
        -1.21206673e-5 * d_tr * d_tr * d_tr + \
        -2.18203660e-7 * ta * d_tr * d_tr * d_tr + \
        7.51269482e-9 * ta * ta * d_tr * d_tr * d_tr + \
        9.79063848e-11 * ta * ta * ta * d_tr * d_tr * d_tr + \
        1.25006734e-6 * vel * d_tr * d_tr * d_tr + \
        -1.81584736e-9 * ta * vel * d_tr * d_tr * d_tr + \
        -3.52197671e-10 * ta * ta * vel * d_tr * d_tr * d_tr + \
        -3.36514630e-8 * vel * vel * d_tr * d_tr * d_tr + \
        1.35908359e-10 * ta * vel * vel * d_tr * d_tr * d_tr + \
        4.17032620e-10 * vel * vel * vel * d_tr * d_tr * d_tr + \
        -1.30369025e-9 * d_tr * d_tr * d_tr * d_tr + \
        4.13908461e-10 * ta * d_tr * d_tr * d_tr * d_tr + \
        9.22652254e-12 * ta * ta * d_tr * d_tr * d_tr * d_tr + \
        -5.08220384e-9 * vel * d_tr * d_tr * d_tr * d_tr + \
        -2.24730961e-11 * ta * vel * d_tr * d_tr * d_tr * d_tr + \
        1.17139133e-10 * vel * vel * d_tr * d_tr * d_tr * d_tr + \
        6.62154879e-10 * d_tr * d_tr * d_tr * d_tr * d_tr + \
        4.03863260e-13 * ta * d_tr * d_tr * d_tr * d_tr * d_tr + \
        1.95087203e-12 * vel * d_tr * d_tr * d_tr * d_tr * d_tr + \
        -4.73602469e-12 * d_tr * d_tr * d_tr * d_tr * d_tr * d_tr + \
        5.12733497 * pa_pr + \
        -0.312788561 * ta * pa_pr + \
        -0.0196701861 * ta * ta * pa_pr + \
        9.99690870e-4 * ta * ta * ta * pa_pr + \
        9.51738512e-6 * ta * ta * ta * ta * pa_pr + \
        -4.66426341e-7 * ta * ta * ta * ta * ta * pa_pr + \
        0.548050612 * vel * pa_pr + \
        -0.00330552823 * ta * vel * pa_pr + \
        -0.00164119440 * ta * ta * vel * pa_pr + \
        -5.16670694e-6 * ta * ta * ta * vel * pa_pr + \
        9.52692432e-7 * ta * ta * ta * ta * vel * pa_pr + \
        -0.0429223622 * vel * vel * pa_pr + \
        0.00500845667 * ta * vel * vel * pa_pr + \
        1.00601257e-6 * ta * ta * vel * vel * pa_pr + \
        -1.81748644e-6 * ta * ta * ta * vel * vel * pa_pr + \
        -1.25813502e-3 * vel * vel * vel * pa_pr + \
        -1.79330391e-4 * ta * vel * vel * vel * pa_pr + \
        2.34994441e-6 * ta * ta * vel * vel * vel * pa_pr + \
        1.29735808e-4 * vel * vel * vel * vel * pa_pr + \
        1.29064870e-6 * ta * vel * vel * vel * vel * pa_pr + \
        -2.28558686e-6 * vel * vel * vel * vel * vel * pa_pr + \
        -0.0369476348 * d_tr * pa_pr + \
        0.00162325322 * ta * d_tr * pa_pr + \
        -3.14279680e-5 * ta * ta * d_tr * pa_pr + \
        2.59835559e-6 * ta * ta * ta * d_tr * pa_pr + \
        -4.77136523e-8 * ta * ta * ta * ta * d_tr * pa_pr + \
        8.64203390e-3 * vel * d_tr * pa_pr + \
        -6.87405181e-4 * ta * vel * d_tr * pa_pr + \
        -9.13863872e-6 * ta * ta * vel * d_tr * pa_pr + \
        5.15916806e-7 * ta * ta * ta * vel * d_tr * pa_pr + \
        -3.59217476e-5 * vel * vel * d_tr * pa_pr + \
        3.28696511e-5 * ta * vel * vel * d_tr * pa_pr + \
        -7.10542454e-7 * ta * ta * vel * vel * d_tr * pa_pr + \
        -1.24382300e-5 * vel * vel * vel * d_tr * pa_pr + \
        -7.38584400e-9 * ta * vel * vel * vel * d_tr * pa_pr + \
        2.20609296e-7 * vel * vel * vel * vel * d_tr * pa_pr + \
        -7.32469180e-4 * d_tr * d_tr * pa_pr + \
        -1.87381964e-5 * ta * d_tr * d_tr * pa_pr + \
        4.80925239e-6 * ta * ta * d_tr * d_tr * pa_pr + \
        -8.75492040e-8 * ta * ta * ta * d_tr * d_tr * pa_pr + \
        2.77862930e-5 * vel * d_tr * d_tr * pa_pr + \
        -5.06004592e-6 * ta * vel * d_tr * d_tr * pa_pr + \
        1.14325367e-7 * ta * ta * vel * d_tr * d_tr * pa_pr + \
        2.53016723e-6 * vel * vel * d_tr * d_tr * pa_pr + \
        -1.72857035e-8 * ta * vel * vel * d_tr * d_tr * pa_pr + \
        -3.95079398e-8 * vel * vel * vel * d_tr * d_tr * pa_pr + \
        -3.59413173e-7 * d_tr * d_tr * d_tr * pa_pr + \
        7.04388046e-7 * ta * d_tr * d_tr * d_tr * pa_pr + \
        -1.89309167e-8 * ta * ta * d_tr * d_tr * d_tr * pa_pr + \
        -4.79768731e-7 * vel * d_tr * d_tr * d_tr * pa_pr + \
        7.96079978e-9 * ta * vel * d_tr * d_tr * d_tr * pa_pr + \
        1.62897058e-9 * vel * vel * d_tr * d_tr * d_tr * pa_pr + \
        3.94367674e-8 * d_tr * d_tr * d_tr * d_tr * pa_pr + \
        -1.18566247e-9 * ta * d_tr * d_tr * d_tr * d_tr * pa_pr + \
        3.34678041e-10 * vel * d_tr * d_tr * d_tr * d_tr * pa_pr + \
        -1.15606447e-10 * d_tr * d_tr * d_tr * d_tr * d_tr * pa_pr + \
        -2.80626406 * pa_pr * pa_pr + \
        0.548712484 * ta * pa_pr * pa_pr + \
        -0.00399428410 * ta * ta * pa_pr * pa_pr + \
        -9.54009191e-4 * ta * ta * ta * pa_pr * pa_pr + \
        1.93090978e-5 * ta * ta * ta * ta * pa_pr * pa_pr + \
        -0.308806365 * vel * pa_pr * pa_pr + \
        0.0116952364 * ta * vel * pa_pr * pa_pr + \
        4.95271903e-4 * ta * ta * vel * pa_pr * pa_pr + \
        -1.90710882e-5 * ta * ta * ta * vel * pa_pr * pa_pr + \
        0.00210787756 * vel * vel * pa_pr * pa_pr + \
        -6.98445738e-4 * ta * vel * vel * pa_pr * pa_pr + \
        2.30109073e-5 * ta * ta * vel * vel * pa_pr * pa_pr + \
        4.17856590e-4 * vel * vel * vel * pa_pr * pa_pr + \
        -1.27043871e-5 * ta * vel * vel * vel * pa_pr * pa_pr + \
        -3.04620472e-6 * vel * vel * vel * vel * pa_pr * pa_pr + \
        0.0514507424 * d_tr * pa_pr * pa_pr + \
        -0.00432510997 * ta * d_tr * pa_pr * pa_pr + \
        8.99281156e-5 * ta * ta * d_tr * pa_pr * pa_pr + \
        -7.14663943e-7 * ta * ta * ta * d_tr * pa_pr * pa_pr + \
        -2.66016305e-4 * vel * d_tr * pa_pr * pa_pr + \
        2.63789586e-4 * ta * vel * d_tr * pa_pr * pa_pr + \
        -7.01199003e-6 * ta * ta * vel * d_tr * pa_pr * pa_pr + \
        -1.06823306e-4 * vel * vel * d_tr * pa_pr * pa_pr + \
        3.61341136e-6 * ta * vel * vel * d_tr * pa_pr * pa_pr + \
        2.29748967e-7 * vel * vel * vel * d_tr * pa_pr * pa_pr + \
        3.04788893e-4 * d_tr * d_tr * pa_pr * pa_pr + \
        -6.42070836e-5 * ta * d_tr * d_tr * pa_pr * pa_pr + \
        1.16257971e-6 * ta * ta * d_tr * d_tr * pa_pr * pa_pr + \
        7.68023384e-6 * vel * d_tr * d_tr * pa_pr * pa_pr + \
        -5.47446896e-7 * ta * vel * d_tr * d_tr * pa_pr * pa_pr + \
        -3.59937910e-8 * vel * vel * d_tr * d_tr * pa_pr * pa_pr + \
        -4.36497725e-6 * d_tr * d_tr * d_tr * pa_pr * pa_pr + \
        1.68737969e-7 * ta * d_tr * d_tr * d_tr * pa_pr * pa_pr + \
        2.67489271e-8 * vel * d_tr * d_tr * d_tr * pa_pr * pa_pr + \
        3.23926897e-9 * d_tr * d_tr * d_tr * d_tr * pa_pr * pa_pr + \
        -0.0353874123 * pa_pr * pa_pr * pa_pr + \
        -0.221201190 * ta * pa_pr * pa_pr * pa_pr + \
        0.0155126038 * ta * ta * pa_pr * pa_pr * pa_pr + \
        -2.63917279e-4 * ta * ta * ta * pa_pr * pa_pr * pa_pr + \
        0.0453433455 * vel * pa_pr * pa_pr * pa_pr + \
        -0.00432943862 * ta * vel * pa_pr * pa_pr * pa_pr + \
        1.45389826e-4 * ta * ta * vel * pa_pr * pa_pr * pa_pr + \
        2.17508610e-4 * vel * vel * pa_pr * pa_pr * pa_pr + \
        -6.66724702e-5 * ta * vel * vel * pa_pr * pa_pr * pa_pr + \
        3.33217140e-5 * vel * vel * vel * pa_pr * pa_pr * pa_pr + \
        -0.00226921615 * d_tr * pa_pr * pa_pr * pa_pr + \
        3.80261982e-4 * ta * d_tr * pa_pr * pa_pr * pa_pr + \
        -5.45314314e-9 * ta * ta * d_tr * pa_pr * pa_pr * pa_pr + \
        -7.96355448e-4 * vel * d_tr * pa_pr * pa_pr * pa_pr + \
        2.53458034e-5 * ta * vel * d_tr * pa_pr * pa_pr * pa_pr + \
        -6.31223658e-6 * vel * vel * d_tr * pa_pr * pa_pr * pa_pr + \
        3.02122035e-4 * d_tr * d_tr * pa_pr * pa_pr * pa_pr + \
        -4.77403547e-6 * ta * d_tr * d_tr * pa_pr * pa_pr * pa_pr + \
        1.73825715e-6 * vel * d_tr * d_tr * pa_pr * pa_pr * pa_pr + \
        -4.09087898e-7 * d_tr * d_tr * d_tr * pa_pr * pa_pr * pa_pr + \
        0.614155345 * pa_pr * pa_pr * pa_pr * pa_pr + \
        -0.0616755931 * ta * pa_pr * pa_pr * pa_pr * pa_pr + \
        0.00133374846 * ta * ta * pa_pr * pa_pr * pa_pr * pa_pr + \
        0.00355375387 * vel * pa_pr * pa_pr * pa_pr * pa_pr + \
        -5.13027851e-4 * ta * vel * pa_pr * pa_pr * pa_pr * pa_pr + \
        1.02449757e-4 * vel * vel * pa_pr * pa_pr * pa_pr * pa_pr + \
        -0.00148526421 * d_tr * pa_pr * pa_pr * pa_pr * pa_pr + \
        -4.11469183e-5 * ta * d_tr * pa_pr * pa_pr * pa_pr * pa_pr + \
        -6.80434415e-6 * vel * d_tr * pa_pr * pa_pr * pa_pr * pa_pr + \
        -9.77675906e-6 * d_tr * d_tr * pa_pr * pa_pr * pa_pr * pa_pr + \
        0.0882773108 * pa_pr * pa_pr * pa_pr * pa_pr * pa_pr + \
        -0.00301859306 * ta * pa_pr * pa_pr * pa_pr * pa_pr * pa_pr + \
        0.00104452989 * vel * pa_pr * pa_pr * pa_pr * pa_pr * pa_pr + \
        2.47090539e-4 * d_tr * pa_pr * pa_pr * pa_pr * pa_pr * pa_pr + \
        0.00148348065 * pa_pr * pa_pr * pa_pr * pa_pr * pa_pr * pa_pr

    return utci_approx

def Solweig_2015a_metdata_noload(inputdata, location, UTC, leafon1, leafoff1):
    """
    This function is used to process the input meteorological file.
    It also calculates Sun position based on the time specified in the met-file
    
    :param inputdata:
    :param location:
    :param UTC:
    :return:
    """
    
    met = inputdata
    data_len = len(met[:, 0])
    dectime = met[:, 1]+met[:, 2] / 24 + met[:, 3] / (60*24.)
    dectimemin = met[:, 3] / (60*24.)
    if data_len == 1:
        halftimestepdec = 0
    else:
        halftimestepdec = (dectime[1] - dectime[0]) / 2.
    
    time = dict()
    time['sec'] = 0
    time['UTC'] = UTC
    sunmaximum = 0.
    # (WRI) - Redefined these constants as function parameters
    # leafon1 = 97  #TODO this should change
    # leafoff1 = 300  #TODO this should change
    
    # initialize matrices
    altitude = np.empty(shape=(1, data_len))
    azimuth = np.empty(shape=(1, data_len))
    zen = np.empty(shape=(1, data_len))
    jday = np.empty(shape=(1, data_len))
    YYYY = np.empty(shape=(1, data_len))
    leafon = np.empty(shape=(1, data_len))
    altmax = np.empty(shape=(1, data_len))
    
    sunmax = dict()
    
    for i, row in enumerate(met[:, 0]):
        if met[i, 1] == 221:
            test = 4
        YMD = datetime.datetime(int(met[i, 0]), 1, 1) + datetime.timedelta(int(met[i, 1]) - 1)
        # Finding maximum altitude in 15 min intervals (20141027)
        if (i == 0) or (np.mod(dectime[i], np.floor(dectime[i])) == 0):
            fifteen = 0.
            sunmaximum = -90.
            sunmax['zenith'] = 90.
            while sunmaximum <= 90. - sunmax['zenith']:
                sunmaximum = 90. - sunmax['zenith']
                fifteen = fifteen + 15. / 1440.
                HM = datetime.timedelta(days=(60*10)/1440.0 + fifteen)
                YMDHM = YMD + HM
                time['year'] = YMDHM.year
                time['month'] = YMDHM.month
                time['day'] = YMDHM.day
                time['hour'] = YMDHM.hour
                time['min'] = YMDHM.minute
                # sunmax = sp.sun_position(time,location)
                sunmax = sun_position(time, location)
        
        altmax[0, i] = sunmaximum
        
        half = datetime.timedelta(days=halftimestepdec)
        H = datetime.timedelta(hours=met[i, 2])
        M = datetime.timedelta(minutes=met[i, 3])
        YMDHM = YMD + H + M - half
        time['year'] = YMDHM.year
        time['month'] = YMDHM.month
        time['day'] = YMDHM.day
        time['hour'] = YMDHM.hour
        time['min'] = YMDHM.minute
        #sun = sp.sun_position(time, location)
        sun = sun_position(time, location)
        altitude[0, i] = 90. - sun['zenith']
        azimuth[0, i] = sun['azimuth']
        zen[0, i] = sun['zenith'] * (np.pi/180.)
        
        # day of year and check for leap year
        if calendar.isleap(time['year']):
            dayspermonth = np.atleast_2d([31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
        else:
            dayspermonth = np.atleast_2d([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
        # jday[0, i] = np.sum(dayspermonth[0, 0:time['month']-1]) + time['day'] # bug when a new day 20191015
        YYYY[0, i] = met[i, 0]
        doy = YMD.timetuple().tm_yday
        jday[0, i] = doy
        if (doy > leafon1) | (doy < leafoff1):
            leafon[0, i] = 1
        else:
            leafon[0, i] = 0
    
    return YYYY, altitude, azimuth, zen, jday, leafon, dectime, altmax


def gvf_2018a(wallsun, walls, buildings, scale, shadow, first, second, dirwalls, Tg, Tgwall, Ta, emis_grid, ewall,
              alb_grid, SBC, albedo_b, rows, cols, Twater, lc_grid, landcover):
    azimuthA = np.arange(5, 359, 20)  # Search directions for Ground View Factors (GVF)
    
    #### Ground View Factors ####
    gvfLup = np.zeros((rows, cols))
    gvfalb = np.zeros((rows, cols))
    gvfalbnosh = np.zeros((rows, cols))
    gvfLupE = np.zeros((rows, cols))
    gvfLupS = np.zeros((rows, cols))
    gvfLupW = np.zeros((rows, cols))
    gvfLupN = np.zeros((rows, cols))
    gvfalbE = np.zeros((rows, cols))
    gvfalbS = np.zeros((rows, cols))
    gvfalbW = np.zeros((rows, cols))
    gvfalbN = np.zeros((rows, cols))
    gvfalbnoshE = np.zeros((rows, cols))
    gvfalbnoshS = np.zeros((rows, cols))
    gvfalbnoshW = np.zeros((rows, cols))
    gvfalbnoshN = np.zeros((rows, cols))
    gvfSum = np.zeros((rows, cols))

    #  sunwall=wallinsun_2015a(buildings,azimuth(i),shadow,psi(i),dirwalls,walls);
    sunwall = (wallsun / walls * buildings) == 1  # new as from 2015a

    for j in np.arange(0, azimuthA.__len__()):
        _, gvfLupi, gvfalbi, gvfalbnoshi, gvf2 = sunonsurface_2018a(azimuthA[j], scale, buildings, shadow, sunwall,
                                                                    first,
                                                                    second, dirwalls * np.pi / 180, walls, Tg, Tgwall,
                                                                    Ta,
                                                                    emis_grid, ewall, alb_grid, SBC, albedo_b, Twater,
                                                                    lc_grid, landcover)

        gvfLup = gvfLup + gvfLupi
        gvfalb = gvfalb + gvfalbi
        gvfalbnosh = gvfalbnosh + gvfalbnoshi
        gvfSum = gvfSum + gvf2

        if (azimuthA[j] >= 0) and (azimuthA[j] < 180):
            gvfLupE = gvfLupE + gvfLupi
            gvfalbE = gvfalbE + gvfalbi
            gvfalbnoshE = gvfalbnoshE + gvfalbnoshi

        if (azimuthA[j] >= 90) and (azimuthA[j] < 270):
            gvfLupS = gvfLupS + gvfLupi
            gvfalbS = gvfalbS + gvfalbi
            gvfalbnoshS = gvfalbnoshS + gvfalbnoshi

        if (azimuthA[j] >= 180) and (azimuthA[j] < 360):
            gvfLupW = gvfLupW + gvfLupi
            gvfalbW = gvfalbW + gvfalbi
            gvfalbnoshW = gvfalbnoshW + gvfalbnoshi

        if (azimuthA[j] >= 270) or (azimuthA[j] < 90):
            gvfLupN = gvfLupN + gvfLupi
            gvfalbN = gvfalbN + gvfalbi
            gvfalbnoshN = gvfalbnoshN + gvfalbnoshi

    gvfLup = gvfLup / azimuthA.__len__() + SBC * emis_grid * (Ta + 273.15) ** 4
    gvfalb = gvfalb / azimuthA.__len__()
    gvfalbnosh = gvfalbnosh / azimuthA.__len__()

    gvfLupE = gvfLupE / (azimuthA.__len__() / 2) + SBC * emis_grid * (Ta + 273.15) ** 4
    gvfLupS = gvfLupS / (azimuthA.__len__() / 2) + SBC * emis_grid * (Ta + 273.15) ** 4
    gvfLupW = gvfLupW / (azimuthA.__len__() / 2) + SBC * emis_grid * (Ta + 273.15) ** 4
    gvfLupN = gvfLupN / (azimuthA.__len__() / 2) + SBC * emis_grid * (Ta + 273.15) ** 4
    
    gvfalbE = gvfalbE / (azimuthA.__len__() / 2)
    gvfalbS = gvfalbS / (azimuthA.__len__() / 2)
    gvfalbW = gvfalbW / (azimuthA.__len__() / 2)
    gvfalbN = gvfalbN / (azimuthA.__len__() / 2)
    
    gvfalbnoshE = gvfalbnoshE / (azimuthA.__len__() / 2)
    gvfalbnoshS = gvfalbnoshS / (azimuthA.__len__() / 2)
    gvfalbnoshW = gvfalbnoshW / (azimuthA.__len__() / 2)
    gvfalbnoshN = gvfalbnoshN / (azimuthA.__len__() / 2)

    gvfNorm = gvfSum / (azimuthA.__len__())
    gvfNorm[buildings == 0] = 1
    
    return gvfLup, gvfalb, gvfalbnosh, gvfLupE, gvfalbE, gvfalbnoshE, gvfLupS, gvfalbS, gvfalbnoshS, gvfLupW, gvfalbW, gvfalbnoshW, gvfLupN, gvfalbN, gvfalbnoshN, gvfSum, gvfNorm


def cylindric_wedge(zen, svfalfa, rows, cols):

    # Fraction of sunlit walls based on sun altitude and svf wieghted building angles
    # input: 
    # sun zenith angle "beta"
    # svf related angle "alfa"

    beta=zen
    alfa=svfalfa
    
    # measure the size of the image
    # sizex=size(svfalfa,2)
    # sizey=size(svfalfa,1)
    
    xa=1-2./(np.tan(alfa)*np.tan(beta))
    ha=2./(np.tan(alfa)*np.tan(beta))
    ba=(1./np.tan(alfa))
    hkil=2.*ba*ha
    
    qa=np.zeros((rows, cols))
    # qa(length(svfalfa),length(svfalfa))=0;
    qa[xa<0]=np.tan(beta)/2
    
    Za=np.zeros((rows, cols))
    # Za(length(svfalfa),length(svfalfa))=0;
    Za[xa<0]=((ba[xa<0]**2)-((qa[xa<0]**2)/4))**0.5
    
    phi=np.zeros((rows, cols))
    #phi(length(svfalfa),length(svfalfa))=0;
    phi[xa<0]=np.arctan(Za[xa<0]/qa[xa<0])
    
    A=np.zeros((rows, cols))
    # A(length(svfalfa),length(svfalfa))=0;
    A[xa<0]=(np.sin(phi[xa<0])-phi[xa<0]*np.cos(phi[xa<0]))/(1-np.cos(phi[xa<0]))
    
    ukil=np.zeros((rows, cols))
    # ukil(length(svfalfa),length(svfalfa))=0
    ukil[xa<0]=2*ba[xa<0]*xa[xa<0]*A[xa<0]
    
    Ssurf=hkil+ukil
    
    F_sh=(2*np.pi*ba-Ssurf)/(2*np.pi*ba)#Xa
    
    return F_sh


def Lside_veg_v2015a(svfS,svfW,svfN,svfE,svfEveg,svfSveg,svfWveg,svfNveg,svfEaveg,svfSaveg,svfWaveg,svfNaveg,azimuth,altitude,Ta,Tw,SBC,ewall,Ldown,esky,t,F_sh,CI,LupE,LupS,LupW,LupN):
    # This m-file is the current one that estimates L from the four cardinal points 20100414
    # to deal with  RuntimeWarning: invalid value encountered in arcsin
    epsilon = 1e-10  # A small value to avoid log(0)
    svfE = np.clip(svfE, 0, 1 - epsilon)
    svfS = np.clip(svfS, 0, 1 - epsilon)
    svfW = np.clip(svfW, 0, 1 - epsilon)
    svfN = np.clip(svfN, 0, 1 - epsilon)
    
    #Building height angle from svf
    svfalfaE=np.arcsin(np.exp((np.log(1-svfE))/2))
    svfalfaS=np.arcsin(np.exp((np.log(1-svfS))/2))
    svfalfaW=np.arcsin(np.exp((np.log(1-svfW))/2))
    svfalfaN=np.arcsin(np.exp((np.log(1-svfN))/2))
    
    vikttot=4.4897
    aziW=azimuth+t
    aziN=azimuth-90+t
    aziE=azimuth-180+t
    aziS=azimuth-270+t
    
    F_sh = 2*F_sh-1  #(cylindric_wedge scaled 0-1)
    
    c=1-CI
    Lsky_allsky = esky*SBC*((Ta+273.15)**4)*(1-c)+c*SBC*((Ta+273.15)**4)
    
    ## Least
    [viktveg, viktwall, viktsky, viktrefl] = Lvikt_veg(svfE, svfEveg, svfEaveg, vikttot)
    
    if altitude > 0:  # daytime
        alfaB=np.arctan(svfalfaE)
        betaB=np.arctan(np.tan((svfalfaE)*F_sh))
        betasun=((alfaB-betaB)/2)+betaB
        # betasun = np.arctan(0.5*np.tan(svfalfaE)*(1+F_sh)) #TODO This should be considered in future versions
        if (azimuth > (180-t))  and  (azimuth <= (360-t)):
            Lwallsun=SBC*ewall*((Ta+273.15+Tw*np.sin(aziE*(np.pi/180)))**4)*\
                viktwall*(1-F_sh)*np.cos(betasun)*0.5
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*F_sh*0.5
        else:
            Lwallsun=0
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5
    else: #nighttime
        Lwallsun=0
        Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5
    
    Lsky=((svfE+svfEveg-1)*Lsky_allsky)*viktsky*0.5
    Lveg=SBC*ewall*((Ta+273.15)**4)*viktveg*0.5
    Lground=LupE*0.5
    Lrefl=(Ldown+LupE)*(viktrefl)*(1-ewall)*0.5
    Least=Lsky+Lwallsun+Lwallsh+Lveg+Lground+Lrefl
    
    # clear alfaB betaB betasun Lsky Lwallsh Lwallsun Lveg Lground Lrefl viktveg viktwall viktsky
    
    ## Lsouth
    [viktveg,viktwall,viktsky,viktrefl]=Lvikt_veg(svfS,svfSveg,svfSaveg,vikttot)
    
    if altitude>0: # daytime
        alfaB=np.arctan(svfalfaS)
        betaB=np.arctan(np.tan((svfalfaS)*F_sh))
        betasun=((alfaB-betaB)/2)+betaB
        # betasun = np.arctan(0.5*np.tan(svfalfaS)*(1+F_sh))
        if (azimuth <= (90-t))  or  (azimuth > (270-t)):
            Lwallsun=SBC*ewall*((Ta+273.15+Tw*np.sin(aziS*(np.pi/180)))**4)*\
                viktwall*(1-F_sh)*np.cos(betasun)*0.5
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*F_sh*0.5
        else:
            Lwallsun=0
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5
    else: #nighttime
        Lwallsun=0
        Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5

    Lsky=((svfS+svfSveg-1)*Lsky_allsky)*viktsky*0.5
    Lveg=SBC*ewall*((Ta+273.15)**4)*viktveg*0.5
    Lground=LupS*0.5
    Lrefl=(Ldown+LupS)*(viktrefl)*(1-ewall)*0.5
    Lsouth=Lsky+Lwallsun+Lwallsh+Lveg+Lground+Lrefl
    
    # clear alfaB betaB betasun Lsky Lwallsh Lwallsun Lveg Lground Lrefl viktveg viktwall viktsky
    
    ## Lwest
    [viktveg,viktwall,viktsky,viktrefl]=Lvikt_veg(svfW,svfWveg,svfWaveg,vikttot)
    
    if altitude>0: # daytime
        alfaB=np.arctan(svfalfaW)
        betaB=np.arctan(np.tan((svfalfaW)*F_sh))
        betasun=((alfaB-betaB)/2)+betaB
        # betasun = np.arctan(0.5*np.tan(svfalfaW)*(1+F_sh))
        if (azimuth > (360-t))  or  (azimuth <= (180-t)):
            Lwallsun=SBC*ewall*((Ta+273.15+Tw*np.sin(aziW*(np.pi/180)))**4)*\
                viktwall*(1-F_sh)*np.cos(betasun)*0.5
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*F_sh*0.5
        else:
            Lwallsun=0
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5
    else: #nighttime
        Lwallsun=0
        Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5

    Lsky=((svfW+svfWveg-1)*Lsky_allsky)*viktsky*0.5
    Lveg=SBC*ewall*((Ta+273.15)**4)*viktveg*0.5
    Lground=LupW*0.5
    Lrefl=(Ldown+LupW)*(viktrefl)*(1-ewall)*0.5
    Lwest=Lsky+Lwallsun+Lwallsh+Lveg+Lground+Lrefl
    
    # clear alfaB betaB betasun Lsky Lwallsh Lwallsun Lveg Lground Lrefl viktveg viktwall viktsky
    
    ## Lnorth
    [viktveg,viktwall,viktsky,viktrefl]=Lvikt_veg(svfN,svfNveg,svfNaveg,vikttot)
    
    if altitude>0: # daytime
        alfaB=np.arctan(svfalfaN)
        betaB=np.arctan(np.tan((svfalfaN)*F_sh))
        betasun=((alfaB-betaB)/2)+betaB
        # betasun = np.arctan(0.5*np.tan(svfalfaN)*(1+F_sh))
        if (azimuth > (90-t))  and  (azimuth <= (270-t)):
            Lwallsun=SBC*ewall*((Ta+273.15+Tw*np.sin(aziN*(np.pi/180)))**4)*\
                viktwall*(1-F_sh)*np.cos(betasun)*0.5
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*F_sh*0.5
        else:
            Lwallsun=0
            Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5
    else: #nighttime
        Lwallsun=0
        Lwallsh=SBC*ewall*((Ta+273.15)**4)*viktwall*0.5

    Lsky=((svfN+svfNveg-1)*Lsky_allsky)*viktsky*0.5
    Lveg=SBC*ewall*((Ta+273.15)**4)*viktveg*0.5
    Lground=LupN*0.5
    Lrefl=(Ldown+LupN)*(viktrefl)*(1-ewall)*0.5
    Lnorth=Lsky+Lwallsun+Lwallsh+Lveg+Lground+Lrefl

    # clear alfaB betaB betasun Lsky Lwallsh Lwallsun Lveg Lground Lrefl viktveg viktwall viktsky
    
    return Least,Lsouth,Lwest,Lnorth

def Kup_veg_2015a(radI,radD,radG,altitude,svfbuveg,albedo_b,F_sh,gvfalb,gvfalbE,gvfalbS,gvfalbW,gvfalbN,gvfalbnosh,gvfalbnoshE,gvfalbnoshS,gvfalbnoshW,gvfalbnoshN):

    Kup=(gvfalb*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnosh

    KupE=(gvfalbE*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshE

    KupS=(gvfalbS*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshS

    KupW=(gvfalbW*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshW

    KupN=(gvfalbN*radI*np.sin(altitude*(np.pi/180.)))+(radD*svfbuveg+albedo_b*(1-svfbuveg)*(radG*(1-F_sh)+radD*F_sh))*gvfalbnoshN

    return Kup, KupE, KupS, KupW, KupN

def saturated_vapor_pressure_hpa(db_temp):
    """Calculate saturated vapor pressure (hPa) at temperature (C).

    This equation of saturation vapor pressure is specific to the UTCI model.
    """
    g = (-2836.5744, -6028.076559, 19.54263612, -0.02737830188, 0.000016261698,
         7.0229056e-10, -1.8680009e-13)
    tk = db_temp + 273.15  # air temp in K
    es = 2.7150305 * math.log(tk)
    for i, x in enumerate(g):
        es = es + (x * (tk**(i - 2)))
    es = math.exp(es) * 0.01
    return es


def prepareVegMeteo(leafon, metdata, height, dectime):
    ## use vegetation, the transmissivity of light through vegetation, default is 0.03 in Solweig
    trans = 0.03
    psi = leafon * trans
    psi[leafon == 0] = 0.5

    # %Creating vectors from meteorological input
    DOY = metdata[:, 1]
    hours = metdata[:, 2]
    minu = metdata[:, 3]
    Ta = metdata[:, 11]
    RH = metdata[:, 10]
    radG = metdata[:, 14]
    radD = metdata[:, 21]
    radI = metdata[:, 22]
    P = metdata[:, 12]
    Ws = metdata[:, 9]

    # %Parameterisarion for Lup
    if not height:
        height = 1.1

    # %Radiative surface influence, Rule of thumb by Schmid et al. (1990).
    first = np.round(height)
    if first == 0.:
        first = 1.
    second = np.round((height * 20.))

    # Initialisation of time related variables
    if Ta.__len__() == 1:
        timestepdec = 0
    else:
        timestepdec = dectime[1] - dectime[0]
    
    radD = metdata[:, 21]
    return psi, DOY, hours, minu, Ta, RH, radG, radD, radI, P, Ws, height, \
        first, second, timestepdec
