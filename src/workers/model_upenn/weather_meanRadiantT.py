# # This function is used to analyze the thermal comfort MRT
## based on the modified code from UMEP using the LiDAR data
## as the input and the dynamic WEATHER data, the weather data
## is collected from, NREL, https://maps.nrel.gov/nsrdb-viewer/
## begin May 23,2020 @ Xiaojiang Li, Temple University
import shutil

# this the cpu version, with out the calculation of the UTCI
# last by Xiaojiang Li, Oct 15, 2024

# this the cpu version, with out the calculation of the UTCI
# last by Xiaojiang Li, Oct 15, 2024

# last updated by Xiaojiang Li, June 1st, 2025

import numpy as np
import os, os.path
from osgeo import gdal
from osgeo.gdalconst import *
import pandas as pd

from src.workers.model_upenn.libraries import solweiglib



def preprocessMeteorolgoicalData(csvfile, month='8', day='1', hour='12'):
    '''This script is used to preprocess the meteoroglocial data, a complete version 
    has the hourly level meteorologocal data
    
    Parameters:
        csvfile: a csv file of the weather data, could be downloaded from NREL
                 or the processed one row mean value
        month: default '8'
        day: default '1'
        hours: default '12'
    Return:
        one row or many row records that meet the conditions, like certain month, days, etc
    '''

    ####--------------------- meteorological data------------
    # csvfile = 'weather-data.csv'
    rows_to_keep = [1]
    # weather_df = pd.read_csv(csvfile, ',', skiprows = lambda x: x in rows_to_keep)
    weather_df = pd.read_csv(csvfile, sep=',', skiprows = lambda x: x in rows_to_keep, low_memory=False)

    # only keep the record from the third row, the third row would be used as the field name
    new_header = weather_df.iloc[0]
    weather_df = weather_df[1:]
    weather_df.columns = new_header
    
    # only select the necessary fields
    fields = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'DHI', 'DNI', \
            'GHI', 'Clearsky DHI', 'Clearsky DNI','Clearsky GHI', \
            'Wind Speed', 'Relative Humidity', 'Temperature', 'Pressure']
    # windspeed, m/s, Temperature C
    weather_df = weather_df[fields]

    # get the weather record for any specified month, day, hour, and minutes
    weather_df_res = weather_df.loc[(weather_df['Month'] == month) & \
                                    (weather_df['Day'] == day) & \
                                    (weather_df['Hour'] == hour) & \
                                    (weather_df['Minute'] == '0')]

    return weather_df_res


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

def _get_epsg_code(raster_path):
    from osgeo import osr
    with gdal.Open(raster_path) as dataset:
        # Get the spatial reference from the dataset
        projection = dataset.GetProjection()
        spatial_ref = osr.SpatialReference()
        spatial_ref.ImportFromWkt(projection)

        # Extract the EPSG code
        epsg_code = int(spatial_ref.GetAttrValue("AUTHORITY", 1))
    return epsg_code

def run_mrt_calculations(method_params):
    '''
    Parameters:
    landcover: switch to use land cover or not, landcover=1, use; landocover=0, not use.
    onlyglobal: switch for handling global radiation. 0=both the direct and diffuse available, 1=only the global radiation is available, use the reidn's method
    '''

    landcover = 1
    onlyglobal = 1

    dsmfile = method_params['INPUT_DSM']
    svffolder = method_params['INPUT_SVF']
    wallfile = method_params['INPUT_HEIGHT']
    aspectfile = method_params['INPUT_ASPECT']
    chmfile = method_params['INPUT_CDSM']
    albedo_file = method_params['INPUT_ALBEDO']

    TRANS_VEG = method_params['TRANS_VEG']
    LEAF_START = method_params['LEAF_START']
    LEAF_END = method_params['LEAF_END']
    CONIFER_TREES = method_params['CONIFER_TREES']
    INPUT_TDSM = method_params['INPUT_TDSM']
    INPUT_THEIGHT = method_params['INPUT_THEIGHT']
    lufile = method_params['INPUT_LC']
    USE_LC_BUILD = method_params['USE_LC_BUILD']
    INPUT_DEM = method_params['INPUT_DEM']
    SAVE_BUILD = method_params['SAVE_BUILD']
    INPUT_ANISO = method_params['INPUT_ANISO']
    albedo_b = method_params['ALBEDO_WALLS']
    albedo_g = method_params['ALBEDO_GROUND']
    ewall = method_params['EMIS_WALLS']
    eground = method_params['EMIS_GROUND']
    ABS_S = method_params['ABS_S']
    ABS_L = method_params['ABS_L']
    POSTURE = method_params['POSTURE']
    CYL = method_params['CYL']
    csvfile = method_params['INPUTMET']
    ONLYGLOBAL = method_params['ONLYGLOBAL']
    UTC = float(method_params['UTC'])
    POI_FILE = method_params['POI_FILE']
    POI_FIELD = method_params['POI_FIELD']
    AGE = method_params['AGE']
    ACTIVITY = method_params['ACTIVITY']
    CLO = method_params['CLO']
    WEIGHT = method_params['WEIGHT']
    HEIGHT = method_params['HEIGHT']
    SEX = method_params['SEX']
    SENSOR_HEIGHT = method_params['SENSOR_HEIGHT']
    OUTPUT_TMRT = method_params['OUTPUT_TMRT']
    OUTPUT_KDOWN = method_params['OUTPUT_KDOWN']
    OUTPUT_KUP = method_params['OUTPUT_KUP']
    OUTPUT_LDOWN = method_params['OUTPUT_LDOWN']
    OUTPUT_LUP = method_params['OUTPUT_LUP']
    OUTPUT_SH = method_params['OUTPUT_SH']
    OUTPUT_TREEPLANTER = method_params['OUTPUT_TREEPLANTER']
    mrtfolder = method_params['OUTPUT_DIR']

    epsgcode = _get_epsg_code(dsmfile)

    # the air temperature tile
    airTfile = os.path.join(os.path.dirname(csvfile), 'air_temperature', 'clipped_airT.tif').replace('scratch_target', 'sample_cities')

    # THE FOLLOWING IS THE ENCODING OF LAND USE/COVER
    # lc_class:
    #     lin = ['Name              Code Alb  Emis Ts_deg Tstart TmaxLST \n',
    #         'Roofs(buildings)   2   0.18 0.95 0.58   -9.78  15.0\n',
    #         'Dark_asphalt       1   0.18 0.95 0.58   -9.78  15.0\n',
    #         'Cobble_stone_2014a 0   0.20 0.95 0.37   -3.41  15.0\n',
    #         'Water              7   0.05 0.98 0.00    0.00  12.0\n',
    #         'Grass_unmanaged    5   0.16 0.94 0.21   -3.38  14.0\n',
    #         'bare_soil          6   0.25 0.94 0.33   -3.01  14.0\n',
    #         'Walls             99   0.20 0.90 0.58   -3.41  15.0']
    # The albedo of the tree canopy is not considered. This is because there is no need to consider
    # the albedo of trees, since they would block the solar radiation from reaching the ground. In 
    # this case, the tree canopy should be reclassified as the grass, beneath the tree

    lc_class = prepareAlbedo()

    ## the model parameters setting
    Twater = 15.0 # water temperature, this doesn't impact the mean radiant temperature
    ani = 0
    diffsh = None

    lon, lat, scale, rows, cols, alt, dsm, svf, svfN, svfS, svfE, \
        svfW, svfveg, svfNveg, svfSveg, svfEveg, svfWveg, svfaveg, svfNaveg, \
        svfSaveg, svfEaveg, svfWaveg, svfalfa, wallheight, wallaspect,\
        amaxvalue, vegdsm, vegdsm2, bush, svfbuveg, gdal_dsm = solweiglib.prepareData(mrtfolder, svffolder, dsmfile, chmfile, wallfile, aspectfile, epsgcode)

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
    svfalfa = np.arcsin(np.exp((np.log((1. - tmp)) / 2.)))

    # vegetation dsm
    # TODO Should this be a parameter???
    usevegdem = 1

    # %Initialization of maps
    Knight = np.zeros((rows, cols))
    Tgmap1 = np.zeros((rows, cols))
    Tgmap1E = np.zeros((rows, cols))
    Tgmap1S = np.zeros((rows, cols))
    Tgmap1W = np.zeros((rows, cols))
    Tgmap1N = np.zeros((rows, cols))

    #### ------------land cover and albedo------------------
    # [TgK, Tstart, lcgrid, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall] = solweiglib.landcoverAlbedo(root, base, Knight, albedo_g, eground, lc_class, landcover)
    [TgK, Tstart, lcgrid, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST, TmaxLST_wall] = (
        solweiglib.landcoverAlbedoNew(lufile, albedo_file, Knight, albedo_g, eground, lc_class, landcover))


    # here the dsm is the building height, non-building has height of zero
    # buildings = dsm - dem
    # buildings[buildings < 2.] = 1.
    # buildings[buildings >= 2.] = 0.
    buildings = dsm
    # TODO are these values in feet or meters?
    buildings[dsm < 2.] = 1.
    buildings[dsm >= 2.] = 0.

    ## loop all the weather csv data to get the information meteorological data
    from datetime import datetime
    sample_datetime = datetime(2022, 7, 1, 12)
    day_summer_df = preprocessMeteorolgoicalData(csvfile, month=str(sample_datetime.month), day=str(sample_datetime.day), hour=str(sample_datetime.hour))

    # calculate the average of mean radiant temperature in summer
    tmrt_mean = np.zeros((rows, cols))
    # number of hours in summer from June to August
    # TODO Should this be a parameter???
    num_hour = 0

    # loop hourly weather data
    for idx, row in day_summer_df.iterrows():
        # date information and metdata
        [metdata, year, month, day, hour, minu, doy] = solweiglib.metdataParse(row)

        ## other parameters
        absK, absL, pos, cyl, Fside, Fup, height, Fcyl, elvis, \
            timeadd, timeaddE, timeaddS, timeaddW, timeaddN, \
                firstdaytime = solweiglib.otherParameters()

        # Based on the previous functions to load the meteological data using the lon, lat as the input
        location = {'longitude': lon, 'latitude': lat, 'altitude': alt}
        YYYY, altitude, azimuth, zen, jday, leafon, dectime, altmax = \
            solweiglib.Solweig_2015a_metdata_noload(metdata, location, UTC)

        ## use vegetation, the transmissivity of light through vegetation, default is 0.03 in Solweig
        psi, DOY, hours, minus, Ta, RH, radG, radD, radI, P, Ws, height, \
            first, second, timestepdec = solweiglib.prepareVegMeteo(leafon, metdata, height, dectime)

        # night and day time
        # TODO Should this be a parameter???
        CI = 0  # #  If metfile starts at night, CI = 1.

        ## after preparation of parameters, start to compute the mean radiant temperature
        tmrtplot = np.zeros((rows, cols))
        TgOut1 = np.zeros((rows, cols))

        for i in np.arange(0, Ta.__len__()):
            # Nocturnal cloudfraction from Offerle et al. 2003
            if (dectime[i] - np.floor(dectime[i])) == 0:
                daylines = np.where(np.floor(dectime) == dectime[i])
                if daylines.__len__() > 1:
                    alt = altitude[0][daylines]
                    alt2 = np.where(alt > 1)
                    rise = alt2[0][0]
                    [_, CI, _, _, _] = solweiglib.clearnessindex_2013b(zen[0, i + rise + 1], jday[0, i + rise + 1],
                                                            Ta[i + rise + 1],
                                                            RH[i + rise + 1] / 100., radG[i + rise + 1], location,
                                                            P[i + rise + 1])  # i+rise+1 to match matlab code. correct?
                    if (CI > 1.) or (CI == np.inf):
                        CI = 1.
                else:
                    CI = 1.

            if os.path.exists(airTfile):
                airT_ds = gdal.Open(airTfile)
                print("the air temperature file is:", airTfile)
                airT_value = airT_ds.ReadAsArray().astype(float)
                print("The largest value is:", airT_value.max())
            else:
                airT_value = Ta[i]

            Tmrt, Kdown, Kup, Ldown, Lup, Tg, ea, esky, I0, CI, shadow, firstdaytime, timestepdec, timeadd, \
            Tgmap1, timeaddE, Tgmap1E, timeaddS, Tgmap1S, timeaddW, Tgmap1W, timeaddN, Tgmap1N, \
            Keast, Ksouth, Kwest, Knorth, Least, Lsouth, Lwest, Lnorth, KsideI, TgOut1, TgOut, radIout, radDout \
                = solweiglib.Solweig_2019a_calc(i, dsm, scale, rows, cols, svf, svfN, svfW, svfE, svfS, svfveg,
                    svfNveg, svfEveg, svfSveg, svfWveg, svfaveg, svfEaveg, svfSaveg, svfWaveg, svfNaveg,
                    vegdsm, vegdsm2, albedo_b, absK, absL, ewall, Fside, Fup, Fcyl, altitude[0][i],
                    azimuth[0][i], zen[0][i], jday[0][i], usevegdem, onlyglobal, buildings, location,
                    psi[0][i], landcover, lcgrid, dectime[i], altmax[0][i], wallaspect,
                    wallheight, cyl, elvis, airT_value, RH[i], radG[i], radD[i], radI[i], P[i], amaxvalue,
                    bush, Twater, TgK, Tstart, alb_grid, emis_grid, TgK_wall, Tstart_wall, TmaxLST,
                    TmaxLST_wall, first, second, svfalfa, svfbuveg, firstdaytime, timeadd, timeaddE, timeaddS,
                    timeaddW, timeaddN, timestepdec, Tgmap1, Tgmap1E, Tgmap1S, Tgmap1W, Tgmap1N, CI, TgOut1, diffsh, ani)

            tmrtplot = tmrtplot + Tmrt
            tmrt_mean = tmrt_mean + Tmrt
            num_hour = num_hour + 1

            if altitude[0][i] > 0:
                w = 'D'
            else:
                w = 'N'

        print('The number of elements in Ta is:', Ta.__len__())
        tmrtplot = tmrtplot / Ta.__len__()
        print(tmrtplot.shape)

        mrtFile = f"Tmrt_{sample_datetime.year}_{sample_datetime.timetuple().tm_yday}_{hour}00D.tif"
        print('The output file name is:', mrtFile)
        solweiglib.saverasternd(gdal_dsm, os.path.join(mrtfolder, mrtFile), tmrtplot)



    # tmrt_mean = tmrt_mean / float(num_hour)

    # mrtFile = "%s-MRT.tif"%(city)
    # print('The output file name is:', mrtFile)
    # solweiglib.saverasternd(gdal_dsm, os.path.join(mrtfolder, mrtFile), tmrt_mean)
