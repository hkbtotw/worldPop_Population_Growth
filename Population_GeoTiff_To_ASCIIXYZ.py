# -*- coding: utf-8 -*-
# import json
import geopandas as gpd
# from fiona import _shim, schema
import pandas as pd
import os, sys
import glob
# from Credential import *
from math import radians, cos, sin, asin, sqrt
# from fiona import._shim    #for input output with geopandas package

from datetime import datetime, date,  timedelta
from dateutil.relativedelta import relativedelta

import warnings
from tqdm import *

from raster2xyz.raster2xyz import Raster2xyz

#enable tqdm with pandas, progress_apply
tqdm.pandas()


warnings.filterwarnings('ignore')



######################################
### specify current directory
file_path=os.getcwd()

## datawarehouse path
dw_path='D:\\DataWarehouse\\Population_Density\\Raw_Data\\'

raw_data_path='\\Age_Geotiff\\'
converted_data_path='\\Age_XYZ\\'

####################################

## list all geotiff file
fileList=glob.glob(dw_path+raw_data_path+"*.tif")
print(' --- file :',fileList)

for file in tqdm(fileList[1:]):
    # print(' ==> ',file)
    filename=file.split('\\')[len(file.split('\\'))-1].split('.')[0]
    print(' : ',filename)
    outputfile=dw_path+converted_data_path+filename+'.csv'
    outputfile_parquet=dw_path+converted_data_path+filename+'.parquet'
    rtxyz=Raster2xyz()
    rtxyz.translate(file, outputfile)
    dfDummy=pd.read_csv(outputfile)
    dfDummy=dfDummy[dfDummy['z']>0].copy().reset_index(drop=True)
    dfDummy.to_csv(outputfile, index=False)
    print(len(dfDummy),' --- filename ---',dfDummy.head(3),' :: ',dfDummy.columns)
    dfDummy.to_parquet(outputfile_parquet)

    del rtxyz, dfDummy

print('***********************************************')
print('*************** DONE **************************')
print('***********************************************')
