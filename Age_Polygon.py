# -*- coding: utf-8 -*-
# import json
from h3 import h3
import geojson
import PySimpleGUI as sg
from shapely.geometry import Polygon, Point
import geopandas as gpd
# from fiona import _shim, schema
import pandas as pd
import os, sys

#### population 
import psycopg2
from csv_join_tambon import Reverse_GeoCoding_Province, Reverse_GeoCoding, Reverse_GeoCoding_ALL
# from Credential import *
from math import radians, cos, sin, asin, sqrt
import math
# from fiona import._shim    #for input output with geopandas package

from datetime import datetime, date,  timedelta
from dateutil.relativedelta import relativedelta

##### plot
import matplotlib.pyplot as plt
import matplotlib as mpl
from adjustText import adjust_text
import seaborn as sns

###  hidden import xyzservices to create executable file :  https://github.com/geopandas/xyzservices/blob/main/README.md
# import contextily as ctx
import warnings
from tqdm import *
import glob
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


#enable tqdm with pandas, progress_apply
tqdm.pandas()


fp = mpl.font_manager.FontProperties(family='Tahoma',size=13)
warnings.filterwarnings('ignore')

###################################################################
##### Set plot parameters to enable the text display in eastern language
sns.set(style='whitegrid', palette='pastel', color_codes=True)
sns.mpl.rc('figure', figsize=(10,6))
# plt.rcParams['font.family']='tahoma'
plt.rc('font',family='tahoma')
##############################################################################################


#######################################
def Generate_Geometry_4326(latList, lngList):        
    crs = {'init':'EPSG:4326'}
    geometry = Polygon(zip(lngList, latList))
    return geometry

def Create_Geometry(LocationList):
    latList=[];lngList=[]
    for n in LocationList:                
        latList.append(n[1])
        lngList.append(n[0])
    return latList, lngList

def Extract_Latitude_Coor(coord):
    return list(coord[0])[1]

def Extract_Longitude_Coor(coord):
    return list(coord[0])[0]

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def Pack_Coor(lat, lng):
    #print(' ==>',lat, ' :: ',lng, ' ==> ',(lng,lat))
    return (lng,lat)

def Generate_Geopandas(dfIn, latCol, lngCol):
    dfIn['coords']=dfIn.apply(lambda x: Pack_Coor(x[latCol],x[lngCol]) ,axis=1)
    # print(len(dfIn), '*********** ---- Geopandas Generation---- ',dfIn.head(5),' :: ',dfIn.columns)
    crs = {'init':'EPSG:4326'}
    geometry = [Point(xy) for xy in zip(dfIn[lngCol], dfIn[latCol])]
    return gpd.GeoDataFrame(dfIn,   crs = crs,  geometry = geometry)

def Extract_Lat_Lng_Txt(stringList):
    latList=[];lngList=[]
    for string in stringList:
        latList.append(float(string.split('!')[0]))
        lngList.append(float(string.split('!')[1]))
    return latList, lngList

def Remove_string(x,word):
    try:
        return x.replace(word,'')
    except:
        return x
def Replace_string(x,word1,word2):
    try:
        return x.replace(word1,word2)
    except:
        return x

###### read information ######
### actual facebook population
def Read_FB_Population_General_Prv(province_string):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        if(len(province_string)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT lng, lat, population, p_name_t, a_name_t, t_name_t FROM public.\"fb_population_general\" where p_name_t in """+str(province_string)+"""  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT lng, lat, population, p_name_t, a_name_t, t_name_t FROM public.\"fb_population_general\" """

        dfout = pd.read_sql_query(sql, connection)

        # print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

### h3 grid information
def Read_H3_Grid_Lv8_Province_PAT(province):
    #print('------------- Start ReadDB -------------', province)
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT    [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
      ,[population_elder]
      ,[population_under_five]
      ,[population_515_2560]
      ,[population_men]
      ,[population_women]
      ,[population_general_5]
      ,[population_youth_5]
      ,[population_elder_5]
      ,[population_under_five_5]
      ,[population_515_2560_5]
      ,[population_men_5]
      ,[population_women_5]
      ,[ext_711_073]
      ,[ext_Retail_073]
      ,[ext_Residential_073]
      ,[ext_Restaurant_073]
      ,[ext_Education_073]
      ,[ext_Hotel_073]
      ,[ext_711_5C]
      ,[ext_Retail_5C]
      ,[ext_Residential_5C]
      ,[ext_Restaurant_5C]
      ,[ext_Education_5C]
      ,[ext_Hotel_5C]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
  FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Ext_Province_PAT]
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

###### map
def Generate_Map(in_gdf,provinceList, input_name):
    file_path= os.getcwd()
    
    prv_path='TH_province.shp'    # for province-boundary map
    shp_path = 'TH_amphoe_border.shp'   # for district boundary and name in each province
    subd_path='TH_tambon_boundary.shp'  # for subdistrict boundary and name

    markerSize=8
    ##############################################

    # Plot the map
    fig, ax = plt.subplots(1, figsize=(10, 10))
    #Set aspect to equal
    ax.set_aspect('equal')

    ### Read province / district shapefile
    prv_gdf=gpd.read_file(file_path+'\\SHAPE\\'+prv_path)
    prv_gdf = prv_gdf.to_crs(epsg=4326)
    gdf = gpd.read_file(file_path+'\\SHAPE\\'+shp_path)
    gdf = gdf.to_crs(epsg=4326)
    subd_gdf = gpd.read_file(file_path+'\\SHAPE\\'+subd_path)
    subd_gdf = subd_gdf.to_crs(epsg=4326)
    gdf['coords'] = gdf['geometry'].apply(lambda x: x.representative_point().coords[:])
    gdf['coords'] = [coords[0] for coords in gdf['coords']]
    gdf['a_name_t']=gdf.apply(lambda x: Remove_string(x['a_name_t'],'กิ่งอำเภอ') ,axis=1)   
    gdf['a_name_t']=gdf.apply(lambda x: Replace_string(x['a_name_t'],'หนองบุนนาก','หนองบุญมาก') ,axis=1)
    gdf['a_name_t']=gdf.apply(lambda x: Replace_string(x['a_name_t'],'ปทุมรัตต์','ปทุมรัตน์') ,axis=1)
    gdf['ref']=gdf['p_name_t']+'_'+gdf['a_name_t']

    subd_gdf=subd_gdf[subd_gdf['p_name_t'].isin(provinceList)]     # plot all district in province
    subd_gdf['coords'] = subd_gdf['geometry'].apply(lambda x: x.representative_point().coords[:])
    subd_gdf['coords'] = [coords[0] for coords in subd_gdf['coords']]
    subd_gdf['a_name_t']=subd_gdf.apply(lambda x: Remove_string(x['a_name_t'],'กิ่งอำเภอ') ,axis=1)
    subd_gdf['a_name_t']=subd_gdf.apply(lambda x: Replace_string(x['a_name_t'],'หนองบุนนาก','หนองบุญมาก') ,axis=1)
    subd_gdf['a_name_t']=subd_gdf.apply(lambda x: Replace_string(x['a_name_t'],'ปทุมรัตต์','ปทุมรัตน์') ,axis=1)

    prv_gdf_selected=prv_gdf[prv_gdf['p_name_t'].isin(provinceList)].copy().reset_index(drop=True)
    minx, miny, maxx, maxy = prv_gdf_selected.total_bounds
    # print('****************************************************')
    # print(' boundary : x - Longitude ',minx,' :: ',maxx)
    # print(' boundary : y - Latitude ',miny,' :: ',maxy)
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    # print('****************************************************')

    ### plot province
    prv_gdf_selected.boundary.plot(ax=ax, alpha=1.0,linewidth=0.8, edgecolor='black', zorder=1)
    
    # ### select & plot district
    gdf_selected=gdf[gdf['p_name_t'].isin(provinceList)]     # plot all district in province
    # gdf_selected=gdf[gdf['a_name_t']==district]     # plot only selected district in province
    gdf_selected.boundary.plot(ax=ax, alpha=1.0,linewidth=0.4, edgecolor='grey', zorder=1)  

    ### select & plot district
    subd_gdf_selected=subd_gdf.copy()     # plot all district in province
    # gdf_selected=gdf[gdf['a_name_t']==district]     # plot only selected district in province
    # subd_gdf_selected.boundary.plot(ax=ax, alpha=1.0,linewidth=0.4, edgecolor='grey', zorder=1)  


    ### plot polygon
    in_gdf.boundary.plot(ax=ax, alpha=1.0,linewidth=1.0, edgecolor='red', zorder=1)  

    #### display district name
    #### for crowded provinces
    # for idx, row in tqdm(subd_gdf_selected.iterrows()):
    #     plt.annotate(text=row['t_name_t'], xy=row['coords'], horizontalalignment='left',verticalalignment='bottom', color='black', size=8,zorder=5)
        

    texts=[     
                plt.annotate(text=row['a_name_t'], xy=row['coords'], horizontalalignment='left',verticalalignment='bottom', color='black', size=8,zorder=5) for idx, row in gdf_selected.iterrows()                
                # plt.annotate(text=row['t_name_t'], xy=row['coords'], horizontalalignment='left',verticalalignment='bottom', color='black', size=8,zorder=5) for idx, row in subd_gdf_selected.iterrows()                
            ]    
    adjust_text(texts)

    # print(' input_gdf : ', input_gdf.head(3),' : ',input_gdf.columns)    
    # gdf_selected.to_excel(os.getcwd()+'\\'+'check_gdfselected.xlsx',index=False)
    # input_gdf.to_excel(os.getcwd()+'\\'+'check_inputgdf.xlsx',index=False)

    def Unpack_Coor(x):
        try:
            return x[0]
        except: 
            return x
    in_gdf['coords']=in_gdf.apply(lambda x: Unpack_Coor(x['coords'])  ,axis=1)
 
    #### display polygon index
    texts=[     
                plt.annotate(text=str(row['PolygonNumber']), xy=row['coords'], horizontalalignment='left',verticalalignment='bottom', color='red', size=14,zorder=5) for idx, row in in_gdf.iterrows()                
            ]    
    adjust_text(texts)
 
    # Contextily basemap:   Don't see difference eventhough adding map 
    ###  ref:  https://stackoverflow.com/questions/56559520/change-background-map-for-contextily
    ### https://contextily.readthedocs.io/en/latest/providers_deepdive.html
    # ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)   

    ### set axis off
    ax.set_axis_off() 

    filename=str(input_name)+'_area.png'    
    try:
        fig.savefig(file_path+'\\'+filename, dpi=600)         
    except:
        filename=''

    # plt.show()   

    del prv_gdf, gdf, prv_gdf_selected #, gdf_selected

    return None


#### h3
def Exe_geo_to_h3(lat, lng, h3_resolution):
    return h3.geo_to_h3(lat, lng, h3_resolution)

def Exe_h3_to_geoboundary(hex_id):
    return h3.h3_to_geo_boundary(hex_id)

def Exe_Lat_h3_to_geo(hex_id):
    return h3.h3_to_geo(hex_id)[0]
def Exe_Lng_h3_to_geo(hex_id):
    return h3.h3_to_geo(hex_id)[1]


def Generate_Geometry_4326(latList, lngList):        
    crs = {'init':'EPSG:4326'}
    geometry = Polygon(zip(lngList, latList))
    return geometry

def Generate_HexGeometry_2(hexId):
    dummyString=str(h3.h3_to_geo_boundary(hexId))
    # print(' ---> ',dummyString, ' : ',type(dummyString))
    words=['((','))']
    for word in words:
        dummyString =  dummyString.replace(word, "").strip()
    stringList=dummyString.split('), (')
    # print(' list : ',stringList)
    latList=[]; lngList=[]
    for component in stringList:
        latList.append(float(component.split(',')[0]))
        lngList.append(float(component.split(',')[1]))
    # print(latList, ' :: ',lngList)
    return Generate_Geometry_4326(latList, lngList)


def Generate_Geopandas_3(dfIn, latCol, lngCol, index_col):
    dfIn['coords']=dfIn.progress_apply(lambda x: Pack_Coor(x[latCol],x[lngCol]) ,axis=1)
    # print(len(dfIn), '*********** ---- Geopandas Generation---- ',dfIn.head(5),' :: ',dfIn.columns)
    crs = {'init':'EPSG:4326'}  
    geomList=[]
    for hexId in tqdm(dfIn[index_col]): 
        geomList.append(Generate_HexGeometry_2(hexId))
    geometry=geomList
    return gpd.GeoDataFrame(dfIn,   crs = crs, geometry=geometry)


def Generate_Plot_Shapefile(df_xyz,file_path,province, temp_shp, if_save,h3_resolution):
    ### province='นนทบุรี'
    #### select province for testing
    if(len(province)>0):
        df_xyz=df_xyz[df_xyz['p_name_t']==province].copy().reset_index(drop=True)
    ########################

    #### convert lat lng to hex_id
    df_xyz['hex_id']=df_xyz.progress_apply(lambda x: Exe_geo_to_h3(x['Latitude'],x['Longitude'],h3_resolution) ,axis=1)
    print(len(df_xyz),' --- xyz h3 ----', df_xyz.head(3),' ::: ',df_xyz.columns)
    crs = {'init':'EPSG:4326'}  
    geomList=[]
    for hexId in tqdm(df_xyz['hex_id']): 
        geomList.append(Generate_HexGeometry_2(hexId))
    xyz_gdf=gpd.GeoDataFrame(df_xyz,   crs = crs, geometry=geomList)

    if(if_save==1):
        # df_xyz[df_xyz['p_name_t']=='นนทบุรี'].to_excel(file_path+'\\'+temp_output,index=False)
        xyz_gdf.to_file(file_path+'\\'+temp_shp) 

    return xyz_gdf

def Check_Province(dfIn,province, th_boundary):
    dfIn.rename(columns={'Lat':'Latitude','Lng':'Longitude'},inplace=True)
    dfIn=Reverse_GeoCoding_ALL(dfIn, th_boundary)
    includeList=['hex_id', 'Latitude', 'Longitude', 'p_name_t']
    dfIn=dfIn[includeList].copy()
    dfIn=dfIn[dfIn['p_name_t']==province].copy().reset_index(drop=True)
    dfIn.rename(columns={'Latitude':'Lat','Longitude':'Lng'},inplace=True)
    # print(' Ring 2 : ',dfIn, ' :: ',dfIn.columns)
    return dfIn

# input - df: a Dataframe, chunkSize: the chunk size
# output - a list of DataFrame
# purpose - splits the DataFrame into smaller chunks
def split_dataframe(df, chunk_size): 
    chunks = list()
    # num_chunks = math.ceil(len(df) / chunk_size)
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks
#####################################

######################################
### specify current directory
file_path=os.getcwd()

dw_path='D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\'

geocode_path='D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ_geocode\\'

final_path='D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_Final\\'

## specify h3 resolution
h3_resolution=8
ring_level=2

### shape file
th_boundary_file='TH_tambon_boundary.shp'

### specify filename
xyz_population_file='tha_pd_2019_1km_UNadj_ASCII_XYZ.csv'

h3_population_lb8_file='population_Lv8_nonthaburi.csv'

##### temporaty parquet
xyz_population_parquet='xyz_population.parquet'

#### temporary output
temp_output='check_output.xlsx'
temp_shp='check_shapefile.shp'
filename_all='worldPop_allAge_2020'
filename_geocode='worldPop_allAge_2020_geocode'
filename_final='worldPop_allAge_2020_final'

### chunk_size
chunk_size=100000

##### provinceList
# provinceList=['กรุงเทพมหานคร','สมุทรปราการ','ปทุมธานี','นนทบุรี','นครราชสีมา','บุรีรัมย์','ยโสธร','ร้อยเอ็ด','ศรีสะเกษ','สุรินทร์','อำนาจเจริญ','อุบลราชธานี','เพชรบูรณ์','กำแพงเพชร','ชัยนาท','นครสวรรค์','พระนครศรีอยุธยา','พิจิตร','ลพบุรี','สระบุรี','สิงห์บุรี','อ่างทอง','อุทัยธานี',
#              'จันทบุรี','ฉะเชิงเทรา','ชลบุรี','ตราด','นครนายก','ปราจีนบุรี','ระยอง','สระแก้ว','เชียงใหม่','เชียงราย','แพร่','แม่ฮ่องสอน','ตาก','น่าน','พะเยา','พิษณุโลก','ลำปาง','ลำพูน','สุโขทัย','อุตรดิตถ์',
#              'เลย','กาฬสินธุ์','ขอนแก่น','ชัยภูมิ','นครพนม','บึงกาฬ','มหาสารคาม','มุกดาหาร','สกลนคร','หนองคาย','หนองบัวลำภู','อุดรธานี',
#             'เพชรบุรี','กาญจนบุรี','นครปฐม','ประจวบคีรีขันธ์','ราชบุรี','สมุทรสงคราม','สมุทรสาคร','สุพรรณบุรี',
#             'กระบี่','ชุมพร','ตรัง','นครศรีธรรมราช','นราธิวาส','ปัตตานี','พังงา','พัทลุง','ภูเก็ต','ยะลา','ระนอง','สงขลา','สตูล','สุราษฎร์ธานี']

# provinceList=['จันทบุรี','ฉะเชิงเทรา','ชลบุรี','ตราด','นครนายก','ปราจีนบุรี','ระยอง']  #1
# provinceList=['สระแก้ว','เชียงใหม่','เชียงราย','แพร่',] #2
# provinceList=['พะเยา','พิษณุโลก','ลำปาง','ลำพูน','สุโขทัย','อุตรดิตถ์']  #3
# provinceList=['กรุงเทพมหานคร','สมุทรปราการ','ปทุมธานี','นนทบุรี',]  #4

# provinceList=['นครราชสีมา','บุรีรัมย์','ยโสธร','ร้อยเอ็ด','ศรีสะเกษ','สุรินทร์','อำนาจเจริญ','อุบลราชธานี','เพชรบูรณ์','กำแพงเพชร','ชัยนาท','นครสวรรค์','พระนครศรีอยุธยา','พิจิตร','ลพบุรี','สระบุรี','สิงห์บุรี','อ่างทอง','อุทัยธานี']
# provinceList=['นครพนม','บึงกาฬ']  #1
# provinceList=['สกลนคร','หนองคาย']  #2
# provinceList=['นครศรีธรรมราช']  #3
provinceList=['นครปฐม']  #4     #'ประจวบคีรีขันธ์',

####################################
### read boundary file for checking province
th_boundary=gpd.read_file(file_path+'\\SHAPE\\'+th_boundary_file)

###################################
##set parameter for operation selection
if_integrate_file=0

if_reverse_geocode=0

if_integrate_geocode=0    ### integrate latlong to geocode and rawdata for each category to form the geocoded data for each province 

if_integrate_province_category=0   #### Integrate all geocoded data of each province into one file per province , final data

if_interpolation=0   ##### read final data to do interpolation to H# grid

if_interpolation_2=1
####################################
if(if_integrate_file==1):   
    ## list all geotiff parquet file
    fileList=glob.glob(dw_path+'\\'+"*.parquet")
    # print(' --- file :',fileList)

    ### initiate first dataframe
    print(' initial dataframe : ',fileList[0])
    file=fileList[0]
    filename=file.split('\\')[len(file.split('\\'))-1].split('.')[0]
    rawDf=pd.read_parquet(fileList[0])    
    rawDf.rename(columns={'z':filename},inplace=True)
    for file in tqdm(fileList[1:]):
        print(' next dataframe : ',file)
        filename=file.split('\\')[len(file.split('\\'))-1].split('.')[0]
        dummyDf=pd.read_parquet(file)
        dummyDf.rename(columns={'z':filename},inplace=True)
        rawDf=rawDf.merge(dummyDf, on=['x','y'], how='left')
        del dummyDf
    # rawDf.head(1000).to_excel(file_path+'\\'+'check_result.xlsx',index=False)
    rawDf.to_parquet(dw_path+'\\Age_XYZ_combined\\'+filename_all+'.parquet')
    del rawDf

if(if_reverse_geocode==1):   ### takes 3 hours    
    rawDf=pd.read_parquet(dw_path+'\\Age_XYZ_combined\\'+filename_all+'.parquet')
    includeList=['x','y']
    rawDf=rawDf[includeList].copy()
    rawDf.rename(columns={'x':'Longitude','y':'Latitude'},inplace=True)
    print(' ==>  input : ',len(rawDf),' :: ',rawDf.columns)

    mainDf=pd.DataFrame()
    dfList=split_dataframe(rawDf, chunk_size)
    del rawDf
    
    for df in tqdm(dfList):
        logging.warning(' :: Reverse geocoding - Start ')
        df=Reverse_GeoCoding(df, th_boundary)        
        logging.warning(' :: Reverse geocoding - End ')
        includeList=['Longitude', 'Latitude', 'p_name_t', 'a_name_t',  't_name_t', 's_region']
        df=df[includeList].copy()
        mainDf=mainDf.append(df).reset_index(drop=True)
        print(len(mainDf),' :: ===>  ',mainDf.columns)
    
    ## for testing    ##################################
    # mainDf.to_excel(file_path+'\\'+'check_result.xlsx',index=False)
    mainDf.to_parquet(geocode_path+'\\'+filename_geocode+'.parquet')
    mainDf=pd.read_parquet(geocode_path+'\\'+filename_geocode+'.parquet')
    print(filename_geocode,' ==>  input : ',len(mainDf),' :: ',mainDf.columns)
    logging.warning(' :: Complete ')    
    del dfList, mainDf

if(if_integrate_geocode==1):
    # data_name='tha_f_0_2020'
    fileList=glob.glob(dw_path+'\\'+"*.parquet")    
    # print(' file : ',fileList)

        # 'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_0_2020.parquet',
            #  'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_10_2020.parquet',
            #   'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_15_2020.parquet',
            #    'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_1_2020.parquet',
            #     'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_20_2020.parquet',
            #      'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_25_2020.parquet',
            #       'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_30_2020.parquet',
            #        'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_35_2020.parquet',
            # 'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_40_2020.parquet',
    # 1st run
    # fileList=[ 
    #         'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_45_2020.parquet', 
    #         'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_50_2020.parquet',
    #          'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_55_2020.parquet',
    #           'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_5_2020.parquet',
    #            'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_60_2020.parquet',
    #             'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_65_2020.parquet',
    #              'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_70_2020.parquet',
    #               'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_75_2020.parquet',
    #                'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_f_80_2020.parquet',
    #                 'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_0_2020.parquet', 
    #                 'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_10_2020.parquet',
    #                  'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_15_2020.parquet',
    #                   'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_1_2020.parquet']
                       
    ### 2nd run          
    fileList=  ['D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_20_2020.parquet', 
                       'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_25_2020.parquet', 
                       'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_30_2020.parquet',
                        'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_35_2020.parquet',
                         'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_40_2020.parquet',
                          'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_45_2020.parquet',
                           'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_50_2020.parquet', 
                           'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_55_2020.parquet', 
                           'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_5_2020.parquet',
                            'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_60_2020.parquet', 
                            'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_65_2020.parquet',
                             'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_70_2020.parquet',
                              'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_75_2020.parquet',
                               'D:\\DataWarehouse\\Population_Density\\Raw_Data\\Age_XYZ\\tha_m_80_2020.parquet']


    logging.warning(' :: Integrate Geocode - Start ')
    dfGeo=pd.read_parquet(geocode_path+'\\'+filename_geocode+'.parquet')
    print(' GEO ==>  input : ',len(dfGeo),' --- ',dfGeo.head(3),' :: ',dfGeo.columns)
    
    for file in tqdm(fileList):        
        file_name=file.split('\\')[len(file.split('\\'))-1].split('.')[0]
        print(' filename  : ',file_name)
        # # df_xyz=pd.read_parquet(dw_path+'\\Age_XYZ_combined\\'+filename_all+'.parquet')
        df_xyz=pd.read_parquet(file)
        df_xyz.rename(columns={'x':'Longitude','y':'Latitude', 'z':file_name},inplace=True)
        print(' XYZ ==>  input : ',len(df_xyz),' --- ',df_xyz.head(3),' :: ',df_xyz.columns)

        logging.warning(' :: Merging ')
        for province in tqdm(provinceList):

            #############################################################################
            ###########  create new directory if not exist ##############################
            path = geocode_path+'\\'+province+'\\'

            # Check whether the specified path exists or not
            isExist = os.path.exists(path)
            if not isExist:            
            # Create a new directory because it does not exist 
                os.makedirs(path)
                print("The new directory is created!", path)
            ############################################################################

            print(' --- > ',province)
            dfSel=dfGeo[dfGeo['p_name_t']==province].copy().reset_index(drop=True)
            dfSel=dfSel.merge(df_xyz, on=['Latitude','Longitude'], how='left', indicator=True)
            print(' Merged XYZ ==>  input : ',len(dfSel),' --- ',dfSel.head(3),' :: ',dfSel.columns)
            dfSel.to_parquet(path+'\\'+file_name+'!'+filename_final+'_'+province+'.parquet')
            del dfSel
    
    logging.warning(' :: Integrate Geocode - End ')
    del df_xyz, dfGeo

if(if_integrate_province_category==1):
    logging.warning(' :: Integrate Province + Category ')

    for province in tqdm(provinceList):   ### change list elements for testing here !!!
        data_path=geocode_path+'\\'+province+'\\'
        
        fileList=glob.glob(data_path+'\\'+"*.parquet")          
        print(' fileList : ',fileList)

        #### initial dataframe
        file=fileList[0]
        filename=file.split('\\')[len(file.split('\\'))-1].split('.')[0]
        columnName=filename.split('!')[0]
        print(' filename : ',filename,' :: ',columnName)        
        mainDf=pd.read_parquet(file)
        mainDf.drop(columns=['_merge'],inplace=True)

        for file in tqdm(fileList[1:]):   #### dont change [1:] this is correct !!!
            filename=file.split('\\')[len(file.split('\\'))-1].split('.')[0]
            columnName=filename.split('!')[0]
            print(' filename : ',filename,' :: ',columnName)
            dummyDf=pd.read_parquet(file)
            dummyDf.drop(columns=['_merge'],inplace=True)
            mainDf=mainDf.merge(dummyDf, on=['Latitude','Longitude', 'p_name_t', 'a_name_t', 't_name_t', 's_region'], how='left')
            print(len(mainDf),' ---- ', mainDf.head(3),' :: ',mainDf.columns)
            del dummyDf

        # for testing
        # mainDf.to_excel(file_path+'\\'+'check_merge.xlsx',index=False)
        mainDf.to_parquet(final_path+'\\'+str(province)+'_worldPop_2020_Age.parquet')
        # mainDf.to_csv(final_path+'\\'+str(province)+'_worldPop_2020_Age.csv')
        del mainDf

if(if_interpolation==1):
    ######## for testing ############
    # provinceList=['สมุทรปราการ']
    #################################
    logging.warning(' :: Interpolation***********  S t A t T  ***********')
    for province in tqdm(provinceList):        
        suffix='_worldPop_2020_Age.parquet'
        filename=province+suffix
        print(' ----> ',province,' :: ',filename)
        df_xyz=pd.read_parquet(final_path+'\\'+filename)
        print(len(df_xyz),' ---- df_xyz---- ',df_xyz.head(3),' :: ',df_xyz.columns)
        
        xyz_gdf=Generate_Plot_Shapefile(df_xyz,file_path,province, temp_shp, 0, h3_resolution)
        print(len(xyz_gdf),' ---- gdf xyz---- ',xyz_gdf.head(3),' :: ',xyz_gdf.columns)
        # xyz_gdf.to_excel(file_path+'\\'+'check_orginal_population.xlsx',index=False)
        del df_xyz  ## replace with xyz_gdf

        ############################################
        #### for testing
        # xyz_gdf=xyz_gdf.head(10000).copy()
        ############################################

        #### main dataframe
        mainDf=pd.DataFrame()
        columnList=['tha_f_0_2020','tha_f_10_2020','tha_f_15_2020','tha_f_1_2020','tha_f_20_2020','tha_f_25_2020',
        'tha_f_30_2020','tha_f_35_2020','tha_f_40_2020','tha_f_45_2020','tha_f_50_2020','tha_f_55_2020','tha_f_60_2020','tha_f_65_2020',
        'tha_f_70_2020','tha_f_75_2020','tha_f_80_2020',
        'tha_m_0_2020','tha_m_10_2020','tha_m_15_2020','tha_m_1_2020','tha_m_20_2020','tha_m_25_2020',
        'tha_m_30_2020','tha_m_35_2020','tha_m_40_2020','tha_m_45_2020','tha_m_50_2020','tha_m_55_2020','tha_m_60_2020','tha_m_65_2020',
        'tha_m_70_2020','tha_m_75_2020','tha_m_80_2020']
        

        def Exe_Interpolation(xyz_gdf, ring_level, province, th_boundary, colIn, colOut):
            subMainDf=pd.DataFrame()
            for lat, lng, hex_id, population in tqdm(zip(xyz_gdf['Latitude'],xyz_gdf['Longitude'],xyz_gdf['hex_id'],xyz_gdf[colIn])):
                # print(lat,' :: ',lng,' ==> ',hex_id,' :: ',population)
                dfDummy=pd.DataFrame(list(h3.k_ring(hex_id,ring_level)),columns=['hex_id'])
                dfDummy['Lat']=dfDummy.apply(lambda x: Exe_Lat_h3_to_geo(x['hex_id']) ,axis=1)
                dfDummy['Lng']=dfDummy.apply(lambda x: Exe_Lng_h3_to_geo(x['hex_id']) ,axis=1)
                # print(' Ring : ', dfDummy)
                dfDummy=Check_Province(dfDummy, province, th_boundary)

                dfDummy['radius']=dfDummy.apply(lambda x: haversine(lng, lat, x['Lng'], x['Lat']) ,axis=1 )
                dfDummy=dfDummy[dfDummy['radius']<=1.0].copy().reset_index(drop=True)    ### select only grid with radius less than 1 km
                totalDistance=dfDummy['radius'].sum()
                # print(' total : ', totalDistance)

                dfDummy['weight']=dfDummy['radius']/totalDistance
                dfDummy[colOut]=dfDummy['weight']*population

                includeList=['hex_id',colOut]
                dfDummy=dfDummy[includeList].copy()
                # print(dfDummy)       

                subMainDf=subMainDf.append(dfDummy).reset_index(drop=True)
                subMainDf=subMainDf.groupby(['hex_id'])[colOut].sum().reset_index()
                # print(len(subMainDf),' -- sub mainDf --- ',subMainDf, ' :: ',subMainDf.columns)
                del dfDummy
            return subMainDf

        mainDf=Exe_Interpolation(xyz_gdf, ring_level, province, th_boundary, 'tha_f_0_2020', 'age_0')

        for column in tqdm(columnList[1:]):
            ageStr=column.split('_')[2]
            print(' column ==> ', column)
            dfDummy=Exe_Interpolation(xyz_gdf, ring_level, province, th_boundary, column, 'age_'+ageStr)
            mainDf=mainDf.merge(dfDummy, on=['hex_id'],how='left')

        print(len(mainDf),' -- mainDf --- ',mainDf.head(3), ' :: ',mainDf.columns) 

        crs = {'init':'EPSG:4326'}  
        geomList=[]
        for hexId in tqdm(mainDf['hex_id']): 
            geomList.append(Generate_HexGeometry_2(hexId))
        final_gdf=gpd.GeoDataFrame(mainDf,   crs = crs, geometry=geomList)
        del mainDf

        print(len(final_gdf),' --- FINAL -- ', final_gdf,' :: ',final_gdf.columns)
        final_gdf.to_csv(file_path+'\\'+province+'_check_int_population.csv',index=False)   ### check if interpolated population in each province is still the same as that of the original data
        # final_gdf.to_excel(file_path+'\\'+province+'_check_int_population.xlsx',index=False)   ### check if interpolated population in each province is still the same as that of the original data
        # final_gdf.to_file(file_path+'\\Output\\'+temp_shp) 
        final_gdf.to_parquet(final_path+'\\result\\'+province+'_worldPop2020_int_h3Lv3.parquet')

        del xyz_gdf
        del final_gdf

    
if(if_interpolation_2==1):
    #################################
    logging.warning(' :: Interpolation***********  S t A t T  ***********')
    for province in tqdm(provinceList):        
        suffix='_worldPop_2020_Age.parquet'
        filename=province+suffix
        print(' ----> ',province,' :: ',filename)
        df_xyz=pd.read_parquet(final_path+'\\'+filename)
        print(len(df_xyz),' ---- df_xyz---- ',df_xyz.head(3),' :: ',df_xyz.columns)
        
        xyz_gdf=Generate_Plot_Shapefile(df_xyz,file_path,province, temp_shp, 0, h3_resolution)
        print(len(xyz_gdf),' ---- gdf xyz---- ',xyz_gdf.head(3),' :: ',xyz_gdf.columns)
        # xyz_gdf.to_excel(file_path+'\\'+'check_orginal_population.xlsx',index=False)
        del df_xyz  ## replace with xyz_gdf

        ############################################
        #### for testing
        # xyz_gdf=xyz_gdf.head(10000).copy()
        ############################################

        #### main dataframe
        mainDf=pd.DataFrame()
        columnList=['tha_f_0_2020','tha_f_10_2020','tha_f_15_2020','tha_f_1_2020','tha_f_20_2020','tha_f_25_2020',
        'tha_f_30_2020','tha_f_35_2020','tha_f_40_2020','tha_f_45_2020','tha_f_50_2020','tha_f_55_2020','tha_f_60_2020','tha_f_65_2020',
        'tha_f_70_2020','tha_f_75_2020','tha_f_80_2020',
        'tha_m_0_2020','tha_m_10_2020','tha_m_15_2020','tha_m_1_2020','tha_m_20_2020','tha_m_25_2020',
        'tha_m_30_2020','tha_m_35_2020','tha_m_40_2020','tha_m_45_2020','tha_m_50_2020','tha_m_55_2020','tha_m_60_2020','tha_m_65_2020',
        'tha_m_70_2020','tha_m_75_2020','tha_m_80_2020']  #34
        

        def Exe_Interpolation(xyz_gdf, ring_level, province, th_boundary, colIn, colOut):
            subMainDf=pd.DataFrame()
            for lat, lng, hex_id, population in tqdm(zip(xyz_gdf['Latitude'],xyz_gdf['Longitude'],xyz_gdf['hex_id'],xyz_gdf[colIn])):
                # print(lat,' :: ',lng,' ==> ',hex_id,' :: ',population)
                dfDummy=pd.DataFrame(list(h3.k_ring(hex_id,ring_level)),columns=['hex_id'])
                dfDummy['Lat']=dfDummy.apply(lambda x: Exe_Lat_h3_to_geo(x['hex_id']) ,axis=1)
                dfDummy['Lng']=dfDummy.apply(lambda x: Exe_Lng_h3_to_geo(x['hex_id']) ,axis=1)
                # print(' Ring : ', dfDummy)
                dfDummy=Check_Province(dfDummy, province, th_boundary)

                dfDummy['radius']=dfDummy.apply(lambda x: haversine(lng, lat, x['Lng'], x['Lat']) ,axis=1 )
                dfDummy=dfDummy[dfDummy['radius']<=1.0].copy().reset_index(drop=True)    ### select only grid with radius less than 1 km
                totalDistance=dfDummy['radius'].sum()
                # print(' total : ', totalDistance)

                dfDummy['weight']=dfDummy['radius']/totalDistance
                dfDummy[colOut]=dfDummy['weight']*population

                includeList=['hex_id',colOut]
                dfDummy=dfDummy[includeList].copy()
                # print(dfDummy)       

                subMainDf=subMainDf.append(dfDummy).reset_index(drop=True)
                subMainDf=subMainDf.groupby(['hex_id'])[colOut].sum().reset_index()
                # print(len(subMainDf),' -- sub mainDf --- ',subMainDf, ' :: ',subMainDf.columns)
                del dfDummy
            return subMainDf
        #####################################################
        def Exe_Interpolation_ALL(xyz_gdf, ring_level, province, th_boundary, columnList):
            subMainDf=pd.DataFrame()
            xyz_gdf['ref']=xyz_gdf['Latitude'].astype(str)+'!'+xyz_gdf['Longitude'].astype(str)+'!'+xyz_gdf['hex_id'].astype(str)
            xyzList=list(xyz_gdf['ref'])
            for ref in tqdm(xyzList):
                dfDummy_0=xyz_gdf[xyz_gdf['ref']==ref].copy().reset_index(drop=True)
                hex_id=list(dfDummy_0['hex_id'].values)[0]
                lat=list(dfDummy_0['Latitude'].values)[0]
                lng=list(dfDummy_0['Longitude'].values)[0]
                # print(lat,' :: ',lng,' ==> ',hex_id,' :: ')
                dfDummy=pd.DataFrame(list(h3.k_ring(hex_id,ring_level)),columns=['hex_id'])
                dfDummy['Lat']=dfDummy.apply(lambda x: Exe_Lat_h3_to_geo(x['hex_id']) ,axis=1)
                dfDummy['Lng']=dfDummy.apply(lambda x: Exe_Lng_h3_to_geo(x['hex_id']) ,axis=1)
                # print(' Ring : ', dfDummy)
                dfDummy=Check_Province(dfDummy, province, th_boundary)
                if(len(dfDummy)>0):

                    dfDummy['radius']=dfDummy.apply(lambda x: haversine(lng, lat, x['Lng'], x['Lat']) ,axis=1 )
                    dfDummy=dfDummy[dfDummy['radius']<=1.5].copy().reset_index(drop=True)    ### select only grid with radius less than 1 km
                    totalDistance=dfDummy['radius'].sum()
                    # print(' total : ', totalDistance)

                    dfDummy['weight']=dfDummy['radius']/totalDistance
                    for col in columnList:
                        dfDummy[col]=dfDummy['weight']*list(dfDummy_0[col].values)[0]

                    includeList=['hex_id']+columnList
                    dfDummy=dfDummy[includeList].copy()
                    # print(dfDummy)       

                    subMainDf=subMainDf.append(dfDummy).reset_index(drop=True)
                    subMainDf=subMainDf.groupby(['hex_id'])[columnList].sum().reset_index()
                    # print(len(subMainDf),' -- sub mainDf --- ',subMainDf, ' :: ',subMainDf.columns)
                del dfDummy
            return subMainDf

        mainDf=Exe_Interpolation_ALL(xyz_gdf, ring_level, province, th_boundary, columnList)
        ######################################################
        print(len(mainDf),' -- mainDf --- ',mainDf.head(3), ' :: ',mainDf.columns) 

        crs = {'init':'EPSG:4326'}  
        geomList=[]
        for hexId in tqdm(mainDf['hex_id']): 
            geomList.append(Generate_HexGeometry_2(hexId))
        final_gdf=gpd.GeoDataFrame(mainDf,   crs = crs, geometry=geomList)
        del mainDf

        print(len(final_gdf),' --- FINAL -- ', final_gdf,' :: ',final_gdf.columns)
        final_gdf.to_csv(file_path+'\\'+province+'_check_int_population.csv',index=False)   ### check if interpolated population in each province is still the same as that of the original data
        # final_gdf.to_excel(file_path+'\\'+province+'_check_int_population.xlsx',index=False)   ### check if interpolated population in each province is still the same as that of the original data
        # final_gdf.to_file(file_path+'\\Output\\'+temp_shp) 
        final_gdf.to_parquet(final_path+'\\result\\'+province+'_worldPop2020_int_h3Lv3.parquet')

        del xyz_gdf
        del final_gdf
    
    logging.warning(' :: Interpolation -------------- E n D -------------- ')



