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
from csv_join_tambon import Reverse_GeoCoding, Reverse_GeoCoding_ALL
from Credential import *
from math import radians, cos, sin, asin, sqrt
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
from shapely import wkt

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

################ Write data
def Write_Population_WorldPop_H3Lv8_2019(df_input):
    print('------------- Start WriteDB : Population_WorldPop_H3Lv8_2019-----------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    # df_input=df_input.replace({np.nan:None})
    df_write=df_input
    # print(' col : ',df_write.columns)

	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad    

    #- View all records from the table
    
    sql="""select * from [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2019]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2019](	    
         [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[pop]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[DBCreatedAt]
  
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?
    )""",
        row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['pop']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['DBCreatedAt']
  
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Write_Population_WorldPop_H3Lv8_2020(df_input):
    print('------------- Start WriteDB : Population_WorldPop_H3Lv8_2020-----------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    # df_input=df_input.replace({np.nan:None})
    df_write=df_input
    # print(' col : ',df_write.columns)

	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad    

    #- View all records from the table
    
    sql="""select * from [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2020]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2020](	    
         [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[pop]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[DBCreatedAt]
  
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?
    )""",
        row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['pop']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['DBCreatedAt']
  
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Write_Population_WorldPop_H3Lv8_2018(df_input):
    print('------------- Start WriteDB : Population_WorldPop_H3Lv8_2018-----------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    # df_input=df_input.replace({np.nan:None})
    df_write=df_input
    # print(' col : ',df_write.columns)

	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad    

    #- View all records from the table
    
    sql="""select * from [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2018]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2018](	    
         [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[pop]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[DBCreatedAt]
  
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?
    )""",
        row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['pop']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['DBCreatedAt']
  
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Write_Population_WorldPop_H3Lv8_2017(df_input):
    print('------------- Start WriteDB : Population_WorldPop_H3Lv8_2017-----------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    # df_input=df_input.replace({np.nan:None})
    df_write=df_input
    # print(' col : ',df_write.columns)

	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad    

    #- View all records from the table
    
    sql="""select * from [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2017]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2017](	    
         [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[pop]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[DBCreatedAt]
  
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?
    )""",
        row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['pop']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['DBCreatedAt']
  
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Write_Population_WorldPop_H3Lv8_2016(df_input):
    print('------------- Start WriteDB : Population_WorldPop_H3Lv8_2016-----------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    # df_input=df_input.replace({np.nan:None})
    df_write=df_input
    # print(' col : ',df_write.columns)

	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad    

    #- View all records from the table
    
    sql="""select * from [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2016]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2016](	    
         [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[pop]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[DBCreatedAt]
  
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?
    )""",
        row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['pop']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['DBCreatedAt']
  
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')


def Write_Thailand_worldPop_H3Lv8_2016_2020(df_input):
    print('------------- Start WriteDB : Thailand_worldPop_H3Lv8_2016_2020-----------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    # df_input=df_input.replace({np.nan:None})
    df_write=df_input
    # print(' col : ',df_write.columns)

	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad    

    #- View all records from the table
    
    sql="""select * from [TSR_ADHOC].[dbo].[Thailand_worldPop_H3Lv8_2016_2020]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[Thailand_worldPop_H3Lv8_2016_2020](	    
         [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[Pop_2016]
      ,[Pop_2017]
      ,[Pop_2018]
      ,[Pop_2019]
      ,[Pop_2020]
      ,[PopG_5yr]
      ,[PopG_3yr]
      ,[PopG_1yr]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[DBCreatedAt]
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,?,
    ?,?
    )""",
        row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['Pop_2016']
      ,row['Pop_2017']
      ,row['Pop_2018']
      ,row['Pop_2019']
      ,row['Pop_2020']
      ,row['PopG_5yr']
      ,row['PopG_3yr']
      ,row['PopG_1yr']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['DBCreatedAt']
  
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

################### Read data
def Read_Population_WorldPop_H3Lv8_2016(province):
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT   [hex_id]
                    ,[Latitude]
                    ,[Longitude]
                    ,[pop]
                    ,[geometry]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[s_region]
                    ,[DBCreatedAt]
                FROM [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2016]   
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def Read_Population_WorldPop_H3Lv8_2017(province):
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT   [hex_id]
                    ,[Latitude]
                    ,[Longitude]
                    ,[pop]
                    ,[geometry]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[s_region]
                    ,[DBCreatedAt]
                FROM [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2017]   
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def Read_Population_WorldPop_H3Lv8_2018(province):
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT   [hex_id]
                    ,[Latitude]
                    ,[Longitude]
                    ,[pop]
                    ,[geometry]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[s_region]
                    ,[DBCreatedAt]
                FROM [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2018]   
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def Read_Population_WorldPop_H3Lv8_2019(province):
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT   [hex_id]
                    ,[Latitude]
                    ,[Longitude]
                    ,[pop]
                    ,[geometry]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[s_region]
                    ,[DBCreatedAt]
                FROM [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2019]   
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def Read_Population_WorldPop_H3Lv8_2020(province):
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT   [hex_id]
                    ,[Latitude]
                    ,[Longitude]
                    ,[pop]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[s_region]
                    ,[DBCreatedAt]
                FROM [TSR_ADHOC].[dbo].[Population_WorldPop_H3Lv8_2020]   
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')
    return dfout

def Get_Population_Data(province,year):
    if(year=='2016'):
        df_out=Read_Population_WorldPop_H3Lv8_2016(province)
    elif(year=='2017'):
        df_out=Read_Population_WorldPop_H3Lv8_2017(province)
    elif(year=='2018'):
        df_out=Read_Population_WorldPop_H3Lv8_2018(province)
    elif(year=='2019'):
        df_out=Read_Population_WorldPop_H3Lv8_2019(province)
    elif(year=='2020'):
        df_out=Read_Population_WorldPop_H3Lv8_2020(province)
    else:
        print(' ---- E r R o R -----')
        exit()
    return df_out

#####################################

######################################
### specify current directory
file_path=os.getcwd()

output_path='D:\\DataWarehouse\\Population_Density\\Population_Output\\'

upload_input_path='D:\\DataWarehouse\\Population_Density\\Population_Output\\parquet\\'

## specify h3 resolution
h3_resolution=8
ring_level=2

### shape file
th_boundary_file='TH_tambon_boundary.shp'

############################################################
### change here to select input data for interpolation 
year='2016'     ### DONE '2018' '2019'  '2020'
### specify filename
xyz_population_file='tha_pd_'+year+'_1km_UNadj_ASCII_XYZ.csv'
###########################################################


h3_population_lb8_file='population_Lv8_nonthaburi.csv'

##### temporaty parquet
xyz_population_parquet='xyz_population.parquet'

#### temporary output
temp_output='check_output.xlsx'
temp_shp='population_worldPop_'+year+'_shapefile.shp'

#######################################
#### select Operational Parameters
if_create_population_data=0

if_upload_data=0

if_combine_data=1
######################################
### yearList
yearList=['2016','2017','2018','2019','2020']

##### provinceList
# provinceList=['กรุงเทพมหานคร','สมุทรปราการ','ปทุมธานี','นนทบุรี',
provinceList=['นครราชสีมา','บุรีรัมย์','ยโสธร','ร้อยเอ็ด','ศรีสะเกษ','สุรินทร์','อำนาจเจริญ','อุบลราชธานี',
                'เพชรบูรณ์','กำแพงเพชร','ชัยนาท','นครสวรรค์','พระนครศรีอยุธยา','พิจิตร',
                'ลพบุรี','สระบุรี','สิงห์บุรี','อ่างทอง','อุทัยธานี','จันทบุรี','ฉะเชิงเทรา','ชลบุรี','ตราด','นครนายก',
                'ปราจีนบุรี','ระยอง','สระแก้ว','เชียงใหม่','เชียงราย','แพร่','แม่ฮ่องสอน','ตาก',
                'น่าน','พะเยา','พิษณุโลก','ลำปาง','ลำพูน','สุโขทัย','อุตรดิตถ์',
             'เลย','กาฬสินธุ์','ขอนแก่น','ชัยภูมิ','นครพนม','บึงกาฬ','มหาสารคาม','มุกดาหาร','สกลนคร','หนองคาย','หนองบัวลำภู','อุดรธานี',
            'เพชรบุรี','กาญจนบุรี','นครปฐม','ประจวบคีรีขันธ์','ราชบุรี','สมุทรสงคราม','สมุทรสาคร','สุพรรณบุรี',
            'กระบี่','ชุมพร','ตรัง','นครศรีธรรมราช','นราธิวาส','ปัตตานี','พังงา','พัทลุง','ภูเก็ต','ยะลา','ระนอง','สงขลา','สตูล','สุราษฎร์ธานี']
# provinceList=['จันทบุรี']
####################################
### read boundary file for checking province
th_boundary=gpd.read_file(file_path+'\\SHAPE\\'+th_boundary_file)

###################################

if(if_create_population_data==1):
    #### xyz population
    ############## real operation
    df_xyz=pd.read_csv(file_path+'\\Raw_Data\\'+xyz_population_file)
    df_xyz.rename(columns={'X':'Longitude','Y':'Latitude','Z':'population'}, inplace=True)
    ###### assign physical location
    df_xyz=Reverse_GeoCoding_ALL(df_xyz, th_boundary)
    df_xyz.to_parquet(file_path+'\\Parquet\\'+xyz_population_parquet)
    ############### for testing
    df_xyz=pd.read_parquet(file_path+'\\Parquet\\'+xyz_population_parquet)
    print(len(df_xyz),' --- xyz ----', df_xyz.head(3),' ::: ',df_xyz.columns)
    # df_xyz[df_xyz['p_name_t']=='นนทบุรี'].to_excel(file_path+'\\'+temp_output,index=False)

    includeList=['Longitude', 'Latitude', 'population', 'p_name_t',  'a_name_t',
            't_name_t', 's_region']
    df_xyz=df_xyz[includeList].copy()

    print(len(df_xyz),' ---- df_xyz---- ',df_xyz.head(3),' :: ',df_xyz.columns)

    ############## select Provicne
    # provinceList=['นนทบุรี']
    ##################################
    # populationDf=pd.DataFrame()
    for province in tqdm(provinceList):
        print(' ====> ',province)

        xyz_gdf=Generate_Plot_Shapefile(df_xyz,file_path,province, temp_shp, 0, h3_resolution)
        print(len(xyz_gdf),' ---- gdf xyz---- ',xyz_gdf.head(3),' :: ',xyz_gdf.columns)
        # xyz_gdf.to_excel(file_path+'\\'+'check_orginal_population.xlsx',index=False)
    

        ####for testing
        # xyz_gdf=xyz_gdf.head(1).copy()
        #################

        #### main dataframe
        mainDf=pd.DataFrame()

        for lat, lng, hex_id, population in tqdm(zip(xyz_gdf['Latitude'],xyz_gdf['Longitude'],xyz_gdf['hex_id'],xyz_gdf['population'])):
            # print(lat,' :: ',lng,' ==> ',hex_id,' :: ',population)
            dfDummy=pd.DataFrame(list(h3.k_ring(hex_id,ring_level)),columns=['hex_id'])
            dfDummy['Lat']=dfDummy.apply(lambda x: Exe_Lat_h3_to_geo(x['hex_id']) ,axis=1)
            dfDummy['Lng']=dfDummy.apply(lambda x: Exe_Lng_h3_to_geo(x['hex_id']) ,axis=1)
            # print(' Ring : ', dfDummy)

            dfDummy=Check_Province(dfDummy, province, th_boundary)
            if(len(dfDummy)>0):
                # print(hex_id,'  1 :: ',dfDummy)
                dfDummy['radius']=dfDummy.apply(lambda x: haversine(lng, lat, x['Lng'], x['Lat']) ,axis=1 )
                # print(hex_id,' 2 :: ',dfDummy)
                dfDummy=dfDummy[dfDummy['radius']<=1.5].copy().reset_index(drop=True)    ### select only grid with radius less than 1 km
                totalDistance=dfDummy['radius'].sum()
                # print(' total : ', totalDistance)

                dfDummy['weight']=dfDummy['radius']/totalDistance
                dfDummy['pop']=dfDummy['weight']*population

                includeList=['hex_id','pop']
                dfDummy=dfDummy[includeList].copy()
                # print(dfDummy)       

                mainDf=mainDf.append(dfDummy).reset_index(drop=True)
                mainDf=mainDf.groupby(['hex_id'])['pop'].sum().reset_index()
                # print(len(mainDf),' -- mainDf --- ',mainDf, ' :: ',mainDf.columns)
            del dfDummy
        del xyz_gdf

        crs = {'init':'EPSG:4326'}  
        geomList=[]
        for hexId in tqdm(mainDf['hex_id']): 
            geomList.append(Generate_HexGeometry_2(hexId))
        final_gdf=gpd.GeoDataFrame(mainDf,   crs = crs, geometry=geomList)
        del mainDf

        print(len(final_gdf),' --- FINAL -- ', final_gdf,' :: ',final_gdf.columns)
        # final_gdf.to_excel(file_path+'\\'+'check_int_population.xlsx',index=False)   ### check if interpolated population in each province is still the same as that of the original data
        final_gdf.to_file(output_path+'\\'+province+'_'+temp_shp) 
        final_gdf.to_parquet(output_path+'\\parquet\\'+province+'_WorldPop'+year+'_h3Lv8.parquet') 

        # populationDf=populationDf.append(final_gdf).reset_index(drop=True)

        del final_gdf

    # populationDf.to_parquet(output_path+'\\'+'Thailand_WorldPop2020_h3Lv8.parquet')
    # del populationDf
    del df_xyz  ## replace with xyz_gdf
    print('*********************************************')
    print('***** C o M p L e T e************************')
    print('*********************************************')

if(if_upload_data==1):
    print(' --- Uploading ---')
    for province in tqdm(provinceList):
        print(' ==> ',province)
        suffix='_WorldPop'+year+'_h3Lv8.parquet'
        filename=province+suffix
        df_xyz=gpd.read_parquet(upload_input_path+'\\'+filename)
        # print(len(df_xyz),' ---- xyz ----',df_xyz.head(3),' :: ',df_xyz.columns)

        includeList=['hex_id','geometry']
        df_geometry=df_xyz[includeList].copy()

        df_xyz['Latitude']=df_xyz.apply(lambda x: Exe_Lat_h3_to_geo(x['hex_id'])  ,axis=1)
        df_xyz['Longitude']=df_xyz.apply(lambda x: Exe_Lng_h3_to_geo(x['hex_id'])  ,axis=1)

        df_xyz=Reverse_GeoCoding(df_xyz,th_boundary)        
        includeList=['hex_id', 'pop', 'Latitude', 'Longitude', 'geometry','p_name_t',  'a_name_t', 't_name_t', 's_region']
        df_xyz=df_xyz[includeList].copy()
        df_xyz.drop(columns=['geometry'],inplace=True)
        df_xyz=df_xyz.merge(df_geometry, on=['hex_id'], how='left')

        ### convert geometry to WKT (well known string) to write on the database
        df_xyz['geometry'] = df_xyz.geometry.apply(lambda x: wkt.dumps(x))

        del df_geometry
        df_xyz['DBCreatedAt']=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        print(len(df_xyz),' ---- xyz ----',df_xyz.head(3),' :: ',df_xyz.columns)

        ##### Write Data ###
        print(' Writing : ',province,' . ')
        if(year=='2016'):
            print(' write : ',year)
            Write_Population_WorldPop_H3Lv8_2016(df_xyz)
        elif(year=='2017'):
            print(' write : ',year)
            Write_Population_WorldPop_H3Lv8_2017(df_xyz)
        elif(year=='2018'):
            print(' write : ',year)
            Write_Population_WorldPop_H3Lv8_2018(df_xyz)
        elif(year=='2019'):
            print(' write : ',year)
            Write_Population_WorldPop_H3Lv8_2019(df_xyz)
        elif(year=='2020'):
            print(' write : ',year)
            Write_Population_WorldPop_H3Lv8_2020(df_xyz)
        else:
            print(' ***** E r R o R *****')
        #######################################


        del df_xyz





    print('*********************************************')
    print('***** C o M p L e T e************************')
    print('*********************************************')

if(if_combine_data==1):
    print(' ---- Combining 5 years data -----')
    mainDf=pd.DataFrame()
    for province in tqdm(provinceList):
        year=yearList[0]
        print(province,' ===> ',year)        
        dfPop=Get_Population_Data(province,year)
        column_name='Pop_'+year
        dfPop.rename(columns={'pop':column_name},inplace=True) 
        for year in tqdm(yearList[1:]):
            print(province,' ===> ',year)        
            dfDummy=Get_Population_Data(province,year)
            column_name='Pop_'+year
            dfDummy.rename(columns={'pop':column_name},inplace=True)        
            includeList=['hex_id',column_name]
            dfDummy=dfDummy[includeList].copy()
            dfPop=dfPop.merge(dfDummy,on=['hex_id'], how='left')
            del dfDummy
        dfPop['PopG_5yr']=(dfPop['Pop_2020']-dfPop['Pop_2016'])/dfPop['Pop_2016']
        dfPop['PopG_3yr']=(dfPop['Pop_2020']-dfPop['Pop_2018'])/dfPop['Pop_2018']
        dfPop['PopG_1yr']=(dfPop['Pop_2020']-dfPop['Pop_2019'])/dfPop['Pop_2019']
        print(len(dfPop),' ---- population ---- ',dfPop.head(3),' :: ',dfPop.columns)
        mainDf=mainDf.append(dfPop).reset_index(drop=True)
        # dfPop.to_excel(file_path+'\\'+'check_5years_population.xlsx',index=False)
        print('*******************************************************************************')
        print('*********** Write Start  ******************************************************')
        Write_Thailand_worldPop_H3Lv8_2016_2020(dfPop)
        print('*******************************************************************************')
        del dfPop
    mainDf['DBCreatedAt']=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    print(len(mainDf),' ----  main population ---- ',mainDf.head(3),' :: ',mainDf.columns)
    # mainDf.to_parquet(output_path+'\\DB_parquet\\'+'Thailand_worldPop_2016_2020.parquet')
    
    # print('*******************************************************************************')
    # print('*********** Write Start  ******************************************************')
    # Write_Thailand_worldPop_H3Lv8_2016_2020(mainDf)
    # print('*******************************************************************************')


    del mainDf