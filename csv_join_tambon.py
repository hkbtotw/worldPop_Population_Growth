# Use geo_env_2 environment

# -*- coding: utf-8 -*-
# import os, sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os



def Reverse_GeoCoding(Inputdata, th_boundary):
    #---------------------INPUT SHAPE---------------------
    #path = 'D:/LAB/geopandas_tuitor/'
    # path= os.getcwd()+'\\SHAPE\\'
    # # path= 'C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Python_Geomap\\SHAPE\\'
    # # Importing Thailand ESRI Shapefile 
    # th_boundary = gpd.read_file(path+'TH_tambon_boundary.shp')
    # #A_boundary = gpd.read_file(path+'TH_amphoe_border.shp')
    # #P_boundary = gpd.read_file(path+'TH_province.shp')

    #---------------------INPUT POINT---------------------

    cvm_geo = [Point(xy) for xy in zip(Inputdata['Longitude'].astype(float),Inputdata['Latitude'].astype(float))]
    Inputdata = gpd.GeoDataFrame(Inputdata, geometry = cvm_geo)
    Inputdata.set_crs(epsg=4326, inplace=True)
    Inputdata = Inputdata.to_crs(epsg=32647)
    #cvm_point.plot()

    #--------------------- Spatial Join------------------
    output = gpd.sjoin(Inputdata,th_boundary, how = 'inner', op = 'intersects')
    # output = gpd.sjoin(Inputdata,th_boundary, how = 'left', op = 'intersects')
    output=output.reset_index(drop=True)
    #---------------------- SAVE FILE ------------------
    return output


def Reverse_GeoCoding_Province(Inputdata, th_boundary, province):
    #---------------------INPUT SHAPE---------------------
    #path = 'D:/LAB/geopandas_tuitor/'
    # path= os.getcwd()+'\\SHAPE\\'
    # # path= 'C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Python_Geomap\\SHAPE\\'
    # # Importing Thailand ESRI Shapefile 
    # th_boundary = gpd.read_file(path+'TH_tambon_boundary.shp')
    # #A_boundary = gpd.read_file(path+'TH_amphoe_border.shp')
    # #P_boundary = gpd.read_file(path+'TH_province.shp')

    #---------------------INPUT POINT---------------------
    th_boundary=th_boundary[th_boundary['p_name_t']==province].copy().reset_index(drop=True)
    cvm_geo = [Point(xy) for xy in zip(Inputdata['Longitude'].astype(float),Inputdata['Latitude'].astype(float))]
    Inputdata = gpd.GeoDataFrame(Inputdata, geometry = cvm_geo)
    Inputdata.set_crs(epsg=4326, inplace=True)
    Inputdata = Inputdata.to_crs(epsg=32647)
    #cvm_point.plot()

    #--------------------- Spatial Join------------------
    output = gpd.sjoin(Inputdata,th_boundary, how = 'inner', op = 'intersects')
    # output = gpd.sjoin(Inputdata,th_boundary, how = 'left', op = 'intersects')
    output=output.reset_index(drop=True)
    #---------------------- SAVE FILE ------------------
    return output

def Reverse_GeoCoding_ALL(Inputdata, th_boundary):
    #---------------------INPUT SHAPE---------------------
    #path = 'D:/LAB/geopandas_tuitor/'
    # path= os.getcwd()+'\\SHAPE\\'
    # # path= 'C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Python_Geomap\\SHAPE\\'
    # # Importing Thailand ESRI Shapefile 
    # th_boundary = gpd.read_file(path+'TH_tambon_boundary.shp')
    # #A_boundary = gpd.read_file(path+'TH_amphoe_border.shp')
    # #P_boundary = gpd.read_file(path+'TH_province.shp')

    #---------------------INPUT POINT---------------------

    cvm_geo = [Point(xy) for xy in zip(Inputdata['Longitude'].astype(float),Inputdata['Latitude'].astype(float))]
    Inputdata = gpd.GeoDataFrame(Inputdata, geometry = cvm_geo)
    Inputdata.set_crs(epsg=4326, inplace=True)
    Inputdata = Inputdata.to_crs(epsg=32647)
    #cvm_point.plot()

    #--------------------- Spatial Join------------------
    # output = gpd.sjoin(Inputdata,th_boundary, how = 'inner', op = 'intersects')
    output = gpd.sjoin(Inputdata,th_boundary, how = 'left', op = 'intersects')
    output=output.reset_index(drop=True)
    #---------------------- SAVE FILE ------------------
    return output
