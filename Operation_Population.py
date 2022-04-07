from h3 import h3
from datetime import datetime, date, timedelta
import pandas as pd
import  numpy as np
from geopandas import GeoDataFrame
import geopandas as gpd
from shapely.geometry import Point
import pickle
from Credential import *
import glob
import pyproj    #to convert coordinate system
from shapely.geometry import Polygon, mapping
import psycopg2
import os
from tqdm import *

from PIL import Image

##base64 conversion
import base64
from base64 import *

#enable tqdm with pandas, progress_apply
tqdm.pandas()


# start_datetime = datetime.now()
# print (start_datetime,'execute')
# todayStr=date.today().strftime('%Y-%m-%d')
# nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
# print("TodayStr's date:", todayStr,' -- ',type(todayStr))
# print("nowStr's date:", nowStr,' -- ',type(nowStr))



def Read_AGP_Image_agent():
    print('------------- Start ReadDB : AGP_Image_agent------------')
    
	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad        

     #- View all records from the table    
    sql="""select *  from [TSR_ADHOC].[dbo].[AGP_Image_agent]  """
   
    cursor=conn.cursor()
    dfout=pd.read_sql(sql,conn)

    # print(' dfout : ',dfout)

    cursor.close()
    #conn.close()
    print('------------Complete ReadDB-------------')
    return dfout

def Clear_AGP_Image_agent():
    print('------------- Start WriteDB: Clear AGP Image_agent -------------')
	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn = connect_tad_image

    #- View all records from the table
    sql="""delete from  [TSR_ADHOC].[dbo].[AGP_Image_agent] """   
  
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete CLEAR-------------')

def Write_AGP_Image_agent(df_input):
    print('------------- Start WriteDB: AGP Image_agent-------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn = connect_tad_image

    #- View all records from the table    
    sql="""select * from  [TSR_ADHOC].[dbo].[AGP_Image_agent] """   
    
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[AGP_Image_agent](		
      [CustomerId]
      ,[ref]
      ,[image]
      ,[category]
      ,[CollectedAt]    
	)     
    values(?,?,?,?,?
    
    )""", 
       row['CustomerId']
       , row['ref']
       , row['image']
       , row['category']      
      ,row['CollectedAt']
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Write_AGP_Image_tracker(df_input):
    print('------------- Start WriteDB : AGP_Image_tracker------------')
    
	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn =connect_tad        

     #- View all records from the table    
    sql="""delete  from [TSR_ADHOC].[dbo].[AGP_Image_tracker]  """
   
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    cursor=conn.cursor()
    
    for index, row in tqdm(df_input.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[AGP_Image_tracker](	    
         [CustomerId]
      ,[ref]     
      ,[CollectedAt]      
	)     
    values(?,?,?
    )""",
        row['CustomerId']
        ,row['ref']  
      ,row['CollectedAt']
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Clear_AGP_SalesArea_Link():
    print('------------- Start WriteDB: Clear AGP SalesArea_Link -------------')
	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn = connect_tad_image

    #- View all records from the table
    sql="""delete from  [TSR_ADHOC].[dbo].[AGP_SalesArea_Link] """   
  
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete CLEAR-------------')

def Clear_AGP_SalesArea_Link_2():
    print('------------- Start WriteDB: Clear AGP SalesArea_Link 2-------------')
	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn = connect_tad_image

    #- View all records from the table
    sql="""delete from  [TSR_ADHOC].[dbo].[AGP_SalesArea_Link_2] """   
  
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete CLEAR-------------')

def Write_AGP_SalesArea_Link_2(df_input):
    print('------------- Start WriteDB: AGP SalesArea_Link 2-------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn = connect_tad_image

    #- View all records from the table
    # sql="""delete from  [TSR_ADHOC].[dbo].[AGP_SalesArea_Link_2] """   
    sql="""select * from  [TSR_ADHOC].[dbo].[AGP_SalesArea_Link_2] """   
    
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[AGP_SalesArea_Link_2](		
       [Agent]
      ,[CUST_NM]
      ,[Active_Sub]
      ,[Active_Sub_Target]
      ,[Active_Beer]
      ,[Active_Beer_Target]
      ,[Active_Spirits]
      ,[Active_Spirits_Target]
      ,[Active_NAB]
      ,[Active_NAB_Target]
      ,[Active_Others]
      ,[Active_Others_Target]
      ,[Link]
      ,[Link_Agent]
      ,[LAT]
      ,[LNG]
      ,[s_region]
      ,[p_name_t]
      ,[a_name_t]
      ,[population]
      ,[household]
      ,[UpdateDateTime]
    
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,?,?,
    ?,?,?,?,?,?
    )""", 
       row['Agent']
      ,row['CUST_NM']
      ,row['Active_Sub']
      ,row['Active_Sub_Target']
      ,row['Active_Beer']
      ,row['Active_Beer_Target']
      ,row['Active_Spirits']
      ,row['Active_Spirits_Target']
      ,row['Active_NAB']
      ,row['Active_NAB_Target']
      ,row['Active_Others']
      ,row['Active_Others_Target']
      ,row['Link']
      ,row['Link_Agent']
      ,row['LAT']
      ,row['LNG']
      ,row['s_region']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['population']
      ,row['household']
      ,row['UpdateDateTime']
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Upload_to_Server(filename):
    dummyFile='http://xxxxxx.com/'+filename+'//'
    return dummyFile

def Write_AGP_SalesArea_Link(df_input):
    print('------------- Start WriteDB: AGP SalesArea_Link -------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn = connect_tad

    #- View all records from the table
    sql="""select *  from  [TSR_ADHOC].[dbo].[AGP_SalesArea_Link] """   
    
    cursor=conn.cursor()
    cursor.execute(sql)
    conn.commit()

    for index, row in tqdm(df_write.iterrows()):
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[AGP_SalesArea_Link](		
       [Agent]
      ,[CUST_NM]
      ,[Active_Sub]
      ,[Active_Sub_Target]
      ,[Active_Beer]
      ,[Active_Beer_Target]
      ,[Active_Spirits]
      ,[Active_Spirits_Target]
      ,[Active_NAB]
      ,[Active_NAB_Target]
      ,[Active_Others]
      ,[Active_Others_Target]
      ,[Link]
      ,[Link_Agent]
      ,[LAT]
      ,[LNG]
      ,[s_region]
      ,[p_name_t]
      ,[a_name_t]
      ,[population]
      ,[household]
      ,[UpdateDateTime]
    
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,?,?,
    ?,?,?,?,?,?
    )""", 
       row['Agent']
      ,row['CUST_NM']
      ,row['Active_Sub']
      ,row['Active_Sub_Target']
      ,row['Active_Beer']
      ,row['Active_Beer_Target']
      ,row['Active_Spirits']
      ,row['Active_Spirits_Target']
      ,row['Active_NAB']
      ,row['Active_NAB_Target']
      ,row['Active_Others']
      ,row['Active_Others_Target']
      ,row['Link']
      ,row['Link_Agent']
      ,row['LAT']
      ,row['LNG']
      ,row['s_region']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['population']
      ,row['household']
      ,row['UpdateDateTime']
     ) 
    conn.commit()

    cursor.close()
    #conn.close()
    print('------------Complete WriteDB-------------')

def Read_th_tambon_information():   

    print('------------- Start ReadDB: public.th_tambon_information -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    host=machine_1
    database=server_1
    user=username_1
    password=password_1
    connection = psycopg2.connect(host=host, database=database, user=user, password=password)
    cursor_po = connection.cursor()

    sql = """SELECT * FROM public.\"th_tambon_information\"    """ 

    dfout = pd.read_sql_query(sql, connection)

    print(len(dfout), ' =======================  ',dfout.head(10))

    if connection:
            cursor_po.close()
            connection.close()
            print("PostgreSQL connection is closed")    


    return dfout

def Read_SubAgentByAgent(agentId,startDate,endDate):
    # print('------------- Start ReadDB : SubAgentByAgent-------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connector_ss_etl


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

           
            /* จำนวน sub */
            select * from (
            SELECT distinct [DATE] as maxDate
            ,[CUS_CODE]
            ,[CUS_NM]
            ,[CUS_STS_NM]
            ,[AGENT_CODE]
            ,[AGENT_NM]
            ,row_number() over(partition by [AGENT_CODE],[AGENT_NM],[CUS_CODE],[CUS_NM] order by [DATE] desc) as rowno
            FROM [SalesSupport_ETL].[dbo].[Temp_ETL_SubAgentSales]
            where AGENT_CODE = '"""+str(agentId)+"""' and cast([DATE] as date) 
            between '"""+str(startDate)+"""' and '"""+str(endDate)+"""'
            ) A where A.rowno = 1


    """
    
    dfout=pd.read_sql(sql,conn)
    
    # print(len(dfout.columns),' :: ',dfout.columns)
    # print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    # print(' --------- Reading End -------------')
    return dfout

def Read_Mobility_IsolationScore_twoWeeks(dateIn):
    print('------------- Start ReadDB : Mobility_IsolationScore_twoWeeks -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_remp


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

           SELECT  [EID]
                    ,[ELat]
                    ,[Elong]
                    ,[Freq]
                    ,[TotalCheckIn]
                    ,[PercentIsolation]
                    ,[MeanCheckIn]
                    ,[ReliabilityWeight]
                    ,[IsolationScore]
                    ,[DBCreatedDateTime]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[p_name_e]
                    ,[a_name_e]
                    ,[t_name_e]
                FROM [TB_SR_Employee].[dbo].[Mobility_IsolationScore_twoWeeks]
                where DBCreatedDateTime>'"""+str(dateIn)+"""'
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_CVM_CUST_NEW():
    print('------------- Start ReadDB : DIM_LOC_CVM_CUST_NEW -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT  [rowid]
                ,[CUSTM_CODE]
                ,[CUSTM_NAME]
                ,[CUSTM_LAT]
                ,[CUSTM_LNG]
                ,[WORK_SUB_DISTR]
                ,[WORK_DISTR]
                ,[WORK_PROVI]
                ,[WORK_ZIP]
                ,[CUST_CAT]
                ,[CUST_SUB_CAT]
                ,[s_region]
                ,[p_code]
                ,[a_code]
                ,[t_code]
                ,[p_name_t]
                ,[p_name_e]
                ,[a_name_t]
                ,[a_name_e]
                ,[t_name_t]
                ,[t_name_e]
                ,[prov_idn]
                ,[amphoe_idn]
                ,[tambon_idn]
                ,[MIN_PYMT_DATE]
                ,[CUST_CODE_12DIGI]
                ,[IS_VALID_LOC]
                ,[MAX_PYMT_DATE]
            FROM [TSR_ADHOC].[dbo].[DIM_LOC_CVM_CUST_NEW]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_AGSUB():
    print('------------- Start ReadDB : DIM_LOC_AGSUB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT  [REF]
                    ,[CUST_ID]
                    ,[CUST_ID_INT]
                    ,[CUST_NM]
                    ,[LAT]
                    ,[LNG]
                    ,[MIN_YEARMONTH]
                    ,[UPDATED_DATE]
                    ,[p_code]
                    ,[a_code]
                    ,[t_code]
                    ,[p_name_t]
                    ,[p_name_e]
                    ,[a_name_t]
                    ,[a_name_e]
                    ,[t_name_t]
                    ,[t_name_e]
                    ,[s_region]
                    ,[prov_idn]
                    ,[amphoe_idn]
                    ,[tambon_idn]
                    ,[CUST_STS]
                FROM [TSR_ADHOC].[dbo].[DIM_LOC_AGSUB]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_SSC_CUST():
    print('------------- Start ReadDB : DIM_LOC_SSC_CUST -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT [RowId]
                ,[Customer]
                ,[CustomerDesc]
                ,[CustType]
                ,[CustGrp1]
                ,[CustGrp1Desc]
                ,[ShopType]
                ,[CustGrp2]
                ,[CustGrp2Desc]
                ,[CustGrp3]
                ,[CustGrp3Desc]
                ,[CustGrp4]
                ,[CustGrp4Desc]
                ,[CustGrp5]
                ,[Region]
                ,[LONGITUDE]
                ,[LATITUDE]
                ,[IS_VALID_LATLNG]
                ,[STREET]
                ,[STR_SUPPL3]
                ,[CITY2]
                ,[POST_CODE1]
                ,[CITY1]
                ,[LastUpdate]
                ,[Province_clean]
                ,[Amphur_clean]
                ,[Tambon_clean]
                ,[p_code]
                ,[a_code]
                ,[t_code]
                ,[p_name_t]
                ,[p_name_e]
                ,[a_name_t]
                ,[a_name_e]
                ,[t_name_t]
                ,[t_name_e]
                ,[s_region]
                ,[prov_idn]
                ,[amphoe_idn]
                ,[tambon_idn]
                ,[area_sqm]
                ,[BS_IDX]
                ,[lastactivebillingdate]
            FROM [TSR_ADHOC].[dbo].[DIM_LOC_SSC_CUST]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


def Generate_Lat_Lng_ShpFile(dfIn, file_path):
    dfIn.rename(columns={'LAT':'Latitude','LNG':'Longitude'}, inplace=True)
    print(' --> ',dfIn.head(10))
    # 4 Create tuples of geometry by zipping Longitude and latitude columns in your csv file
    geometry = [Point(xy) for xy in zip(dfIn.Longitude, dfIn.Latitude)] 
    #print(' geometry : ',geometry)

    # 5 Define coordinate reference system on which to project your resulting shapefile
    crs = {'init': 'epsg:4326'}

    # 6 Convert pandas object (containing your csv) to geodataframe object using geopandas
    gdf = GeoDataFrame(dfIn, crs = crs, geometry=geometry)
    print(' gdf : ',gdf)
    # 7 Save file to local destination
    output_filename = file_path + "\\data\\" + "DIM_SHAPE.shp"
    gdf.to_file(filename= output_filename, driver='ESRI Shapefile')

    return gdf

def Read_SHAPE_File(file_path,sub_dir,file_name):
    
    # Read file using gpd.read_file()
    #data = gpd.read_file(file_path+"TH_amphure.shp")
    data1 = gpd.read_file(file_path+sub_dir+file_name, encoding = "iso-8859-1")  #ISO-8859-1
    data1 = data1.to_crs(epsg=4326)
    #print(' gpd : ',data['a_name_t'].head(10))
    print(' gpd : ',data1.head(10))

    return data1

def Read_DIM_TH_R_PROVINCE():
    print('------------- Start ReadDB : DIM_TH_R_PROVINCE -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT [PROVINCE_TH]
                ,[PROVINCE_EN]
                ,[REGION_TBL]
                ,[REGION_SALE]
                ,[REF_PROV]
                ,[COUNTRY]
                ,[BEER_STRATEGIC]
                ,[prov_idn]
                ,[lat]
                ,[lng]
                ,[REGION_THAILAND]
            FROM [TSR_ADHOC].[dbo].[DIM_TH_R_PROVINCE]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


def MapValue_Value(x,dictIn):
    try:
        mapped=dictIn[x]
    except:
        #print('==> ',x)
        mapped=x
    return mapped


def MapValue_String(x,dictIn):
    try:
        mapped=dictIn[x]
    except:
        #print('==> ',x)
        mapped=None
    return mapped

def Read_FCT_SALE_SIS_HAS_Period(date_m1, date_m):
    print('------------- Start ReadDB : FCT_SALE_SIS_HAS -------------')    
    conn = connector_ss_etl

    cursor = conn.cursor()

        #- Select data  all records from the table
    sql="""
       SELECT    [RowId]
                ,[SALE_MONTH]
                ,[WEEK_NAME]
                ,[REGION_SALE_REGION]
                ,[REGION_SALE_ZONE]
                ,[REGION_SALE_PRV]
                ,[REGION_SALE_NM]
                ,[REGION_SALE_LABEL]
                ,[REGION_SALE_DESC]
                ,[SALEMNG_NAME_SPIRITS]
                ,[SALEMNG_NAME_BEER]
                ,[AGENT_CODE]
                ,[AGENT_TGT_CODE]
                ,[AGENT_TGT_NM]
                ,[AGENT_NM]
                ,[AGENT_STS_NM]
                ,[AGENT_EXCLUSIVETYPE_NAME]
                ,[PRODUCT_REWARD_CATEGORY]
                ,[PRODUCT_REWARD_LABEL]
                ,[PRODUCT_REWARD_NAME]
                ,[AMT_DO]
                ,[HAS_QTY]
                ,[REWARD_QTY]
                ,[SALE_QTY]
                ,[SALE_QTY_DO]
                ,[SALE_AMT_REPORT]
                ,[SALE_QTY_SALE]
                ,[SALE_QTY_TAELITER]
                ,[SALE_QTY_VOL]
                ,[SALE_QTY_SALE_DO]
                ,[SALE_QTY_VOL_DO]
                ,[SALES_QTY_TAELITER_DO]
                ,[TIMEDATAVIEW]
                ,[STEP_CLOSE_DATE]
                ,[PRD_BRAND_HAS]
            FROM [SalesSupport_ETL].[dbo].[FCT_SALE_SIS_HAS]
             where REWARD_QTY<>0
             and SALE_MONTH between '"""+str(date_m1)+"""' and  '"""+str(date_m)+"""'
    """
    dfout=pd.read_sql(sql,conn)
    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_TB_VSMS_WebSurvey():
    print('------------- Start ReadDB : TB_VSMS_WebSurvey -------------')    
    conn = connect_survey

    cursor = conn.cursor()

        #- Select data  all records from the table
    sql="""
       SELECT C.[SaleVolumeId]	
                    ,C.[SalesTeamId]	
                    ,C.[UserId]	
                    ,C.[SOCustomerId]	
                    ,'|' as sep_join_mst_sovsms
                    ,BB.*
                    ,'|' as sepend
                    ,C.[BuyFromSOCustomer]	
                    ,C.[BuyFromSOCustomerID]	
                    ,C.[Description]	
                    ,C.[SaleOrgId]	
                    ,C.[NoReceiptNo]	
                    ,C.[ReceiptNo]
                    ,ROW_NUMBER() over(partition by C.SOCustomerId order by C.[OnDate] desc,C.[CreatedDate] desc,C.[UpdatedDate] desc) as row_desc_no
                    ,C.[OnDate]	
                    ,C.[CreatedDate]	
                    ,C.[UpdatedDate]	
                    ,C.[CreatedByUserId]	
                    ,C.[UpdatedByUserId]	
                    ,C.[DelFlag]	
                    ,'|' sep
                    ,C.[QuChangL]	
                    ,C.[QuChangS]	
                    ,C.[QuChangCanL]	
                    ,C.[QuChangCanS]	
                    ,C.[QuChangKEG30]	
                    ,C.[QuChangKEG20]	
                    ,C.[QuArchaL]	
                    ,C.[QuArchaS]	
                    ,C.[QuArchaCanL]	
                    ,C.[QuArchaCanS]	
                    ,C.[QuArchaKEG30]	
                    ,C.[QuArchaKEG20]	
                    ,C.[QuFrederbrauL]	
                    ,C.[QuFrederbrauS]	
                    ,C.[QuFrederbrauCanL]	
                    ,C.[QuFrederbrauCanS]	
                    ,C.[QuFrederbrauKEG30]	
                    ,C.[QuFrederbrauKEG20]	
                    ,C.[QuSinghaL]	
                    ,C.[QuSinghaS]	
                    ,C.[QuSinghaCanL]	
                    ,C.[QuSinghaCanS]	
                    ,C.[QuSinghaKEG30]	
                    ,C.[QuSinghaKEG20]	
                    ,C.[QuLeoL]	
                    ,C.[QuLeoS]	
                    ,C.[QuLeoCanL]	
                    ,C.[QuLeoCanS]	
                    ,C.[QuLeoKEG30]	
                    ,C.[QuLeoKEG20]	
                    ,C.[QuUBeerL]	
                    ,C.[QuUBeerS]	
                    ,C.[QuUBeerCanL]	
                    ,C.[QuUBeerCanS]	
                    ,C.[QuUBeerKEG30]	
                    ,C.[QuUBeerKEG20]	
                    ,C.[QuHeinekenL]	
                    ,C.[QuHeinekenS]	
                    ,C.[QuHeinekenCanL]	
                    ,C.[QuHeinekenCanS]	
                    ,C.[QuHeinekenKEG30]	
                    ,C.[QuHeinekenKEG20]	
                    ,C.[OtherProduct1L]	
                    ,C.[OtherProduct1S]	
                    ,C.[OtherProduct1CanL]	
                    ,C.[OtherProduct1CanS]	
                    ,C.[OtherProduct1KEG30]	
                    ,C.[OtherProduct1KEG20]	
                    ,C.[OtherProduct2L]	
                    ,C.[OtherProduct2S]	
                    ,C.[OtherProduct2CanL]	
                    ,C.[OtherProduct2CanS]	
                    ,C.[OtherProduct2KEG30]	
                    ,C.[OtherProduct2KEG20]	
                    ,C.[OtherProduct3L]	
                    ,C.[OtherProduct3S]	
                    ,C.[OtherProduct3CanL]	
                    ,C.[OtherProduct3CanS]	
                    ,C.[OtherProduct3KEG30]	
                    ,C.[OtherProduct3KEG20]	
                    ,C.[OtherProduct4L]	
                    ,C.[OtherProduct4S]	
                    ,C.[OtherProduct4CanL]	
                    ,C.[OtherProduct4CanS]	
                    ,C.[OtherProduct4KEG30]	
                    ,C.[OtherProduct4KEG20]	
                    ,C.[OtherProduct5L]	
                    ,C.[OtherProduct5S]	
                    ,C.[OtherProduct5CanL]	
                    ,C.[OtherProduct5CanS]	
                    ,C.[OtherProduct5KEG30]	
                    ,C.[OtherProduct5KEG20]	
                    ,C.[OtherProduct6L]	
                    ,C.[OtherProduct6S]	
                    ,C.[OtherProduct6CanL]	
                    ,C.[OtherProduct6CanS]	
                    ,C.[OtherProduct6KEG30]	
                    ,C.[OtherProduct6KEG20]	
                    ,C.[QuChangBP15]	
                    ,C.[QuChangB1P5L]	
                    ,C.[QuChangPet350]	
                    ,C.[QuChangPet460]	
                    ,C.[QuChangPet600]	
                    ,C.[QuChangPet1000]	
                    ,C.[QuChangPet1500]	
                    ,C.[QuChangOW325]	
                    ,C.[QuArchaBP15]	
                    ,C.[QuArchaB1P5L]	
                    ,C.[QuArchaPet350]	
                    ,C.[QuArchaPet460]	
                    ,C.[QuArchaPet600]	
                    ,C.[QuArchaPet1000]	
                    ,C.[QuArchaPet1500]	
                    ,C.[QuArchaOW325]	
                    ,C.[QuFrederbrauBP15]	
                    ,C.[QuFrederbrauB1P5L]	
                    ,C.[QuFrederbrauPet350]	
                    ,C.[QuFrederbrauPet460]	
                    ,C.[QuFrederbrauPet600]	
                    ,C.[QuFrederbrauPet1000]	
                    ,C.[QuFrederbrauPet1500]	
                    ,C.[QuFrederbrauOW325]	
                    ,C.[QuSinghaBP15]	
                    ,C.[QuSinghaB1P5L]	
                    ,C.[QuSinghaPet350]	
                    ,C.[QuSinghaPet460]	
                    ,C.[QuSinghaPet600]	
                    ,C.[QuSinghaPet1000]	
                    ,C.[QuSinghaPet1500]	
                    ,C.[QuSinghaOW325]	
                    ,C.[QuLeoBP15]	
                    ,C.[QuLeoB1P5L]	
                    ,C.[QuLeoPet350]	
                    ,C.[QuLeoPet460]	
                    ,C.[QuLeoPet600]	
                    ,C.[QuLeoPet1000]	
                    ,C.[QuLeoPet1500]	
                    ,C.[QuLeoOW325]	
                    ,C.[QuUBeerBP15]	
                    ,C.[QuUBeerB1P5L]	
                    ,C.[QuUBeerPet350]	
                    ,C.[QuUBeerPet460]	
                    ,C.[QuUBeerPet600]	
                    ,C.[QuUBeerPet1000]	
                    ,C.[QuUBeerPet1500]	
                    ,C.[QuUBeerOW325]	
                    ,C.[QuHeinekenBP15]	
                    ,C.[QuHeinekenB1P5L]	
                    ,C.[QuHeinekenPet350]	
                    ,C.[QuHeinekenPet460]	
                    ,C.[QuHeinekenPet600]	
                    ,C.[QuHeinekenPet1000]	
                    ,C.[QuHeinekenPet1500]	
                    ,C.[QuHeinekenOW325]	
                    ,C.[OtherProduct1BP15]	
                    ,C.[OtherProduct1B1P5L]	
                    ,C.[OtherProduct1Pet350]	
                    ,C.[OtherProduct1Pet460]	
                    ,C.[OtherProduct1Pet600]	
                    ,C.[OtherProduct1Pet1000]	
                    ,C.[OtherProduct1Pet1500]	
                    ,C.[OtherProduct1OW325]	
                    ,C.[OtherProduct2BP15]	
                    ,C.[OtherProduct2B1P5L]	
                    ,C.[OtherProduct2Pet350]	
                    ,C.[OtherProduct2Pet460]	
                    ,C.[OtherProduct2Pet600]	
                    ,C.[OtherProduct2Pet1000]	
                    ,C.[OtherProduct2Pet1500]	
                    ,C.[OtherProduct2OW325]	
                    ,C.[OtherProduct3BP15]	
                    ,C.[OtherProduct3B1P5L]	
                    ,C.[OtherProduct3Pet350]	
                    ,C.[OtherProduct3Pet460]	
                    ,C.[OtherProduct3Pet600]	
                    ,C.[OtherProduct3Pet1000]	
                    ,C.[OtherProduct3Pet1500]	
                    ,C.[OtherProduct3OW325]	
                    ,C.[OtherProduct4BP15]	
                    ,C.[OtherProduct4B1P5L]	
                    ,C.[OtherProduct4Pet350]	
                    ,C.[OtherProduct4Pet460]	
                    ,C.[OtherProduct4Pet600]	
                    ,C.[OtherProduct4Pet1000]	
                    ,C.[OtherProduct4Pet1500]	
                    ,C.[OtherProduct4OW325]	
                    ,C.[OtherProduct5BP15]	
                    ,C.[OtherProduct5B1P5L]	
                    ,C.[OtherProduct5Pet350]	
                    ,C.[OtherProduct5Pet460]	
                    ,C.[OtherProduct5Pet600]	
                    ,C.[OtherProduct5Pet1000]	
                    ,C.[OtherProduct5Pet1500]	
                    ,C.[OtherProduct5OW325]	
                    ,C.[OtherProduct6BP15]	
                    ,C.[OtherProduct6B1P5L]	
                    ,C.[OtherProduct6Pet350]	
                    ,C.[OtherProduct6Pet460]	
                    ,C.[OtherProduct6Pet600]	
                    ,C.[OtherProduct6Pet1000]	
                    ,C.[OtherProduct6Pet1500]	
                    ,C.[OtherProduct6OW325]	
                    ,C.[OtherProduct7L]	
                    ,C.[OtherProduct7S]	
                    ,C.[OtherProduct7CanL]	
                    ,C.[OtherProduct7CanS]	
                    ,C.[OtherProduct7KEG30]	
                    ,C.[OtherProduct7KEG20]	
                    ,C.[OtherProduct7BP15]	
                    ,C.[OtherProduct7B1P5L]	
                    ,C.[OtherProduct7Pet350]	
                    ,C.[OtherProduct7Pet460]	
                    ,C.[OtherProduct7Pet600]	
                    ,C.[OtherProduct7Pet1000]	
                    ,C.[OtherProduct7Pet1500]	
                    ,C.[OtherProduct7OW325]	
                    ,C.[OtherProduct8L]	
                    ,C.[OtherProduct8S]	
                    ,C.[OtherProduct8CanL]	
                    ,C.[OtherProduct8CanS]	
                    ,C.[OtherProduct8KEG30]	
                    ,C.[OtherProduct8KEG20]	
                    ,C.[OtherProduct8BP15]	
                    ,C.[OtherProduct8B1P5L]	
                    ,C.[OtherProduct8Pet350]	
                    ,C.[OtherProduct8Pet460]	
                    ,C.[OtherProduct8Pet600]	
                    ,C.[OtherProduct8Pet1000]	
                    ,C.[OtherProduct8Pet1500]	
                    ,C.[OtherProduct8OW325]	
                    ,C.[OtherProduct2CACanL]	
                    ,C.[OtherProduct4KEG5]	
                    ,C.[QuLeoNo8L]	
                    ,C.[QuLeoNo8LCan]	
                    ,C.[QuChangSP20]	
                    ,C.[QuChangSCP20]	
                FROM [OTC_Survey].[dbo].[TB_VSMS_WebSurvey_TTSaleVolumeSurveyOfSubAgent] C	
                left join ( 	
                    SELECT  A.[SOCustomerId]
                    ,A.[CustomerId]	
                    ,A.[CustomerCode]	
                    ,B.CustomerName
                    ,B.Latitude 
                    ,B.Longitude
                    FROM [OTC_Survey].[dbo].[TB_VSMS_SOCustomer] A
                    left join [OTC_Survey].[dbo].[TB_VSMS_Customer] B on A.CustomerId = B.CustomerId 
                    ) BB on C.SOCustomerId = BB.SOCustomerId
                where C.DelFlag = 0  and cast(OnDate as date) >= '2020-10-01' 	
                and C.BuyFromSOCustomerID in ('0001003133'	
                ,'0001003803'	
                ,'0001010650'	
                ,'0001011676'	
                ,'0001007359'	
                ,'0001008177'	
                ,'0001008273'
                ,'0001008141')	
                order by C.[OnDate] desc,C.[CreatedDate] desc,C.[UpdatedDate] desc	

    """
    dfout=pd.read_sql(sql,conn)
    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

##### find  sales contribution of subagent to global contribution by province
def Read_AGP_Saleout_Subagent():
    print('------------- Start ReadDB : AGP_Saleout_Subagent -------------')    
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
            SELECT [Province]
                ,[AGENT_CODE]
                ,[CUS_CODE]
                ,[CUS_NM]
                ,[CUS_STS_NM]
                ,[Beer]
                ,[Beer_Ratio]
                ,[Beer_AG]
                ,[Spirits]
                ,[Spirits_Ratio]
                ,[Spirits_AG]
                ,[NAB]
                ,[NAB_Ratio]
                ,[NAB_AG]
                ,[Others]
                ,[Others_Ratio]
                ,[Vol_CS]
                ,[Vol_Ratio]
                ,[PERIOD]
                ,[collected_at]
            FROM [TSR_ADHOC].[dbo].[AGP_Saleout_Subagent]
    """    
    dfout=pd.read_sql(sql,conn)    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


###### CVM SSC SalesUnit Location
def Read_DIM_LOC_CVM_SUNIT():
    print('------------- Start ReadDB : DIM_LOC_CVM_SUNIT -------------')    
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
            select SaleOrgId,SalesTeamId,SalesTeamCode,NAME,SalesUnitDesc,LAT,LNG, 
                case when p_name_t is not null then p_name_t else PROV_TH end as PROV_TH ,
                case when NAME is null then SalesUnitDesc else
                case when left(SalesUnitDesc,4)='TBPP' then  concat(NAME,'_TBPP') else NAME end    --- There are duplicates in NAME , the difference in SalesUnitDesc with prefix 'TBPP'
                end as SalesName
            from [TSR_ADHOC].[dbo].[DIM_LOC_CVM_SUNIT] 
            where [ChannelId]=9    ----  Select only CashVan, other categories like Sales Office is ignored    
            and LAT is not null
            and LNG is not null        
            and SaleOrgId=1   --- Select only CVM
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_CVM_SUNIT_rev2():
    print('------------- Start ReadDB : DIM_LOC_CVM_SUNIT_rev2 : Read from Database SUNIT -------------')    
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
          select  CODE
                    ,NAME
                    ,case when left(REGION,1)='R' then  concat('Region ', right(REGION,1)) else REGION end as REGION
                    ,PROV_EN
                    ,PROV_TH
                    ,LAT
                    ,LNG
                    ,tambon_idn
                    ,p_name_t
                    ,a_name_t
                    ,t_name_t
                    ,s_region
                    from(select CODE
                            ,NAME
                            ,REGION
                            ,PROV_EN
                            ,PROV_TH
                            ,case when CODE = '05041' then 17.00749   
                                when CODE = '08408' then 8.011773 
                                when CODE = '12007' then 13.83224 
                                when CODE = '62006' then 15.6135 
                                when CODE = '62008' then 16.46263
                                else LAT end as LAT
                            ,case when CODE = '05041' then 99.80386
                                when CODE = '08408' then 98.35391
                                when CODE = '12007' then 100.5345
                                when CODE = '62006' then 101.0606
                                when CODE = '62008' then 99.54024
                                else LNG end as LNG
                            ,tambon_idn
                            ,p_name_t
                            ,a_name_t
                            ,t_name_t
                            ,s_region
                            ,ROW_NUMBER() over(partition by concat(CODE,NAME,LAT,LNG) order by concat(CODE,NAME,LAT,LNG) desc) as row_number
                            from [TSR_ADHOC].[dbo].[DIM_LOC_CVM_SUNIT] 
                            --where ChannelId=9 
                            ) maintable
                    where row_number=1
                    and LAT is not null
                    and LNG is not null
    """
    
    dfout=pd.read_sql(sql,conn)
    
    # print(len(dfout.columns),' :: ',dfout.columns)
    # print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_SSC_ALLSALE_RawData_New():
    print('------------- Start ReadDB : SSC_ALLSALE_RawData_New -------------')    
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
            SELECT distinct A.[Plant_Code] Map_plant_code
                    ,A.[Plant_Name] Map_plant_name
                    ,B.Plant_Code New_plant_code
                    ,coalesce(B.Plant_Name, C.Plant_Name) New_plant_name
                    ,C.Plant_Code Old_plant_code
                    ,C.Plant_Name Old_plant_name
                    ,D.p_name_t 
                    ,D.a_name_t 
                    ,D.t_name_t
                    ,D.s_region
                    ,D.LAT
                    ,D.LNG                    
            FROM [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] A
            left join (SELECT distinct [Plant_Code],[Plant_Name],[Region_THBEV] FROM [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] where Plant_Code like '3%') B on A.Plant_Name=B.Plant_Name --New
            left join (SELECT distinct [Plant_Code],[Plant_Name],[Region_THBEV] FROM [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] where Plant_Code like '1%') C on A.Plant_Name= case when C.Plant_Name=N'สาขาสระบุรี' then N'สาขาสระบุรี (PT)' else C.Plant_Name end  --Old
            left join [TSR_ADHOC].[dbo].[DIM_LOC_SSC_BRNCH] D on D.SALE_OFFC_CODE=C.Plant_Code 
            where C.Plant_Code is not null    -- Remove duplicate from Saraburi branch
            and D.LAT is not null
            and D.LNG is not null          
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_SSC_ALLSALE_RawData_New_rev2():
    print('------------- Start ReadDB : SSC_ALLSALE_RawData_New_rev2 : Customer location with brand -------------')    
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
          SELECT distinct A.[Plant_Code] Map_plant_code
                                    ,A.[Plant_Name] Map_plant_name
                                    ,B.Plant_Code New_plant_code
                                    ,case when B.Plant_Name is null  then A.Plant_Name else B.Plant_Name end as New_plant_name
                                    ,C.Plant_Code Old_plant_code
                                    ,C.Plant_Name Old_plant_name
                                    ,case when p_name_t is null and A.Plant_Name like N'%ระนอง%' then N'ระนอง' else D.p_name_t end as p_name_t 
                                    ,D.a_name_t 
                                    ,D.t_name_t
                                    ,D.s_region
                                    ,D.LAT
                                    ,D.LNG                    
                            FROM [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] A
                            left join (SELECT distinct [Plant_Code],[Plant_Name],[Region_THBEV] FROM [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] where Plant_Code like '3%') B on A.Plant_Name=B.Plant_Name --New
                            left join (SELECT distinct [Plant_Code],[Plant_Name],[Region_THBEV] FROM [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] where Plant_Code like '1%') C on A.Plant_Name= case when C.Plant_Name=N'สาขาสระบุรี' then N'สาขาสระบุรี (PT)' else C.Plant_Name end  --Old
                            left join [TSR_ADHOC].[dbo].[DIM_LOC_SSC_BRNCH] D on D.SALE_OFFC_CODE=C.Plant_Code 
                            where C.Plant_Code is not null    -- Remove duplicate from Saraburi branch         
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_TH_AMPHUR_TH(province):
    print('------------- Start ReadDB : DIM_TH_AMPHUR_TH -------------', province)
    
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()
    #- Select data  all records from the table
    sql="""
            SELECT [PROVINCE_TH] as province , [AMPHUR_TH] as district
            FROM [TSR_ADHOC].[dbo].[DIM_TH_AMPHUR]
            where PROVINCE_TH = N'"""+str(province)+"""' 
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return list(dfout['district'].unique())

######### read facebok population
def Read_H3_Grid_Lv8_Ext_Province_PAT():
    print('------------- Start ReadDB : H3_Grid_Lv8_Ext_Province_PAT-------------')
    # ODBC Driver 17 for SQL Server
    conn = connect_war
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
          SELECT [hex_id]
                ,[Latitude]
                ,[Longitude]
                ,[population]
                ,[population_youth]
                ,[population_elder]
                ,[population_under_five]
                ,[population_515_2560]                               
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
            where population is not null
            and Latitude is not null
            and Longitude is not null
            and geometry is not null
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


def Read_H3_Kepler_Grid_Lv8_Province_2():
    print('------------- Start ReadDB : H3_Kepler_Grid_Lv8_Province_2-------------')
    # ODBC Driver 17 for SQL Server
    conn = connect_war
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
          SELECT [hex_id]
                ,[Latitude]
                ,[Longitude]
                ,[population]
                ,[population_youth]
                ,[population_elder]
                ,[population_under_five]
                ,[population_515_2560]
                ,[population_men]
                ,[population_women]
                ,[geometry]
                ,[p_name_t]
                ,[DBCreatedAt]
            FROM [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2]
            where population is not null
            and Latitude is not null
            and Longitude is not null
            and geometry is not null
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout



def Read_Center_DIM_TH_R_PROVINCE(province):
    print('------------- Start ReadDB : Center_DIM_TH_R_PROVINCE-------------')
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
          SELECT [PROVINCE_TH]
                ,[REGION_SALE]
                ,[lat]
                ,[lng]      
            FROM [TSR_ADHOC].[dbo].[DIM_TH_R_PROVINCE]
            where PROVINCE_TH <> N'ทุกจังหวัด'
            and PROVINCE_TH <> N'ระบุไม่ได้'
            and PROVINCE_TH = N'"""+str(province)+"""'
    """
    
    dfout=pd.read_sql(sql,conn)
    
    # print(len(dfout.columns),' :: ',dfout.columns)
    # print(dfout)
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


def Read_Center_DIM_TH_R_PROVINCE_ALL():
    print('------------- Start ReadDB : Center_DIM_TH_R_PROVINCE_ALL-------------')
    # ODBC Driver 17 for SQL Server
    conn = connect_tad
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
          SELECT [PROVINCE_TH]
                ,[REGION_SALE]
                ,[lat]
                ,[lng]      
            FROM [TSR_ADHOC].[dbo].[DIM_TH_R_PROVINCE]
            where PROVINCE_TH <> N'ทุกจังหวัด'
            and PROVINCE_TH <> N'ระบุไม่ได้'
      """
    
    dfout=pd.read_sql(sql,conn)
    
    # print(len(dfout.columns),' :: ',dfout.columns)
    # print(dfout)
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

############## Image to Binary
def Image_To_Binary(filename):
    location=os.getcwd()+'\\FIGURE\\AG_SalesArea\\'+filename
    customerId=filename.split('_')[0]    
    print(' customer : ',customerId)
    try:
        file_name=location.split("\\")[len(location.split("\\"))-1].split('.')[0]
        file_extension=location.split("\\")[len(location.split("\\"))-1].split('.')[1]    
        print(file_name,' :: ',file_extension)
        if( (file_extension=='PNG') | (file_extension=='png')):
            print(' converting : ',location)
            im = Image.open(location)
            im.convert('RGB').save(os.getcwd()+'\\FIGURE\\AG_SalesArea\\'+file_name+".jpg","JPEG") #this converts png image as jpeg
            location=os.getcwd()+'\\FIGURE\\AG_SalesArea\\'+file_name+".jpg"
            print(' Reading : ',location)
    except:
        location='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Image_To_Binary\\PIC\\No_Image_Available.jpg'
    img_file = open(location,'rb')
    image_read = img_file.read()   
   
    image_64_encode = base64.b64encode(image_read)
    image_64_encode =image_64_encode.decode('utf-8')    ## to remove "b'""  in front of the binary string
    img_file.close()    
    return image_64_encode

def Image_To_Binary_by_Location(customerId,location):        
    print(' customer : ',customerId)
    try:
        file_name=location.split("\\")[len(location.split("\\"))-1].split('.')[0]
        file_extension=location.split("\\")[len(location.split("\\"))-1].split('.')[1]    
        print(file_name,' :: ',file_extension)
        if( (file_extension=='PNG') | (file_extension=='png')):
            print(' converting : ',location)
            im = Image.open(location)
            im.convert('RGB').save(os.getcwd()+'\\FIGURE\\AG_SalesArea\\'+file_name+".jpg","JPEG") #this converts png image as jpeg
            location=os.getcwd()+'\\FIGURE\\AG_SalesArea\\'+file_name+".jpg"
            print(' Reading : ',location)
    except:
        location='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Image_To_Binary\\PIC\\No_Image_Available.jpg'
    img_file = open(location,'rb')
    image_read = img_file.read()   
   
    image_64_encode = base64.b64encode(image_read)
    image_64_encode =image_64_encode.decode('utf-8')    ## to remove "b'""  in front of the binary string
    img_file.close()    
    return image_64_encode

# ########### read population and household
# dfPop=Read_th_tambon_information()
# includeList=['p_name_t','a_name_t','t_name_t','population','household','s_region']
# dfPop=dfPop[includeList]
# dfPop['key']=dfPop['p_name_t']+'!'+dfPop['a_name_t']
# print(len(dfPop),' --- population ---',dfPop.head(3),' :: ',dfPop.columns)


# dfAgg=dfPop.groupby(['p_name_t','a_name_t']).sum().reset_index()
# print(len(dfAgg),' --- AGG ---',dfAgg.head(3),' :: ',dfAgg.columns)

###****************************************************************
# end_datetime = datetime.now()
# print ('---Start---',start_datetime)
# print('---complete---',end_datetime)
# DIFFTIME = end_datetime - start_datetime 
# DIFFTIMEMIN = DIFFTIME.total_seconds()
# print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')