import json
import pandas as pd
import numpy as np
import boto3
import time
# import s3fs
import io

import pymysql
import configparser

from datetime import datetime,date, timedelta


sdate = date(2019, 1, 3)
edate = date(2019, 10, 1) 

delta = edate - sdate 
dates_list =[]
for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    dates_list.append(day.strftime("%Y-%m-%d"))
    
sept_sample = dates_list[118:149]

#combine loops to run python athena query for all dates and put it into reporting db to different tables
i=0
while i<len(sept_sample):
    #print(f'{sept_sample[i]}')
    date =f'{sept_sample[i]}'
    month_string = f'0{pd.to_datetime(date).month}'if int(pd.to_datetime(date).month)<10 else f'{pd.to_datetime(date).month}'
    day_string= f'0{pd.to_datetime(date).day}' if int(pd.to_datetime(date).day)<10 else f'{pd.to_datetime(date).day}'
    year_string = f'{pd.to_datetime(date).year}'
    #def lambda_handler(event, context):

    def ExpectedFlog(x):
        if df5['start_refs'][x]=='A':
            return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['on_block_time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        elif df5['start_refs'][x]=='D':
            if df5['etd'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['etd'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['Off_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))       
        else:
            return None

    def ExpectedTlog(x):
        if df5['end_refs'][x]=='A':
            return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['on_block_time'][x]))) + pd.Timedelta(minutes=df5['end'][x]))
        elif df5['end_refs'][x]=='D':
            if df5['etd'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['etd'][x]))) + pd.Timedelta(minutes=df5['end'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['off_block_time'][x]))) + pd.Timedelta(minutes=df5['end'][x]))
        else:
            return None


    def arrivalConditions(x):
        if df5['start_refs'][x]=='A' and df5['end_refs'][x]=='A':
            return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['on_block_time'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['on_block_time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        elif df5['start_refs'][x]=='A' and df5['end_refs'][x] =='D':
            if df5['etd'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['etd'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['on_block_time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['off_block_time'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['on_block_time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        elif df5['start_refs'][x]=='D' and df5['end_refs'][x] =='D':
            if df5['etd'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['etd'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['etd'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['off_block_time'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['off_block_time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        else:
            return None

            
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    max_date=None
    min_date= None

    #OpName =event['OperationUnit']
    #OpName=event['OperationUnit']
    #Airline=event['Airline']
    # FlightNumber =event['FlightNumber']


    df1_query=f"""
    select *
    from (
    select t1.*,t2.BodyType as BodyType,t2.aircraft_group as aircraft_group from (
    SELECT
    Airline,
    logid,
    aircraft,
    FlightNumber_Departure,
    ToAirport_IATACode,
    FlightType,
    Bay,
    if(ToAirport_IATACode IN ('HYD', 'DEL', 'BLR', 'BOM', 'PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') 
    AS destination_type, 
    if(Bay IN ('54L', '54R', '55L', '55R', '56L', '56R', '57L', '57R', '58L', '58R', '51',
    '52', '53', '54', '55', '56', '57', '58'), 'Connected', 'Remote') AS BayType,
    (Coalesce( try(date_parse(On_Block_Time,
    '%Y-%m-%d %H:%i:%s')),
    try(date_parse(On_Block_Time,
    '%Y/%m/%d %H:%i:%s')),
    try(date_parse(On_Block_Time,
    '%d %M %Y %H:%i:%s')),
    try(date_parse(On_Block_Time,
    '%d/%m/%Y %H:%i:%s')),
    try(date_parse(On_Block_Time,
    '%d-%m-%Y %H:%i:%s')),
    try(date_parse(On_Block_Time,
    '%Y-%m-%d')),
    try(date_parse(On_Block_Time,
    '%Y/%m/%d')),
    try(date_parse(On_Block_Time,
    '%d %M %Y')) ) ) as On_Block_Time,

    (Coalesce( try(date_parse(Off_Block_Time,
    '%Y-%m-%d %H:%i:%s')),
    try(date_parse(Off_Block_Time,
    '%Y/%m/%d %H:%i:%s')),
    try(date_parse(Off_Block_Time,
    '%d %M %Y %H:%i:%s')),
    try(date_parse(Off_Block_Time,
    '%d/%m/%Y %H:%i:%s')),
    try(date_parse(Off_Block_Time,
    '%d-%m-%Y %H:%i:%s')),
    try(date_parse(Off_Block_Time,
    '%Y-%m-%d')),
    try(date_parse(Off_Block_Time,
    '%Y/%m/%d')),
    try(date_parse(Off_Block_Time,
    '%d %M %Y')) ) ) as Off_Block_Time,

    (Coalesce( try(date_parse(ETD,
    '%Y-%m-%d %H:%i:%s')),
    try(date_parse(ETD,
    '%Y/%m/%d %H:%i:%s')),
    try(date_parse(ETD,
    '%d %M %Y %H:%i:%s')),
    try(date_parse(ETD,
    '%d/%m/%Y %H:%i:%s')),
    try(date_parse(ETD,
    '%d-%m-%Y %H:%i:%s')),
    try(date_parse(ETD,
    '%Y-%m-%d')),
    try(date_parse(ETD,
    '%Y/%m/%d')),
    try(date_parse(ETD,
    '%d %M %Y')) ) ) as etd,

    if(Airline = 'Spicejet',
    2,
    if(Airline IN ('Air India',
    'Etihad',
    'SriLankan Airlines',
    'Alliance Air',
    'Thai Airways',
    'Air Arabia',
    'Cathay Pacific',
    'FlyDubai',
    'Go Air',
    'Jazeera Airways',
    'Silk Air',
    'Vistara'),
    9,
    4)) AS Entity,year,month,day,operationunit

    from dailyflightschedule_merged_parquet where logid is not null
    ) as t1 left join (
    SELECT Hexcode,
        BodyType,
        aircraftType,
        REPLACE(RegNo, '-', '') as aircraft_group
    FROM flightmaster_parquet
    ) as t2 on t1.aircraft = t2.hexcode
    where date(On_Block_Time)= date('{date}') and year='{year_string}'
    and month='{month_string}' and day='{day_string}' and (OperationUnit ='4' or OperationUnit is null)
    group by Airline,
    logid,
    aircraft,
    FlightNumber_Departure,
    ToAirport_IATACode,
    FlightType,
    Bay,
    BayType,
    destination_type,
    aircraft_group,
    On_Block_Time,
    Off_Block_Time,
    etd,
    Entity,year,month,day,operationunit,BodyType
    )as DFS_Body inner join

    (
    SELECT FlightNo,
        OperationName,
        Duration,
        flight_pk,
        flogdate,
        tlogdate
    FROM equipactivitylogs_parquet
    WHERE date(Coalesce( try(date_parse(flogdate, '%Y-%m-%d %H:%i:%s')),
    try(date_parse(flogdate, '%Y/%m/%d %H:%i:%s')), 
    try(date_parse(flogdate, '%d %M %Y %H:%i:%s')), 
    try(date_parse(flogdate, '%d/%m/%Y %H:%i:%s')), 
    try(date_parse(flogdate, '%d-%m-%Y %H:%i:%s')), 
    try(date_parse(flogdate, '%Y-%m-%d')), 
    try(date_parse(flogdate, '%Y/%m/%d')), 
    try(date_parse(flogdate, '%d %M %Y')) ) )=date('{date}') and year='{year_string}'
    and month='{month_string}' and day='{day_string}' 
        AND flogdate is NOT NULL 
        and flight_pk is not null
        -- AND Assigned = '1'
        AND operationname NOT IN ('ETT', 'PCA', 'CAR', 'PCG')
        AND OperationName NOT LIKE 'ac:%'
        AND (duration >'30'
        and flight_pk is not null
        OR OperationName IN ('CHO', 'CHF'))

    ) as EAL_Logs on DFS_Body.logid=EAL_Logs.flight_pk
    """

    response = athena.start_query_execution(QueryString=df1_query, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))

    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']

    while status=='RUNNING' or status=="QUEUED":
        print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

    s3 = boto3.client('s3')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/'+query_execution_id+'.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
    final_parent = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    #final_parent1= final_parent.copy()
    final_parent['time_arrival']=pd.to_datetime(final_parent['flogdate']) - pd.to_datetime(final_parent['On_Block_Time'])
    final_parent['time_departure']=pd.to_datetime(final_parent['Off_Block_Time']) - pd.to_datetime(final_parent['flogdate'])
    final_parent['time_arrival'] = final_parent['time_arrival'].dt.total_seconds().div(60) if final_parent['time_arrival'] is not None else None
    final_parent['time_departure']=final_parent['time_departure'].dt.total_seconds().div(60) if final_parent['time_departure'] is not None else None


    #####################################################################

    df1 = final_parent.copy()
    df1.columns = map(str.lower, df1.columns)
    df1_sub_cat = df1[['airline', 'flightnumber_departure', 'toairport_iatacode','flighttype', 'destination_type','baytype', 'bodytype',
        'operationname', 'entity','aircraft_group','bay']]
    df1_sub_cat = df1_sub_cat.fillna('Unknown')


    df1_numeric_date = df1[['on_block_time','etd' ,'off_block_time','duration', 'time_arrival', 'time_departure',
        'flogdate', 'tlogdate']]


    df3= pd.merge(df1_sub_cat,df1_numeric_date, left_index=True, right_index=True)


    df3_grouped = df3.copy()
    s3 = boto3.resource('s3')
    obj = s3.Object('tbb-temp-research-bucket', 'db_creds_prod.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    #activity_list = ['PCBA', 'PCB', 'PCDA', 'PCD', 'LAA', 'FLE', 'CAT' , 'WFG' , 'TCG' , 'CHO' , 'CHF' , 'PushBack' , 'BBF' , 'BBL' , 'LTD' , 'FTD']
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)

    ###############################################################################################
    #                                                                                             #
    #                                                                                             #
    #                             PTS TABLE ANS LOGIC TO FILTER aircraft_group and Entity         
    #                                                                                             #
    ###############################################################################################
    df3_grouped.aircraft_group = df3_grouped.aircraft_group.replace({'Unknown':'Boeing 777'})
    pts=pd.read_sql("""select * from PTS_tabular""",con=conn)
    pts = pts.drop(index=0).reset_index(drop=True)


    list_AirCraftType_PTS = pts.AirCraftType.unique()
    pts_Entity_list=pts.Entity.unique()
    #df_pts_source_merged = df_pts_source_merged[df_pts_source_merged['aircraft_group'].isin(list_AirCraftType_PTS)]
    #df_pts_source_merged= df_pts_source_merged[df_pts_source_merged['Entity'].isin(pts_Entity_list)]
    df3_grouped['aircraft_group'] = df3_grouped['aircraft_group'].apply(lambda x: 'Boeing 777' if x not in list_AirCraftType_PTS else x)
    df3_grouped['entity']=df3_grouped['entity'].apply(lambda x: 4 if x not in pts_Entity_list else x)

    pts.columns = map(str.lower, pts.columns)
    df_pts_source_merged=pd.merge(df3_grouped, pts, left_on=['entity','aircraft_group','operationname'], right_on=['entity','aircrafttype','operationname'],how='left')

    df_pts_source_merged_verify = df_pts_source_merged.copy()


    ####
    df_pts_source_merged.drop(df_pts_source_merged[(df_pts_source_merged.start_refs=='A')
                        & (df_pts_source_merged.start>0)
                        & (df_pts_source_merged.flogdate< df_pts_source_merged.on_block_time)].index, inplace=True)


    #if ETD is NaT and Off_Block_Time is NaT then filter those rows
    df_pts_source_merged.drop(df_pts_source_merged[(df_pts_source_merged.etd.isna()== True)&
                            (df_pts_source_merged.off_block_time.isna()==True)].index, inplace=True)

    df_pts_source_merged['max_tlogdate']=df_pts_source_merged.groupby(['airline', 'flightnumber_departure', 'toairport_iatacode', 'flighttype',
        'destination_type', 'baytype', 'bodytype', 'operationname', 'entity','aircraft_group', 'bay'])['tlogdate'].transform(max)


    df_pts_source_merged_sorted = df_pts_source_merged.sort_values(by=['airline','flightnumber_departure', 'toairport_iatacode','flighttype', 
                                                                    'destination_type',
        'baytype', 'bodytype', 'operationname', 'entity','aircraft_group', 'bay','flogdate'])

    df_pts_source_merged_sorted['avg_duration'] = pd.to_datetime(df_pts_source_merged_sorted['max_tlogdate'])-pd.to_datetime(df_pts_source_merged_sorted['flogdate'])

    df_pts_source_merged_sorted_non_duplicates = df_pts_source_merged_sorted.drop_duplicates(subset=['airline','flightnumber_departure', 'toairport_iatacode','flighttype', 
                                                                    'destination_type',
        'baytype', 'bodytype', 'operationname', 'entity','aircraft_group', 'bay'],keep='first')
    df5= df_pts_source_merged_sorted_non_duplicates.copy().reset_index(drop=True)




    #####################################################################################################

    x = []
    for each in df5.index:
        x.append(arrivalConditions(each))
    df_ser1 = pd.DataFrame(x, columns=['expected_duration'])

    x = []
    for each in df5.index:
        x.append(ExpectedTlog(each))
    df_ser2 = pd.DataFrame(x, columns=['expected_dlog'])

    x = []
    for each in df5.index:
        x.append(ExpectedFlog(each))
    df_ser3 = pd.DataFrame(x, columns=['expected_flog'])


    df5_sub1 =pd.merge(df5,df_ser1,left_index=True,right_index=True)
    df5_sub2 =pd.merge(df5_sub1,df_ser2,left_index=True,right_index=True)
    df5_sub3 =pd.merge(df5_sub2,df_ser3,left_index=True,right_index=True)


    df6 = df5_sub3.copy()


    start_time = pd.Timestamp('2019-01-01 00:00:00')
    df6['expected_sec_flog_sec']= (pd.to_datetime(df6['expected_flog']) - start_time)
    df6['flog_sec']= (pd.to_datetime(df6['flogdate']) - start_time)
    df6['expected_sec_flog_sec'] = df6['expected_sec_flog_sec'].dt.total_seconds().div(60)
    df6['flog_sec'] =  df6['flog_sec'].dt.total_seconds().div(60)
    df6['diff_min'] = df6['expected_sec_flog_sec']-df6['flog_sec']
    df6['schedulestatus'] = np.where(df6['diff_min']<=0, 'WithinPTS','Delayed')
    df1_agg_1  = pd.DataFrame(df6['schedulestatus'].value_counts(normalize=True).round(2) * 100).reset_index()




    df6['expected_time_arrival'] = pd.to_datetime(df6['expected_flog'])-pd.to_datetime(df6['on_block_time'])
    df6['expected_time_departure'] =pd.to_datetime(df6['off_block_time']) - pd.to_datetime(df6['expected_flog'])

    df6['expected_duration_int']= df6['expected_duration'].dt.total_seconds().div(60) if df6.expected_time_arrival is not None else None
    df6['expected_time_arrival_int']= df6['expected_time_arrival'].dt.total_seconds().div(60) if df6.expected_time_arrival is not None else None
    df6['expected_time_departure_int']= df6['expected_time_departure'].dt.total_seconds().div(60) if df6.expected_time_departure is not None else None

    df6['avg_duration_int']= df6['avg_duration'].dt.total_seconds().div(60) if df6.avg_duration is not None else None

    df6_sub = df6[['operationname','avg_duration_int','expected_duration','expected_duration_int','time_arrival','expected_time_arrival_int','time_departure','expected_time_departure_int']]

    #df6_Active_operations = df6[df6.OperationName.isin(activity_list)]
    df6_Active_operations= df6.copy()
    start_time = pd.Timestamp('2019-01-01 00:00:00')
    df6_Active_operations['expected_sec_flog_sec']= (pd.to_datetime(df6_Active_operations['expected_flog']) - start_time)
    df6_Active_operations['flog_sec']= (pd.to_datetime(df6_Active_operations['flogdate']) - start_time)
    df6_Active_operations['expected_sec_flog_sec'] = df6_Active_operations['expected_sec_flog_sec'].dt.total_seconds().div(60)
    df6_Active_operations['flog_sec'] =  df6_Active_operations['flog_sec'].dt.total_seconds().div(60)
    df6_Active_operations['diff_min'] = df6_Active_operations['expected_sec_flog_sec']-df6_Active_operations['flog_sec']

    df6_Active_operations['schedulestatus'] = np.where(df6_Active_operations['diff_min']<=0, 'WithinPTS','Delayed')
    df1_agg_Active_opp = pd.DataFrame(df6_Active_operations['schedulestatus'].value_counts(normalize=True).round(2) * 100).reset_index()
    df1_agg_Active_opp['archived_date']=f'{date}'
    df1_agg_Active_opp['operationunit']='4'
    df1_agg_Active_opp.rename(columns={'index':'withinstatus'},inplace=True)
    db_write= 'Reporting'
    #db_tbl_name = 'df1_agg_level1_data'
    db_Agg_Level_1_table = 'Turnaround_Agg_Level_1'
    db_Agg_Level_2_table = 'Turnaround_Agg_Pie_Level_2'
    db_Agg_Level_3_table = 'Turnaround_Agg_Airline_Level_3'
    db_Agg_Level_4_table = 'Turnaround_Agg_Aircraft_Level_4'
    hostname_write = 'aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com'

    '''
    Create a mapping of df dtypes to mysql data types (not perfect, but close enough)
    '''
    def dtype_mapping():
        return {'object' : 'TEXT',
            'int64' : 'INT',
            'float64' : 'FLOAT',
            'datetime64[ns]' : 'DATETIME',
            'bool' : 'TINYINT',
            'category' : 'TEXT',
            'timedelta[ns]' : 'TEXT'}

    '''
    Create sql input for table names and types
    '''
    def gen_tbl_cols_sql(df):
        dmap = dtype_mapping()
        sql = "r_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY"
        df1 = df.rename(columns = {"" : "nocolname"})
        hdrs = df1.dtypes.index
        hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
        for i, hl in enumerate(hdrs_list):
            sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
        return sql

    '''
    Create a mysql table from a df
    '''
    def create_mysql_tbl_schema(df, conn, db, tbl_name):
        tbl_cols_sql = gen_tbl_cols_sql(df)
        sql = "CREATE TABLE {0} ({1})".format( db_Agg_Level_1_table, tbl_cols_sql)
        #sql = "CREATE TABLE {1} ({2} constraint {1} primary key (id)) ".format(db, db_tbl_name, tbl_cols_sql)
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.commit()
        
    '''
    Write df data to newly create mysql table
    '''
    def df_to_mysql(df, conn, tbl_name):
        df.to_sql(tbl_name, conn, if_exists='append',index=False)

    ####################### ENGINE TO WRITE ####################################    
        #!pip install mysql-connector
    import mysql.connector
    from sqlalchemy import create_engine
    engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(username, password, hostname_write, jdbcPort, db_write), echo=False)
    #mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8


    ########################ONLY ONCE########################################
    #tbl_cols_sql = gen_tbl_cols_sql(df1_agg_Active_opp)
    #sql = "CREATE TABLE {0} ({1})".format( db_tbl_name, tbl_cols_sql)


    ############### VERY IMP TO WRITE ROWS TO TABLE ##################
    df_to_mysql(df1_agg_Active_opp, engine, db_Agg_Level_1_table)


    df7 = df6_Active_operations[['airline', 'flightnumber_departure', 'toairport_iatacode', 'flighttype',
        'destination_type', 'baytype', 'bodytype', 'operationname', 'entity','aircraft_group','schedulestatus' ,'bay']]
    #df7 = df7[df7.OperationName.isin(activity_list)]
    d1  = pd.DataFrame(df7[['baytype', 'flighttype','bodytype',
                            'destination_type','operationname','schedulestatus','bay']])
    d2 = d1.groupby(['baytype', 'flighttype','bodytype', 'destination_type','operationname','schedulestatus','bay'])['schedulestatus'].agg('count').rename(columns = {'schedulestatus':'ww'})
    d3 = pd.DataFrame(d2)
    d4  = d3.reset_index().rename(columns={0:'count_schedulestatus'})
    d5 = d4.groupby(['baytype', 'flighttype','bodytype', 'destination_type','operationname','bay']).agg('sum').reset_index()
    #d5  = d5.rename(columns={'count_ScheduleStatus':'Total_opp_count'})
    d6= pd.merge(d4,d5,on=['baytype', 'flighttype','bodytype', 'destination_type','operationname','bay'])
    d6['percentage_of_total'] = ((d6['count_schedulestatus_x']/d6['count_schedulestatus_y'])*100).round(2)



    #PTS table for full name
    PTS_names= pd.read_sql("select Code,`Function` from PTSMaster",con=conn)
    PTS_names = PTS_names.rename(index=str,columns={'Code':'operationname'})
    PTS_names = PTS_names.drop_duplicates('operationname')
    d6_name_change= pd.merge(d6,PTS_names, how='left', on='operationname')
    d6_name_change = d6_name_change.rename(index=str, columns={'operationname':'shortname','Function':'operationname' })


    ### Agg level pie charts 
    d6_name_change_2 = d6_name_change[['baytype', 'flighttype', 'bodytype', 'destination_type', 'shortname',
        'schedulestatus', 'count_schedulestatus_x',
        'count_schedulestatus_y', 'percentage_of_total', 'operationname','bay']]


    ###OP####
    df_agg_Level2_Pie = d6_name_change_2.copy()
    df_agg_Level2_Pie['archived_date']=f'{date}'
    df_agg_Level2_Pie['operationunit']='4'


    df_to_mysql(df_agg_Level2_Pie, engine, db_Agg_Level_2_table)


    ####################################### Airline #########################

    df6_Active_operations= df6.copy()
    #######################################################################################################

    df_airline= df6_Active_operations[['airline', 'flighttype','destination_type','baytype', 'bodytype',
                                    'operationname','schedulestatus','bay','avg_duration_int','expected_duration',
                                    'expected_duration_int','time_arrival','time_departure','expected_time_arrival_int', 
                                    'expected_time_departure_int']]

    #df_airline = df_airline[df_airline.OperationName.isin(activity_list)]
    df_airline = df_airline.round(0)

    df_airline = df_airline.groupby(['airline', 'flighttype','destination_type','baytype', 'bodytype',
                                    'operationname','schedulestatus','bay']).agg('mean').reset_index()
    df_airline.rename(columns={'avg_duration_int':'Acutal_Duration','expected_duration':'Expected_Duration',
                        'time_arrival':'Acutal_Arrival', 'time_departure': 'Actual_Departure',
                        'expected_time_arrival_int': 'Expected_Arrival','expected_time_departure_int':'Expected_Departure'
                        },inplace=True)

    df_airline_2 = df_airline[['airline', 'flighttype', 'destination_type', 'baytype', 'bodytype',
        'operationname', 'schedulestatus', 'Acutal_Duration',
        'expected_duration_int', 'Acutal_Arrival', 'Actual_Departure',
        'Expected_Arrival', 'Expected_Departure','bay']]

    df_airline_2 = df_airline_2[~(df_airline_2.airline=='unknown')]
    df_airline_final = df_airline_2.copy()


    df_airline_final['archived_date']=f'{date}'
    df_airline_final['operationunit']='4'


    df_to_mysql(df_airline_final, engine, db_Agg_Level_3_table)

    ################### aircraft #########################
    df_flight= df6_Active_operations[['airline', 'flighttype','destination_type','baytype', 'bodytype','flightnumber_departure','toairport_iatacode',
                                    'operationname','schedulestatus','bay','avg_duration_int','expected_duration',
                                    'expected_duration_int','time_arrival','time_departure','expected_time_arrival_int', 
                                    'expected_time_departure_int']]
    df_flight = df_flight.round(0)
    df_flight = df_flight.groupby(['airline','flightnumber_departure','toairport_iatacode', 'flighttype','destination_type','baytype', 'bodytype',
                                    'operationname','schedulestatus','bay']).agg('mean').reset_index()

    df_flight.rename(columns={'avg_duration_int':'Acutal_Duration','expected_duration':'Expected_Duration',
                        'time_arrival':'Acutal_Arrival', 'time_departure': 'Actual_Departure',
                        'expected_time_arrival_int': 'Expected_Arrival','expected_time_departure_int':'Expected_Departure'
                        },inplace=True)
    df_flight_2= df_flight[['airline', 'flightnumber_departure', 'flighttype',
        'destination_type', 'baytype', 'bodytype', 'operationname',
        'schedulestatus', 'Acutal_Duration', 'expected_duration_int',
        'Acutal_Arrival', 'Actual_Departure', 'Expected_Arrival',
        'Expected_Departure', 'bay', 'toairport_iatacode']]

    Operation_full_names = d6_name_change[['shortname','operationname']]
    Operation_full_names.rename(columns={'operationname':'opfullname'}, inplace=True)
    Operation_full_names=Operation_full_names.drop_duplicates().reset_index(drop=True)
    df_flight_merged= pd.merge(df_flight_2,Operation_full_names, left_on=['operationname'],right_on=['shortname'],how='left')


    df_flight_final = df_flight_merged.copy()
    df_flight_final['archived_date']=f'{date}'
    df_flight_final['operationunit']='4'


    df_to_mysql(df_flight_final, engine, db_Agg_Level_4_table)


    i+=1


