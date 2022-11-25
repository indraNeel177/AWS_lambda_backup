import json
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import numpy as np
import configparser
import boto3
import io
def lambda_handler(event,context):
    
    def slots_min(x):
        if(x == 0 or x < 15):
            return '00 - 15'
        elif (x == 15 or x < 30):
            return '15 - 30'
        elif (x == 30 or x < 45):
            return '30 - 45'
        else:
            return '45 - 00'
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod_read_replica.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)
    start=datetime.now() 
    
    OpName=event['OperationUnit']
    print('opName',OpName)
    Airline= event['Airline']
    flights = event['flights']
    
    #OpName=22
    #Airline= ""
    
    yesterday= event['yesterday']
    #yesterday = "True"
    
#     hyderabad connected bays
# ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    
    condition1 = '' if Airline=="" else f"where Airline='{Airline}'"
    condition2 = '' if Airline=="" else f"and Airline='{Airline}'"
    
    airline_filter_condition= "" if flights=="" or flights==None else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    
    if OpName == '4':
        df2 = pd.read_sql(f"""
     select t1.*,t2.bodytype from (
    select Logid,Airline,Aircraft,flightnumber_arrival,FromAirport_IATACode,Belt_FirstBag,Belt_LastBag,belt,On_Block_Time,FlightType,IFNULL(TerminalArrival,'DISABLE') as TerminalArrival ,
    if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type
    ,if(Bay IN ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58'),'Connected','Remote') 
    AS BayType
    from DailyFlightSchedule_Merged where 
    operationunit={OpName}  {airline_filter_condition} and (
     date(Belt_FirstBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} or 
     date(Belt_LastBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition})
    ) as t1 left join (select hexcode,bodytype from FlightMaster) as t2 
    on t1.Aircraft= t2.hexcode {condition1}
    """,con=conn)
    elif OpName =='13':
        df2 = pd.read_sql(f"""
     select t1.*,t2.bodytype from (
    select Logid,Airline,Aircraft,flightnumber_arrival,FromAirport_IATACode,Belt_FirstBag,Belt_LastBag,belt,On_Block_Time,FlightType,IFNULL(TerminalArrival,'DISABLE') as TerminalArrival ,
    if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type
    ,if(Bay IN ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22'),'Connected','Remote') 
    AS BayType
    from DailyFlightSchedule_Merged where 
    operationunit={OpName}  {airline_filter_condition} and (
     date(Belt_FirstBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} or 
     date(Belt_LastBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition})
    ) as t1 left join (select hexcode,bodytype from FlightMaster) as t2 
    on t1.Aircraft= t2.hexcode {condition1}""",con=conn)
    else :
        df2=pd.read_sql(f"""
     select t1.*,t2.bodytype from (
    select Logid,Airline,Aircraft,flightnumber_arrival,FromAirport_IATACode,Belt_FirstBag,Belt_LastBag,belt,On_Block_Time,
    AircraftRegistration_Arrival,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival
    ,FlightType,
    if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type
    ,if(TerminalArrival in ('T3'),'Connected','Remote') as BayType
    from DailyFlightSchedule_Merged where 
    operationunit=22  {airline_filter_condition}  and (
     date(Belt_FirstBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} or 
     date(Belt_LastBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}) 
    ) as t1 left join (select hexcode,bodytype,REPLACE(RegNo, '-', '') as RegNo from FlightMaster) as t2 
    on t1.AircraftRegistration_Arrival= t2.RegNo having TerminalArrival in ('T1','T2','T3') {condition2}
    """,con=conn)
    
    print(df2)
    df2 = df2.copy()  if Airline=="" else df2[df2.Airline==f'{Airline}'].reset_index(drop=True)
    
    
    today = (pd.to_datetime('today')+pd.Timedelta('05:30:00')).replace(hour=0, minute=0, second=0)
    today_last = (pd.to_datetime('today') +pd.Timedelta('05:30:00')).replace(hour=23, minute=59, second=59)
    print('today',today)
    yesterday_date=pd.Timestamp('now', tz='Asia/Kolkata').date()-timedelta(1)
    print("yesterday",yesterday_date)
    yesterday_start=pd.datetime(yesterday_date.year, yesterday_date.month, yesterday_date.day, 0,0,0)
    yesterday_last=pd.datetime(yesterday_date.year, yesterday_date.month, yesterday_date.day, 23,59,59)
    
    if not yesterday=='True':
        df2.Belt_FirstBag = df2.Belt_FirstBag.apply(lambda x: today if pd.Timestamp(x).date() < pd.Timestamp('now', tz='Asia/Kolkata').date() else x )
        df2.Belt_LastBag = df2.Belt_LastBag.apply(lambda x: today_last if pd.Timestamp(x).date() > pd.Timestamp('now', tz='Asia/Kolkata').date() else x )
    elif yesterday=='True':
        print("yes")
        df2.Belt_FirstBag = df2.Belt_FirstBag.apply(lambda x: yesterday_start if pd.Timestamp(x).date() < yesterday_date else x )
        df2.Belt_LastBag = df2.Belt_LastBag.apply(lambda x: yesterday_last if pd.Timestamp(x).date() > yesterday_date else x )
    
 
    # Total belt time
    
    df2['Belt_FirstBag_hour']= df2.Belt_FirstBag.dt.hour
    df2['Belt_FirstBag_minute']= df2.Belt_FirstBag.dt.minute
    
    df2['Belt_LastBag_hour']= df2.Belt_LastBag.dt.hour
    df2['Belt_LastBag_minute']= df2.Belt_LastBag.dt.minute
    
    df2['slot_last'] = df2.Belt_LastBag_minute.apply(slots_min)
    
    df2['slot'] = df2.Belt_FirstBag_minute.apply(slots_min)
    
    df2['slots_min'] = df2.Belt_FirstBag_minute.apply(slots_min) 
    
    df3 = df2
    if(OpName == '13'):
        df2['FlightType'] = 'Domestic'
    df2 = df2[['Logid', 'Airline', 'Aircraft', 'flightnumber_arrival',
           'FromAirport_IATACode', 'Belt_FirstBag', 'Belt_LastBag', 'belt',
           'FlightType', 'destination_type', 'BayType', 'bodytype',
           'Belt_FirstBag_hour', 'Belt_FirstBag_minute', 'Belt_LastBag_hour',
           'Belt_LastBag_minute', 'slot_last', 'slot', 'slots_min','TerminalArrival']] if OpName=='13' else df2[['Logid', 'Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
           'FromAirport_IATACode', 'Belt_FirstBag', 'Belt_LastBag', 'belt',
           'FlightType', 'destination_type', 'BayType', 'bodytype',
           'Belt_FirstBag_hour', 'Belt_FirstBag_minute', 'Belt_LastBag_hour',
           'Belt_LastBag_minute', 'slot_last', 'slot', 'slots_min','TerminalArrival']]
    

    df_new1= df2[['Airline', 'Aircraft', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt','TerminalArrival',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','slot', 'slot_last']] if OpName=='13' else df2[['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt','TerminalArrival',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','slot', 'slot_last']]
    
    
    melted_df = df_new1.reset_index().melt(['Airline', 'Aircraft', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','TerminalArrival'], ['slot', 'slot_last'], value_name='final_slot') if OpName=='13' else df_new1.reset_index().melt(['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','TerminalArrival'], ['slot', 'slot_last'], value_name='final_slot')
    
    melted_df = melted_df.sort_values(by='flightnumber_arrival')
    melted_df = melted_df.drop('variable',axis=1)
    melted_df_non_duplicate = melted_df.drop_duplicates(subset=['flightnumber_arrival','final_slot']).reset_index(drop=True)
    melted_df_non_duplicate['count'] = melted_df_non_duplicate.groupby(['Belt_FirstBag_hour','TerminalArrival','final_slot','belt'])['flightnumber_arrival'].transform('count')
    melted_df_non_duplicate['FlightType'] = 'Domestic'
    melted_df_non_duplicate_reorder = melted_df_non_duplicate[['Airline', 'Aircraft', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt', 'FlightType', 'destination_type',
       'BayType', 'bodytype', 'Belt_FirstBag_hour', 
       'final_slot', 'count','TerminalArrival']] if OpName=='13' else  melted_df_non_duplicate[['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt', 'FlightType', 'destination_type',
       'BayType', 'bodytype', 'Belt_FirstBag_hour', 
       'final_slot', 'count','TerminalArrival']]
    
    
    
    melted_df_non_duplicate_reorder = melted_df_non_duplicate_reorder[abs(melted_df_non_duplicate_reorder['count']-melted_df_non_duplicate_reorder['count'].mean())<=abs(3*melted_df_non_duplicate_reorder['count'].std())]
    df3_json = melted_df_non_duplicate_reorder.to_json(orient='split')
#     print(melted_df_non_duplicate_reorder.Belt_FirstBag_hour.value_counts())
    #Can only use .dt accessor with datetimelike values
    # when no data available 
    
    
    #####################################################################################################
    #                                                                                                   #
    #                                   Belt-met                                                                              #
    #                                                                                                   #
    ####################################################################################################
#     df3 = df3.dropna()
    if(OpName == '13'):
        df3['FlightType'] = 'Domestic'
    df3 = df3.dropna()
    df3['time_diff_Belt_FirstBag'] = ((pd.to_datetime(df3['Belt_FirstBag']) - pd.to_datetime(df3['On_Block_Time'])).astype('<m8[m]').astype(int))
    df3['time_diff_Belt_LastBag'] = ((pd.to_datetime(df3['Belt_LastBag']) - pd.to_datetime(df3['On_Block_Time'])).astype('<m8[m]').astype(int))
#     for domestic and internation flightType
    df3['belt_met_status'] = np.where((df3['FlightType'] =='Domestic'),np.where((df3['time_diff_Belt_FirstBag'] <=10) &  (df3['time_diff_Belt_LastBag'] <=30), 'Belt SLA Met', 'Belt SLA NotMet'),np.where((df3['time_diff_Belt_FirstBag'] <=15) &   (df3['time_diff_Belt_LastBag'] <=40), 'Belt SLA Met', 'Belt SLA NotMet'))

    Belt_Met_Not_Met = df3['belt_met_status'].str.get_dummies()
    combindDF = pd.concat([df3,Belt_Met_Not_Met], axis=1)
    combindDF['Belt_met_count'] = combindDF.groupby(['flightnumber_arrival'])['Belt SLA Met'].transform('sum')
    combindDF['Belt_not_met_count'] = combindDF.groupby(['flightnumber_arrival'])['Belt SLA NotMet'].transform('sum')
    combindDF['time_diff_Belt_LastBag'] = combindDF.groupby(['flightnumber_arrival','belt_met_status'])['time_diff_Belt_LastBag'].transform('mean').round()
    combindDF['time_diff_Belt_FirstBag'] = combindDF.groupby(['flightnumber_arrival','belt_met_status'])['time_diff_Belt_FirstBag'].transform('mean').round()
    if(OpName=='22'):
        combindDF.drop(['Belt SLA Met','Belt SLA NotMet','slot_last', 'slot', 'slots_min','Logid', 'Aircraft',
           'FromAirport_IATACode','Belt_LastBag_hour','AircraftRegistration_Arrival'], axis = 1,inplace=True) 
    else:
        combindDF.drop(['Belt SLA Met','Belt SLA NotMet','slot_last', 'slot', 'slots_min','Logid', 'Aircraft',
           'FromAirport_IATACode','Belt_LastBag_hour','Aircraft'], axis = 1,inplace=True) 
    combindDF = combindDF[['flightnumber_arrival','Airline','belt_met_status','belt','TerminalArrival','On_Block_Time','Belt_FirstBag','Belt_LastBag','FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_minute','Belt_LastBag_minute','time_diff_Belt_LastBag','time_diff_Belt_FirstBag','Belt_met_count','Belt_not_met_count']]
    combindDF_final = combindDF.drop_duplicates(subset =['flightnumber_arrival','belt_met_status'], keep = 'first') 
    df4_json = combindDF_final.to_json(orient='split')
    return {
     'statusCode': 200,
     'Belt_heat':json.loads(df3_json),
     'Belt_met':json.loads(df4_json)
    
    }