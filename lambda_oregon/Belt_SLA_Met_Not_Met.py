import json
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import numpy as np
import configparser
import boto3
import io


def lambda_handler(event, context):
    # def slots_min(x):
    #     if(x == 0 or x < 15):
    #         return '00 - 15'
    #     elif (x == 15 or x < 30):
    #         return '15 - 30'
    #     elif (x == 30 or x < 45):
    #         return '30 - 45'
    #     else:
    #         return '45 - 00'

    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources', 'db_creds_ini/db_creds_prod.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname,
                           connect_timeout=5)
    start = datetime.now()

    OpName = event['OperationUnit']
    Airline = event['Airline']
    flights = event['flights']

    # OpName=22
    # Airline= ""

    yesterday = event['yesterday']
    # yesterday = "True"

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday == "True" else ""

    condition1 = '' if Airline == "" else f"where Airline='{Airline}'"
    condition2 = '' if Airline == "" else f"and Airline='{Airline}'"

    airline_filter_condition = "" if flights == "" or flights == None else f" and FlightNumber_Arrival REGEXP '{flights}'"

    df2 = pd.read_sql(f"""
     select t1.*,t2.bodytype from (
    select Logid,Airline,Aircraft,flightnumber_arrival,FromAirport_IATACode,Belt_FirstBag,Belt_LastBag,belt,On_Block_Time,
    FlightType,IFNULL(TerminalArrival,'DISABLE') as TerminalArrival ,
    if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type
    ,if(Bay IN ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58'),'Connected','Remote') 
    AS BayType
    from DailyFlightSchedule_Merged where 
    operationunit={OpName}  {airline_filter_condition} and (
     date(Belt_FirstBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} or 
     date(Belt_LastBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition})
    ) as t1 left join (select hexcode,bodytype from FlightMaster) as t2 
    on t1.Aircraft= t2.hexcode {condition1}
    """, con=conn) if OpName == '4' or OpName == '13' else pd.read_sql(f"""
     select t1.*,t2.bodytype from (
    select Logid,Airline,Aircraft,flightnumber_arrival,FromAirport_IATACode,Belt_FirstBag,Belt_LastBag,belt,On_Block_Time,
    AircraftRegistration_Arrival,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival,
    FlightType,
    if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type
    ,if(TerminalArrival in ('T3'),'Connected','Remote') as BayType
    from DailyFlightSchedule_Merged where 
    operationunit=22  {airline_filter_condition}  and (
     date(Belt_FirstBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} or 
     date(Belt_LastBag) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}) 
    ) as t1 left join (select hexcode,bodytype,REPLACE(RegNo, '-', '') as RegNo from FlightMaster) as t2 
    on t1.AircraftRegistration_Arrival= t2.RegNo having TerminalArrival in ('T1','T2','T3') {condition2}
    """, con=conn)

    df2 = df2.copy() if Airline == "" else df2[df2.Airline == f'{Airline}'].reset_index(drop=True)

    today = pd.to_datetime('today').replace(hour=0, minute=0, second=0)
    today_last = pd.to_datetime('today').replace(hour=23, minute=59, second=59)

    yesterday_date = datetime.date(datetime.now() - timedelta(1))
    print("yesterday", yesterday_date)
    yesterday_start = pd.datetime(yesterday_date.year, yesterday_date.month, yesterday_date.day, 0, 0, 0)
    yesterday_last = pd.datetime(yesterday_date.year, yesterday_date.month, yesterday_date.day, 23, 59, 59)

    if not yesterday == 'True':
        df2.Belt_FirstBag = df2.Belt_FirstBag.apply(
            lambda x: today if pd.Timestamp(x).date() < pd.datetime.now().date() else x)
        df2.Belt_LastBag = df2.Belt_LastBag.apply(
            lambda x: today_last if pd.Timestamp(x).date() > pd.datetime.now().date() else x)
    elif yesterday == 'True':
        print("yes")
        df2.Belt_FirstBag = df2.Belt_FirstBag.apply(
            lambda x: yesterday_start if pd.Timestamp(x).date() < yesterday_date else x)
        df2.Belt_LastBag = df2.Belt_LastBag.apply(
            lambda x: yesterday_last if pd.Timestamp(x).date() > yesterday_date else x)

    # Total belt time
    df2['Belt_FirstBag_hour'] = df2.Belt_FirstBag.dt.hour
    df2['Belt_FirstBag_minute'] = df2.Belt_FirstBag.dt.minute

    df2['Belt_LastBag_hour'] = df2.Belt_LastBag.dt.hour
    df2['Belt_LastBag_minute'] = df2.Belt_LastBag.dt.minute

    # df2['slot_last'] = df2.Belt_LastBag_minute.apply(slots_min)

    # df2['slot'] = df2.Belt_FirstBag_minute.apply(slots_min)

    # df2['slots_min'] = df2.Belt_FirstBag_minute.apply(slots_min)

    #####################################################################################################
    #                                                                                                   #
    #                                   Belt-met  and Not Met                                                                            #
    #                                                                                                   #
    ####################################################################################################
    df2 = df2.dropna()
    df2['time_diff_Belt_FirstBag'] = (
        (pd.to_datetime(df2['Belt_FirstBag']) - pd.to_datetime(df2['On_Block_Time'])).astype('<m8[m]').astype(int))
    df2['time_diff_Belt_LastBag'] = (
        (pd.to_datetime(df2['Belt_LastBag']) - pd.to_datetime(df2['On_Block_Time'])).astype('<m8[m]').astype(int))
    df2['belt_met_status'] = np.where((df2['time_diff_Belt_FirstBag'] <= 10) & (df2['time_diff_Belt_LastBag'] <= 25),
                                      'Belt Met', 'Belt NotMet')
    df2 = (df2.groupby(
        ['flightnumber_arrival', 'Airline', 'belt_met_status', 'belt', 'TerminalArrival', 'On_Block_Time',
         'Belt_FirstBag', 'Belt_LastBag', 'Belt_FirstBag_minute', 'FlightType', 'destination_type', 'BayType',
         'bodytype', 'Belt_LastBag_minute', 'time_diff_Belt_LastBag', 'time_diff_Belt_FirstBag'])
           .apply(lambda x: (x['belt_met_status'] == 'Belt Met').sum())
           .reset_index(name='Belt_met_count'))
    df2 = (df2.groupby(
        ['flightnumber_arrival', 'Airline', 'belt_met_status', 'belt', 'TerminalArrival', 'On_Block_Time',
         'Belt_FirstBag', 'Belt_LastBag', 'FlightType', 'destination_type', 'BayType', 'bodytype',
         'Belt_FirstBag_minute', 'Belt_LastBag_minute', 'time_diff_Belt_LastBag', 'time_diff_Belt_FirstBag',
         'Belt_met_count'])
           .apply(lambda x: (x['belt_met_status'] == 'Belt NotMet').sum())
           .reset_index(name='Belt_not_met_count'))
    df3_json = df2.to_json(orient='split')
    return {
        'statusCode': 200,
        'Belt_met': json.loads(df3_json)

    }
