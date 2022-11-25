import json
import pandas as pd
import numpy as np
import boto3
import time
import io

from datetime import datetime
def lambda_handler(event, context):
    
    def slots_min(x):
        if(x == 0 or x < 15):
            return '00 - 15'
        elif (x == 15 or x < 30):
            return '15 - 30'
        elif (x == 30 or x < 45):
            return '30 - 45'
        else:
            return '45 - 00'
    
    
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    max_date=None
    min_date= None
    flights=None
    
    OpName=event['OperationUnit']
    Airline=event['Airline']
    
    
    
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")
            max_date=datetime.strptime(max_date_str+ ' 23:59:59', '%Y-%m-%d %H:%M:%S').date()
        if 'flights' in event.keys() and event.get("flights")!=None and event.get("flights")!="":             
            flights = event['flights']
            
    condition1 = '' if Airline=="" or Airline==None else f"where Airline='{Airline}'"
    condition2 = '' if Airline=="" or Airline==None else f"and Airline='{Airline}'"
    
    flights_filter=f"AND regexp_like(FlightNumber_Arrival, '{flights}') " if flights!='' and  flights!=None else ''
            
            
    
    query_to_execute=f"""SELECT t1.*,
                                 t2.bodytype
                        FROM 
                            (SELECT Logid,
                                 Airline,
                                 Aircraft,
                                 flightnumber_arrival,
                                 FromAirport_IATACode,
                                 Belt_FirstBag,
                                 Belt_LastBag,
                                 belt,
                                 On_Block_Time,
                                 AircraftRegistration_Arrival,
                                 if(TerminalArrival IN ('1C','1D'),'T1',TerminalArrival) AS TerminalArrival ,FlightType, if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type ,if(TerminalArrival IN ('T3'),'Connected','Remote') AS BayType
                            FROM dailyflightschedule_merged_parquet
                            WHERE operationunit='{OpName}'
                                    {flights_filter}
                                    AND ( date_parse(Belt_FirstBag,'%Y-%m-%d %H:%i:%s')>=date('{min_date_str}')
                                    AND date_parse(Belt_LastBag, '%Y-%m-%d %H:%i:%s') <=date('{max_date_str}') )) AS t1
                        LEFT JOIN 
                            (SELECT hexcode,
                                 bodytype,
                                 REPLACE(RegNo,
                                 '-', '') AS RegNo
                            FROM FlightMaster_parquet) AS t2
                            ON t1.AircraftRegistration_Arrival= t2.RegNo
                        where TerminalArrival IN ('T1','T2','T3') {condition2}
                        group by Logid,
         Airline,
         Aircraft,
         flightnumber_arrival,
         FromAirport_IATACode,
         Belt_FirstBag,
         Belt_LastBag,
         belt ,On_Block_Time,AircraftRegistration_Arrival,
         FlightType,TerminalArrival,destination_type,BayType,bodytype
                        """ if OpName=='22' else f"""SELECT t1.*,
                                t2.bodytype
                        FROM 
                            (SELECT Logid,
                                Airline,
                                Aircraft,
                                flightnumber_arrival,
                                FromAirport_IATACode,
                                Belt_FirstBag,
                                Belt_LastBag,
                                belt ,
                                On_Block_Time,
                                FlightType,
                                case
                                WHEN TerminalArrival is NULL THEN
                                'DISABLE'
                                ELSE TerminalArrival
                                END AS TerminalArrival , if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type ,if(Bay IN ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58'),'Connected','Remote') AS BayType
                            FROM DailyFlightSchedule_Merged_parquet
                            WHERE operationunit='{OpName}'
                                    {flights_filter}
                                    AND ( date_parse(Belt_FirstBag,'%Y-%m-%d %H:%i:%s')>=date('{min_date_str}')
                                    AND date_parse(Belt_LastBag, '%Y-%m-%d %H:%i:%s') <=date('{max_date_str}') )
                                    AND Belt_lastbag !=''
                                    AND belt_firstBag!='' ) AS t1
                        LEFT JOIN 
                            (SELECT hexcode,
                                bodytype
                            FROM FlightMaster_Parquet) AS t2
                            ON t1.Aircraft= t2.hexcode {condition1}
                            group by Logid,
         Airline,
         Aircraft,
         flightnumber_arrival,
         FromAirport_IATACode,
         Belt_FirstBag,
         Belt_LastBag,
         belt ,
         On_Block_Time,
         FlightType,TerminalArrival,destination_type,BayType,bodytype
                            """
                            
                            
                            
    print(query_to_execute)
    response = athena.start_query_execution(QueryString=query_to_execute, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))
    
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']
    
    while status=='RUNNING' or status=="QUEUED":
        # print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
    
    # query_execution_id = '0b191db7-2bfd-4a9c-a977-ee39690711b7'
    s3 = boto3.client('s3')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/'+query_execution_id+'.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
    belt_util_archived_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    
    belt_util_archived_data = belt_util_archived_data.copy()  if Airline=="" else belt_util_archived_data[belt_util_archived_data.Airline==f'{Airline}'].reset_index(drop=True)
    
    # belt_util_archived_data.Belt_FirstBag = belt_util_archived_data.Belt_FirstBag.apply(lambda x: min_date if pd.Timestamp(x).date() < datetime.strptime(min_date_str,"%Y-%m-%d").date() else x )
    # belt_util_archived_data.Belt_LastBag = belt_util_archived_data.Belt_LastBag.apply(lambda x: max_date if pd.Timestamp(x).date() > datetime.strptime(max_date_str,"%Y-%m-%d").date() else x )
    
    
    
    belt_util_archived_data['Belt_FirstBag']= pd.to_datetime(belt_util_archived_data.Belt_FirstBag)
    belt_util_archived_data['Belt_LastBag']= pd.to_datetime(belt_util_archived_data.Belt_LastBag)
    
    belt_util_archived_data['Belt_FirstBag_hour']= belt_util_archived_data.Belt_FirstBag.dt.hour
    belt_util_archived_data['Belt_FirstBag_minute']= belt_util_archived_data.Belt_FirstBag.dt.minute
    
    belt_util_archived_data['Belt_LastBag_hour']= belt_util_archived_data.Belt_LastBag.dt.hour
    belt_util_archived_data['Belt_LastBag_minute']= belt_util_archived_data.Belt_LastBag.dt.minute
    
    belt_util_archived_data['slot_last'] = belt_util_archived_data.Belt_LastBag_minute.apply(slots_min)
    
    belt_util_archived_data['slot'] = belt_util_archived_data.Belt_FirstBag_minute.apply(slots_min)
    
    belt_util_archived_data['slots_min'] = belt_util_archived_data.Belt_FirstBag_minute.apply(slots_min) 
    
    belt_util_SLA_met_archived_data = belt_util_archived_data
    
     #####################################################################################################
    #                                                                                                   #
    #                                   Belt-met                                                                              #
    #                                                                                                   #
    ####################################################################################################
    belt_util_SLA_met_archived_data = belt_util_SLA_met_archived_data.dropna()
    belt_util_SLA_met_archived_data['time_diff_Belt_FirstBag'] = ((pd.to_datetime(belt_util_SLA_met_archived_data['Belt_FirstBag']) - pd.to_datetime(belt_util_SLA_met_archived_data['On_Block_Time'])).astype('<m8[m]').astype(int))
    belt_util_SLA_met_archived_data['time_diff_Belt_LastBag'] = ((pd.to_datetime(belt_util_SLA_met_archived_data['Belt_LastBag']) - pd.to_datetime(belt_util_SLA_met_archived_data['On_Block_Time'])).astype('<m8[m]').astype(int))
    belt_util_SLA_met_archived_data['belt_met_status'] = np.where((belt_util_SLA_met_archived_data['time_diff_Belt_FirstBag'] <=10) &  (belt_util_SLA_met_archived_data['time_diff_Belt_LastBag'] <=25), 'Belt Met', 'Belt NotMet')
    belt_util_SLA_met_archived_data = (belt_util_SLA_met_archived_data.groupby(['flightnumber_arrival','Airline','belt_met_status','belt','TerminalArrival','On_Block_Time','Belt_FirstBag','Belt_LastBag','Belt_FirstBag_minute','FlightType', 'destination_type', 'BayType', 'bodytype','Belt_LastBag_minute','time_diff_Belt_LastBag','time_diff_Belt_FirstBag'])
         .apply(lambda x: (x['belt_met_status']== 'Belt Met').sum())
         .reset_index(name='Belt_met_count'))
    belt_util_SLA_met_archived_data = (belt_util_SLA_met_archived_data.groupby(['flightnumber_arrival','Airline','belt_met_status','belt','TerminalArrival','On_Block_Time','Belt_FirstBag','Belt_LastBag','FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_minute','Belt_LastBag_minute','time_diff_Belt_LastBag','time_diff_Belt_FirstBag','Belt_met_count'])
         .apply(lambda x: (x['belt_met_status']== 'Belt NotMet').sum())
         .reset_index(name='Belt_not_met_count'))
    df4_json = belt_util_SLA_met_archived_data.to_json(orient='split')
    

    
    return {
     'statusCode': 200,
     'Belt_met':json.loads(df4_json)
    
    }
