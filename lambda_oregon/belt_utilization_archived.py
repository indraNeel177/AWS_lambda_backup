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
    if OpName == '4':
        connected_bay = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
    else:
        connected_bay = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22')
         
    
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
                                END AS TerminalArrival , if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type ,if(Bay IN {connected_bay},'Connected','Remote') AS BayType
                            FROM DailyFlightSchedule_Merged_parquet
                            WHERE operationunit='{OpName}'
                                    {flights_filter}
                                    AND ( date_parse(Belt_FirstBag,'%Y-%m-%d %H:%i:%s')>=date('{min_date_str}')
                                    AND date_parse(Belt_LastBag, '%Y-%m-%d %H:%i:%s') <=date('{max_date_str}') )
                                    AND Belt_lastbag !=''
                                    AND belt_firstBag!='' AND Belt_lastbag !='None'
                                    AND belt_firstBag!='None' ) AS t1
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
    
    belt_util_archived_data = belt_util_archived_data[['Logid', 'Airline', 'Aircraft', 'flightnumber_arrival',
           'FromAirport_IATACode', 'Belt_FirstBag', 'Belt_LastBag', 'belt',
           'FlightType', 'destination_type', 'BayType', 'bodytype',
           'Belt_FirstBag_hour', 'Belt_FirstBag_minute', 'Belt_LastBag_hour',
           'Belt_LastBag_minute', 'slot_last', 'slot', 'slots_min','TerminalArrival']] if OpName=='4' or OpName =='13' else belt_util_archived_data[['Logid', 'Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
           'FromAirport_IATACode', 'Belt_FirstBag', 'Belt_LastBag', 'belt',
           'FlightType', 'destination_type', 'BayType', 'bodytype',
           'Belt_FirstBag_hour', 'Belt_FirstBag_minute', 'Belt_LastBag_hour',
           'Belt_LastBag_minute', 'slot_last', 'slot', 'slots_min','TerminalArrival']]
    
    df_new1= belt_util_archived_data[['Airline', 'Aircraft', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt','TerminalArrival',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','slot', 'slot_last']] if OpName=='4'or OpName =='13' else belt_util_archived_data[['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt','TerminalArrival',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','slot', 'slot_last']]
    
    
    melted_df = df_new1.reset_index().melt(['Airline', 'Aircraft', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','TerminalArrival'], ['slot', 'slot_last'], value_name='final_slot') if OpName=='4' or OpName =='13' else df_new1.reset_index().melt(['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt',
       'FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_hour','TerminalArrival'], ['slot', 'slot_last'], value_name='final_slot')
    
    melted_df = melted_df.sort_values(by='flightnumber_arrival')
    melted_df = melted_df.drop('variable',axis=1)
    #melted_df_non_duplicate = melted_df.drop_duplicates(subset=['flightnumber_arrival','final_slot']).reset_index(drop=True)
    #melted_df_non_duplicate['count'] = melted_df_non_duplicate.groupby(['Belt_FirstBag_hour','final_slot','belt'])['flightnumber_arrival'].transform('count')
    
    #melted_df = melted_df.drop('variable',axis=1)
    melted_df_non_duplicate = melted_df.drop_duplicates(subset=['flightnumber_arrival','final_slot']).reset_index(drop=True)
    melted_df_non_duplicate['count'] = melted_df_non_duplicate.groupby(['Belt_FirstBag_hour','TerminalArrival','final_slot','belt'])['flightnumber_arrival'].transform('count')
    melted_df_non_duplicate['FlightType'] = 'Domestic'
    # new_order = [0,1,2,4,5,6,7,8,9,10,11,3]
    #melted_df_non_duplicate = melted_df_non_duplicate[melted_df_non_duplicate.columns[new_order]]
    melted_df_non_duplicate_reorder = melted_df_non_duplicate[['Airline', 'Aircraft', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt', 'FlightType', 'destination_type',
       'BayType', 'bodytype', 'Belt_FirstBag_hour', 
       'final_slot', 'count','TerminalArrival']] if OpName=='4' or OpName =='13' else  melted_df_non_duplicate[['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
       'FromAirport_IATACode', 'belt', 'FlightType', 'destination_type',
       'BayType', 'bodytype', 'Belt_FirstBag_hour', 
       'final_slot', 'count','TerminalArrival']]
    
    #melted_df_non_duplicate_reorder_final = melted_df_non_duplicate_reorder.groupby(['Airline', 'Aircraft', 'flightnumber_arrival',
    #   'FromAirport_IATACode', 'belt', 'FlightType', 'destination_type',
    #   'BayType', 'bodytype', 'Belt_FirstBag_hour', 
    #   'final_slot', 'count','TerminalArrival']).agg('mean').reset_index().fillna(melted_df_non_duplicate_reorder['count'].mean()).round(0) if OpName=='4' else  melted_df_non_duplicate_reorder.groupby(['Airline', 'AircraftRegistration_Arrival', 'flightnumber_arrival',
    #   'FromAirport_IATACode', 'belt', 'FlightType', 'destination_type',
    #   'BayType', 'bodytype', 'Belt_FirstBag_hour', 
    #   'final_slot', 'count','TerminalArrival']).agg('mean').reset_index().fillna(melted_df_non_duplicate_reorder['count'].mean()).round(0)
    
    melted_df_non_duplicate_reorder = melted_df_non_duplicate_reorder[abs(melted_df_non_duplicate_reorder['count']-melted_df_non_duplicate_reorder['count'].mean())<=abs(3*melted_df_non_duplicate_reorder['count'].std())]
    print('++++++++++++++++++++++++++++++')
    print(f'before string {melted_df_non_duplicate_reorder["count"]}')
    melted_df_non_duplicate_reorder['count']= melted_df_non_duplicate_reorder['count']/((pd.to_datetime(max_date_str) -pd.to_datetime(min_date_str)).days)
    melted_df_non_duplicate_reorder['count']= melted_df_non_duplicate_reorder['count'].round(0)
    print('=======================================')
    print(f'before string {melted_df_non_duplicate_reorder["count"]}')
    print(melted_df_non_duplicate_reorder.columns)
    print((pd.to_datetime(max_date_str) -pd.to_datetime(min_date_str)).days)

    df3_json = melted_df_non_duplicate_reorder.to_json(orient='split')
    
     #####################################################################################################
    #                                                                                                   #
    #                                   Belt-met                                                        #
    #                                                                                                   #
    ####################################################################################################
    
    if(OpName == '13'):
        belt_util_SLA_met_archived_data['FlightType'] = 'Domestic'
    
    
    belt_util_SLA_met_archived_data = belt_util_SLA_met_archived_data.dropna()
    belt_util_SLA_met_archived_data['time_diff_Belt_FirstBag'] = ((pd.to_datetime(belt_util_SLA_met_archived_data['Belt_FirstBag']) - pd.to_datetime(belt_util_SLA_met_archived_data['On_Block_Time'])).astype('<m8[m]').astype(int))
    belt_util_SLA_met_archived_data['time_diff_Belt_LastBag'] = ((pd.to_datetime(belt_util_SLA_met_archived_data['Belt_LastBag']) - pd.to_datetime(belt_util_SLA_met_archived_data['On_Block_Time'])).astype('<m8[m]').astype(int))
#     added logic for Domestic and Internalnational flight
    belt_util_SLA_met_archived_data['belt_met_status'] = np.where((belt_util_SLA_met_archived_data['FlightType'] =='Domestic'),np.where((belt_util_SLA_met_archived_data['time_diff_Belt_FirstBag'] <=10) &  (belt_util_SLA_met_archived_data['time_diff_Belt_LastBag'] <=30), 'Belt SLA Met', 'Belt SLA NotMet'), 
                                                        np.where((belt_util_SLA_met_archived_data['time_diff_Belt_FirstBag'] <=15) &  (belt_util_SLA_met_archived_data['time_diff_Belt_LastBag'] <=40), 'Belt SLA Met', 'Belt SLA NotMet'))
    Belt_Met_Not_Met = belt_util_SLA_met_archived_data['belt_met_status'].str.get_dummies()
    combindDF = pd.concat([belt_util_SLA_met_archived_data,Belt_Met_Not_Met], axis=1)
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

    
    
    
    #combindDF.drop(['Belt SLA Met','Belt SLA NotMet','slot_last', 'slot', 'slots_min','Logid', 'Aircraft',
    #       'FromAirport_IATACode','Belt_LastBag_hour','AircraftRegistration_Arrival'], axis = 1,inplace=True) 
    combindDF = combindDF[['flightnumber_arrival','Airline','belt_met_status','belt','TerminalArrival','On_Block_Time','Belt_FirstBag','Belt_LastBag','FlightType', 'destination_type', 'BayType', 'bodytype','Belt_FirstBag_minute','Belt_LastBag_minute','time_diff_Belt_LastBag','time_diff_Belt_FirstBag','Belt_met_count','Belt_not_met_count']]
    combindDF_final = combindDF.drop_duplicates(subset =['flightnumber_arrival','belt_met_status'], keep = 'first') 
    df4_json = combindDF_final.to_json(orient='split')
    

    
    return {
     'statusCode': 200,
     'Belt_heat':json.loads(df3_json),
     'Belt_met':json.loads(df4_json)
    
    }
