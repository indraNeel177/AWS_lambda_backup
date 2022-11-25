import json
import pandas as pd
import numpy as np
import boto3
import time
import io
from datetime import datetime


def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    max_date=None
    min_date= None
    
    OpName=event['OperationUnit']
    Airline=event['Airline']
    # FlightNumber =event['FlightNumber']
    
    
    equipments = event['equipments']
    #equipments=""
    
    equipment_filter_condition_1= "" if equipments=="" else f" and regexp_like(devid, '{equipments}')"
    
    equipment_filter_condition= "" if equipments=="" else f" and regexp_like(t2.devid,'{equipments}')"
    
    flights = event['flights']
    
    selected_equipment=list(event['selected_equipment'])
    
    selected_equipment_filter="" if selected_equipment==None or len(selected_equipment)==0 else f"AND substr(split_part(DevId, '_',3), 2) in ({str(selected_equipment)[1:-1]})"
    
    print(selected_equipment_filter)
    
    airline_filter_condition= "" if flights=="" else f" where regexp_like(FlightNumber_Arrival, '{flights}')"

    condition1 = '' if Airline=="" else f" where Airline = '{Airline}'"
    
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d').date()
    
    #random was added for testing, please remove and replace by the next line
#     query_for_flight_movement_trips=f"""SELECT Airline,
#               round(((sum(param)/date_diff('day',date('{min_date_str}'),date('{max_date_str}')))*random())/3600,1) AS Total,
#              DeviceName,
#              hour_of_day, DevId 
#     FROM 
#         (SELECT t1.*,
#              if((DeviceName='COA')
#                 AND (Type='Movement')
#                 AND (((substr(FRefId, 1, 6)='PickUp')
#                 AND (substr(TRefId, 1, 6)!='PickUp'))
#                 OR ((substr(FRefId, 1, 6)='Drop')
#                 AND (substr(TRefId, 1, 6)!='Drop'))
#                 OR ((substr(TRefId, 1, 6)='Drop')
#                 AND (substr(FRefId, 1, 6)!='Drop'))
#                 OR ((substr(TRefId, 1, 6)='PickUp')
#                 AND (substr(FRefId, 1, 6)!='PickUp'))), 1, if((DeviceName='ETT')
#                 AND (operationName in('BBF','BBL','FTD','LTD','ITD','BBI')), 1, if((DeviceName!='COA')
#                 AND (DeviceName!='ETT')
#                 AND (assigned='1')
#                 AND (cast(Duration AS integer) >30),1,0))) AS param
#         FROM 
#             (SELECT Flight_PK,
#              hour(date_parse(FLogDate,
#              '%Y-%m-%d %H:%i:%s')) AS hour_of_day, substr(split_part(DevId, '_',3), 2) AS DeviceName,DevId, Type, FRefId, TRefId, operationName, Duration, assigned
#             FROM equipactivitylogs_parquet
#              WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}') AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
#                     AND flogdate!=''
#                     AND flogdate!='nan'
#                     AND Duration!=''  {equipment_filter_condition_1} {selected_equipment_filter}) AS t1
#             LEFT JOIN 
#                 (SELECT devid,
#              OperationUnit
#                 FROM EquipmentMaster_parquet) AS t2
#                     ON t1.Devid = t2.DevId
#                 WHERE t2.OperationUnit='{OpName}' )t3
#             LEFT JOIN 
            
#             (SELECT LogId,
#              Airline
#             FROM DailyFlightSchedule_Merged_parquet {airline_filter_condition}) AS t4
#             ON t3.Flight_PK=t4.LogId {condition1}
#     GROUP BY  DeviceName, hour_of_day, Airline, DevId"""


# --  sum(param) AS Total,
    query_for_flight_movement_trips=f""" SELECT Airline,
        (round(sum(param)/(date_diff('day',date('{min_date_str}'),date('{max_date_str}'))*random()),1)/60 ) as Total,
            
             DeviceName,
             hour_of_day, DevId 
    FROM 
        (SELECT t1.*,
             if((DeviceName='COA')
                AND (Type='Movement')
                AND (((substr(FRefId, 1, 6)='PickUp')
                AND (substr(TRefId, 1, 6)!='PickUp'))
                OR ((substr(FRefId, 1, 6)='Drop')
                AND (substr(TRefId, 1, 6)!='Drop'))
                OR ((substr(TRefId, 1, 6)='Drop')
                AND (substr(FRefId, 1, 6)!='Drop'))
                OR ((substr(TRefId, 1, 6)='PickUp')
                AND (substr(FRefId, 1, 6)!='PickUp'))), 1, if((DeviceName='ETT')
                AND (operationName in('BBF','BBL','FTD','LTD','ITD','BBI')), 1, if((DeviceName!='COA')
                AND (DeviceName!='ETT')
                AND (assigned='1')
                AND (cast(Duration AS integer) >30),1,0))) AS param
        FROM 
            (SELECT Flight_PK,
             hour(date_parse(FLogDate,
             '%Y-%m-%d %H:%i:%s')) AS hour_of_day, substr(split_part(DevId, '_',3), 2) AS DeviceName,DevId, Type, FRefId, TRefId, operationName, Duration, assigned
            FROM equipactivitylogs_parquet
            WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                    AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                    AND flogdate!=''
                    AND flogdate!='nan'
                    AND Duration!=''  {equipment_filter_condition_1} {selected_equipment_filter}) AS t1
            LEFT JOIN 
                (SELECT devid,
             OperationUnit
                FROM EquipmentMaster_parquet) AS t2
                    ON t1.Devid = t2.DevId
                WHERE t2.OperationUnit='{OpName}' )t3
            LEFT JOIN 
            (SELECT LogId,
             Airline
            FROM DailyFlightSchedule_Merged_parquet {airline_filter_condition}) AS t4
            ON t3.Flight_PK=t4.LogId {condition1}
    GROUP BY  DeviceName, hour_of_day, Airline, DevId """
        

    print(query_for_flight_movement_trips)
    response = athena.start_query_execution(QueryString=query_for_flight_movement_trips, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
    flight_movement_trips_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)

    
    
    if Airline!='' and Airline!=None:
        flight_movement_trips_raw_data=flight_movement_trips_raw_data[flight_movement_trips_raw_data.Airline==Airline]
    #flight_movement_trips_raw_data.to_csv("testing.csv",index=False)   
    flight_movement_trips_raw_data.drop('Airline', axis=1, inplace=True)
    flight_movement_trips_raw_data_grouped=flight_movement_trips_raw_data.groupby(['DeviceName', 'hour_of_day'])['DevId'].agg({'Total':'count'}).reset_index()
    #print("hhhhhhhhhhhhhhhhhh",flight_movement_trips_raw_data_grouped.shape)
    print(flight_movement_trips_raw_data_grouped)
    flight_movement_trips_raw_data_grouped=flight_movement_trips_raw_data_grouped.replace(r'^\s*$', np.nan, regex=True).fillna('unknown')
    
    Movement_Trips_JSON=flight_movement_trips_raw_data_grouped.to_json(orient='split')
    
    
    #print(flight_movement_trips_raw_data.columns)
    #flight_movement_trips_raw_data.to_csv("testing.csv",index=False)
    grouped_data_cumsum=flight_movement_trips_raw_data.groupby(['hour_of_day', 'DevId'])['DevId'].agg({'count':'count'}).reset_index()
    #grouped_data_cumsum=flight_movement_trips_raw_data.groupby(['hour_of_day', 'DevId'])['DevId'].agg({'count':'count'}).reset_index()
    
    grouped_data_cumsum['cumsum'] = grouped_data_cumsum.groupby(['DevId'])['count'].transform(lambda x: x.cumsum())
    grouped_data_cumsum= grouped_data_cumsum.sort_values(by=['hour_of_day', 'DevId'])
    
    movement_trips_cumsum_json = grouped_data_cumsum.to_json(orient='split')
    
    
    heatmap_Equip=flight_movement_trips_raw_data_grouped.groupby(['hour_of_day','DeviceName']).agg('sum').reset_index()
    heatmap_Equip=heatmap_Equip[['hour_of_day','DeviceName','Total']]
    heatmap_Equip_json= heatmap_Equip.to_json(orient='split')

    
    return{
        'statusCode': 200,
        'Movement_Trips_Airport':json.loads(Movement_Trips_JSON),
        'heatmap_Equip':json.loads(heatmap_Equip_json),
        'movement_trips_cumsum':json.loads(movement_trips_cumsum_json)
    }
        