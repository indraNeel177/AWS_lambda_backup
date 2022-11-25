import json
import pandas as pd
import numpy as np
import boto3
import time
import s3fs
import io
from datetime import datetime

def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    max_date=None
    min_date= None
    kpi_name=None
    
    #OpName =event['OperationUnit']
    OpName=event['OperationUnit']
    Airline=event['Airline']
    
    # FlightNumber =event['FlightNumber']
    entity='2'

    condition1 = '' if Airline=="" else f" and entity='{entity}'"

    equipments = event['equipments']
    selected_equipment=list(event['selected_equipment'])
    
    
    selected_equipment_condition="" if selected_equipment==None or len(selected_equipment)==0 else f""" AND substr(split_part(DevId, '_',3), 2) IN ({str(selected_equipment)[1:-1]})"""
    
    equipment_filter_condition_1= "" if equipments=="" else f" and regexp_like(devid, '{equipments}')"
    equipment_filter_condition= "" if equipments=="" else f" and regexp_like(t1.devid, '{equipments}')"
    
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d').date()
        if 'KPI' in event.keys() and event.get("KPI")!=None and event.get("KPI")!="":
            kpi_name=event.get("KPI")
    
    result_dict={}
    if kpi_name=='ignition_on_time':
        
        query_for_ignition_by_fuel_type=f"""SELECT t2.ShortDevName AS DevName,
                t1.hrs,
                t1.type,
                t2.operationunit,
                 t2.entity
        FROM 
            (SELECT DevId,
                sum(cast(Duration AS double))/3600 AS hrs,
                if(DevId LIKE '%EBT%','Battery','Diesel') AS type
            FROM EquipActivityLogs_parquet 
            WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                  AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                  AND flogdate!=''
                 AND type='ig_on'   and Duration!='nan' and Duration!=''
            GROUP BY  DevId) AS t1
        LEFT JOIN 
            (SELECT distinct ShortDevName,
                 DevId,
                operationunit,
                entity
            FROM EquipmentMaster_parquet WHERE operationunit='{OpName}' {equipment_filter_condition_1} {condition1}) AS t2
            ON t1.devid=t2.devid
        """
        print(query_for_ignition_by_fuel_type)
        response = athena.start_query_execution(QueryString=query_for_ignition_by_fuel_type, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
        ignition_on_by_fuel_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
        
        ignition_on_by_fuel_raw_data_json=ignition_on_by_fuel_raw_data.to_json(orient='split')
        
        result_dict['Ignition_ON_by_Fuel_Type']= json.loads(ignition_on_by_fuel_raw_data_json)
    
    elif kpi_name=='equipment_moved_when_on':
        query_for_ignition_on_flight_movement_trips=f"""SELECT m2.DevName AS DevName,
             m2.hrs AS hrs_y,
             m1.hrs AS hrs_x,
             m1.type,
             m2.legsize/1000 AS legsize,
             m2.entity
    FROM 
        (SELECT t2.ShortDevName AS DevName,
             t1.hrs,
             t1.type,
             t2.operationunit,
             t2.entity
        FROM 
            (SELECT DevId,
             sum(cast(Duration AS double))/3600.0 AS hrs,
             if(DevId LIKE '%EBT%','Battery','Diesel') AS type
            FROM EquipActivityLogs_parquet
            WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                    AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                    AND flogdate!=''
                    AND lower(type)='ig_on'
                    AND Duration!='nan'
                    AND Duration!=''
            GROUP BY  DevId) AS t1
            LEFT JOIN 
                (SELECT DISTINCT ShortDevName,
             DevId,
             operationunit,
             entity
                FROM EquipmentMaster_parquet
                WHERE operationunit='{OpName}' {equipment_filter_condition_1} {condition1}) AS t2
                    ON t1.devid=t2.devid) m1
            JOIN 
            (SELECT t1.DevId,
             t2.ShortDevName AS DevName,
            sum(t1.hrs) AS hrs,
            t2.operationunit,
             t2.entity,
            sum(cast(t1.legsize AS double)) AS legsize
            FROM 
                (SELECT DevId,
            sum(date_diff('second',date_parse(flogdate, '%Y-%m-%d %H:%i:%s'),date_parse(tlogdate, '%Y-%m-%d %H:%i:%s')))/3600.0 AS hrs,legsize
                FROM EquipActivityLogs_parquet
                WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                        AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                        AND type='Movement'
                GROUP BY  DevId, legsize) AS t1
                LEFT JOIN 
                    (SELECT DISTINCT ShortDevName,
             DevId,
            operationunit,
            entity
                    FROM EquipmentMaster_parquet
                    WHERE operationunit='{OpName}') AS t2
                        ON t1.devid=t2.devid
                    WHERE operationunit='{OpName}' {equipment_filter_condition} {condition1} 
                    GROUP BY  t1.DevId, t2.ShortDevName, t2.operationunit, t2.entity) m2
                    ON m1.DevName=m2.DevName
                    AND m1.operationunit=m2.operationunit
                AND m1.entity=m2.entity
         """
        
        print(query_for_ignition_on_flight_movement_trips)
        response = athena.start_query_execution(QueryString=query_for_ignition_on_flight_movement_trips, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))
        
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        
        while status=='RUNNING' or status=="QUEUED":
            # print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
        
        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/'+query_execution_id+'.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
        df_movement_ignition = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
        
        df_movement_ignition['mov_igon_ratio']= df_movement_ignition.hrs_y/(df_movement_ignition.hrs_x + df_movement_ignition.hrs_y)
        df_movement_ignition = df_movement_ignition.round(2)
        df_movement_ignition = df_movement_ignition.rename(columns={'hrs_x':'Ig_on_hrs','hrs_y':'Movement_hrs'})
        df_movement_ignition['Ig_on_hrs_time']= pd.to_timedelta(round(df_movement_ignition.Ig_on_hrs,1), unit='h')
        #Movement_hrs
        df_movement_ignition['Movement_hrs_time']= pd.to_timedelta(round(df_movement_ignition.Movement_hrs,1), unit='h')
        df_movement_ignition.mov_igon_ratio = df_movement_ignition.mov_igon_ratio*100
        df_movement_ignition.drop([ 'Ig_on_hrs', 'type',  'entity','Movement_hrs', 'Ig_on_hrs_time','Movement_hrs_time'], axis=1, inplace=True)
        df_movement_ignition[(df_movement_ignition.legsize-df_movement_ignition.legsize.mean()).abs() <= (3*df_movement_ignition.legsize.std())]
        df_movement_ignition_json = df_movement_ignition.to_json(orient='split')
        
        result_dict['Ignition_Movement_trips']= json.loads(df_movement_ignition_json)
        
    elif kpi_name=='ignition_on_by_device':
        
        condition2 = '' if Airline=="" else f" and t2.entity='{entity}'"
        query_for_ignition_on_by_device=f"""SELECT m1.DevName as DevName,
             m2.hrs AS hrs,
             m1.TimeServed as TimeServed
    FROM 
        (SELECT t2.ShortDevName AS DevName,
            t1.TimeServed,
             t1.DevId
        FROM 
            (SELECT sum(cast(Duration AS integer))/3600.0 AS TimeServed ,
            DevId
            FROM EquipActivityLogs_parquet
            WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                    AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                    and flogdate!=''
                    AND Duration is NOT null
                    AND Duration!=''
                     and lower(type)='ig_on'
            GROUP BY  DevId) AS t1
            LEFT JOIN 
                (SELECT DISTINCT ShortDevName,
             DevId,
             OperationUnit
                FROM EquipmentMaster_parquet
                WHERE operationunit='{OpName}' {equipment_filter_condition_1} {condition1}) AS t2
                    ON t1.DevId=t2.DevId
                WHERE OperationUnit='{OpName}')as m1
            INNER JOIN 
            (SELECT t2.ShortDevName AS DevName,
             t1.hrs,
             t1.type,
             t2.operationunit,
             t2.entity
            FROM 
                (SELECT DevId,
             sum(cast(Duration AS double))/3600.0 AS hrs,
             if(DevId LIKE '%EBT%','Battery','Diesel') AS type
                FROM EquipActivityLogs_parquet
                WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                        AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                        and flogdate!=''
                        AND lower(type)='ig_on'
                        AND Duration!='nan'
                        AND Duration!=''
                GROUP BY  DevId) AS t1
                LEFT JOIN 
                    (SELECT DISTINCT ShortDevName,
             DevId,
             operationunit,
             entity
                    FROM EquipmentMaster_parquet
                    WHERE operationunit='{OpName}' {condition2}) AS t2
                        ON t1.devid=t2.devid) AS m2
                    ON m1.DevName=m2.DevName """
        
        print(query_for_ignition_on_by_device)
        response = athena.start_query_execution(QueryString=query_for_ignition_on_by_device, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
        ignition_on_by_device_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
        
        # ignition_on_by_device_raw_data['hrs_time'] = pd.to_timedelta(round(ignition_on_by_device_raw_data.hrs,1), unit='h')
        # ignition_on_by_device_raw_data['TimeServed_time']= pd.to_timedelta(round(ignition_on_by_device_raw_data.TimeServed,1), unit='h')
        ignition_on_by_device_raw_data = round(ignition_on_by_device_raw_data,1)
        
        ignition_on_by_device_raw_data_json = ignition_on_by_device_raw_data.to_json(orient='split')

        result_dict['Ignition_ON_by_device']= json.loads(ignition_on_by_device_raw_data_json)
        
    elif kpi_name=='cumulative_equipment_utilization':
        query_for_cumulative_equipment_utilization_ignition_on_time=f"""SELECT t1.hour_of_day AS hour_of_day ,
                 t1.devid AS Devid, t1.ts
        FROM 
            (SELECT hour(date_parse(flogdate,
                 '%Y-%m-%d %H:%i:%s')) AS hour_of_day, DevId,
                 sum(date_diff('second',date_parse(flogdate, '%Y-%m-%d %H:%i:%s'),date_parse(tlogdate, '%Y-%m-%d %H:%i:%s'))/3600) as ts
            FROM EquipActivityLogs_parquet
            WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                    AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                    AND lower(type)='ig_on'
                    AND flogdate!='' AND tlogdate!='' and flogdate is not null and tlogdate is not null
             and flogdate!='nan' and tlogdate!='nan' 
                    {selected_equipment_condition} 
            GROUP BY  hour(date_parse(flogdate, '%Y-%m-%d %H:%i:%s')),DevId
            ORDER BY  hour_of_day ) AS t1
        LEFT JOIN 
            (SELECT DISTINCT devid,
                OperationUnit
            FROM EquipmentMaster_parquet )as t2
            ON t1.devid=t2.devid
        WHERE t1.ts>0 and t2.operationunit = '{OpName}'  {equipment_filter_condition} group by t1.hour_of_day,t1.devid,t1.ts"""
        
        print(query_for_cumulative_equipment_utilization_ignition_on_time)
        response = athena.start_query_execution(QueryString=query_for_cumulative_equipment_utilization_ignition_on_time, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
        cumulative_equipment_utilization_ignition_on_time_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
        
        # if cumulative_equipment_utilization_ignition_on_time_raw_data.empty:
        #     ignition_cumsum_det={'hour_of_day':['NA'],'Devid':['NA'],'count':['NA'],'cumsum':['NA']}
        #     ignition_cumsum_det_json = pd.DataFrame(ignition_cumsum_det).to_json(orient='split')
        #     result_dict['Ignition_cumsum']= json.loads(ignition_cumsum_det_json)
        # else:
        #     #cummilative sum ignition
        #     cumulative_equipment_utilization_ignition_on_time_raw_data['devices'] = cumulative_equipment_utilization_ignition_on_time_raw_data.Devid.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
        #     cumulative_equipment_utilization_ignition_on_time_raw_data['DevName'] =cumulative_equipment_utilization_ignition_on_time_raw_data.devices.str[1:]
        #     ignition_cumsum=cumulative_equipment_utilization_ignition_on_time_raw_data[['hour_of_day','Devid']]
        #     ignition_cumsum_det=ignition_cumsum.groupby(['hour_of_day','Devid']).agg({'Devid':'count'}).rename(columns={'Devid':'count'}).reset_index()
        #     ignition_cumsum_det['cumsum'] = ignition_cumsum_det.groupby(by=['Devid'])['count'].transform(lambda x: x.cumsum())
        #     ignition_cumsum_det_json = ignition_cumsum_det.to_json(orient='split')
        #     result_dict['Ignition_cumsum']= json.loads(ignition_cumsum_det_json)
            
        if cumulative_equipment_utilization_ignition_on_time_raw_data.empty:
            Ignition_heatmap = {'hour_of_day':['NA'],'DevName':['NA'],'count':['NA']}
            ignition_heatmap_json = pd.DataFrame(Ignition_heatmap).to_json(orient='split')
            ignition_cumsum_det={'hour_of_day':['NA'],'Devid':['NA'],'count':['NA'],'cumsum':['NA']}
            ignition_cumsum_det_json = pd.DataFrame(ignition_cumsum_det).to_json(orient='split')
            result_dict['Ignition_cumsum']= json.loads(ignition_cumsum_det_json)
            result_dict['Ignition_heatmap']= json.loads(ignition_heatmap_json)
        else:
            #cummilative sum ignition
            cumulative_equipment_utilization_ignition_on_time_raw_data['devices'] = cumulative_equipment_utilization_ignition_on_time_raw_data.Devid.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
            cumulative_equipment_utilization_ignition_on_time_raw_data['DevName'] =cumulative_equipment_utilization_ignition_on_time_raw_data.devices.str[1:]
            ignition_cumsum=cumulative_equipment_utilization_ignition_on_time_raw_data[['hour_of_day','Devid','ts']]
            ignition_cumsum= ignition_cumsum.sort_values(by='hour_of_day')
            #ignition_cumsum_det=ignition_cumsum.groupby(['hour_of_day','Devid']).agg({'Devid':'count'}).rename(columns={'Devid':'count'}).reset_index()
            ignition_cumsum['cumsum'] = ignition_cumsum.groupby(['Devid'])['ts'].transform(lambda x: x.cumsum()).round(1)
            ignition_cumsum_det=ignition_cumsum.copy()
            ignition_cumsum_det.rename(columns={'ts':'count'},inplace=True)
            ignition_cumsum_det_json = ignition_cumsum_det.to_json(orient='split')
            #ignition heatmap
            cumulative_equipment_utilization_ignition_on_time_raw_data = cumulative_equipment_utilization_ignition_on_time_raw_data.drop(['Devid','devices'],axis=1)
            cumulative_equipment_utilization_ignition_on_time_raw_data=cumulative_equipment_utilization_ignition_on_time_raw_data[['hour_of_day','DevName','ts']]
            cumulative_equipment_utilization_ignition_on_time_raw_data = cumulative_equipment_utilization_ignition_on_time_raw_data.sort_values(by='hour_of_day')
            Ignition_heatmap=cumulative_equipment_utilization_ignition_on_time_raw_data.groupby(['hour_of_day','DevName']).agg({'ts':'sum'}).reset_index().round(1)
            Ignition_heatmap.rename(columns={'ts':'count'},inplace=True)
            ignition_heatmap_json = Ignition_heatmap.to_json(orient='split')
            result_dict['Ignition_cumsum']= json.loads(ignition_cumsum_det_json)
            result_dict['Ignition_heatmap']= json.loads(ignition_heatmap_json)
        
        
            
        
    return {
        'statusCode': 200,
        'result_set':result_dict
        
    }
