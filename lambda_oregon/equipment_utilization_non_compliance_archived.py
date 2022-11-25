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
    Airline=None
    #OpName =event['OperationUnit']
    OpName=event['OperationUnit']
    Airline=event['Airline']
    
    entity='2'
  
    condition1 = '' if Airline=="" else f" and Entity='{entity}'"
    
    equipments = event['equipments']
    equipment_filter_condition_1= "" if equipments=="" else f" and regexp_like(devid, '{equipments}')"
    
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d').date()
            
    query_for_eu_noncompliance=f"""SELECT DevId
        FROM noncompliance_parquet
        WHERE date_parse(logdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                AND date_parse(logdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                AND lower(type)='overspeed' {condition1} {equipment_filter_condition_1}"""
    print(query_for_eu_noncompliance)
    response = athena.start_query_execution(QueryString=query_for_eu_noncompliance, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
    eu_noncompliance_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    df1_1 =pd.DataFrame({'NonCompliance Devices':eu_noncompliance_raw_data.DevId.nunique()},index=[0])
    df1_json = df1_1.to_json(orient='split')
    
    
    dd1= eu_noncompliance_raw_data.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
    eu_noncompliance_raw_data['DevShortName'] =dd1.devname1.str[1:]
    
    #Number of equipments that were non compliant for each type of equipment
    df2 = eu_noncompliance_raw_data.groupby('DevShortName')['DevId'].nunique().reset_index()
    df2_json = df2.to_json(orient='split')
    
    
    #Number of times each equipment was non-compliant
    
    df3= eu_noncompliance_raw_data.sort_values(by='DevShortName')
    df3_1 = df3.groupby(['DevShortName','DevId']).agg({'DevId':'count'}).rename(columns={'DevId':'Devid_count'}).reset_index()
    df3_json = df3_1.to_json(orient='split')  
    
    query_for_eu_noncompliance_hourly_basis=f"""SELECT COUNT(distinct DevId) AS NonCompliance_devices,
                HOUR( date_parse(logdate,
                 '%Y-%m-%d %H:%i:%s')) AS hour_of_day
        FROM NonCompliance_parquet
        WHERE date_parse(logdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                AND date_parse(logdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                AND lower(type)='overspeed' {condition1} and airport='{OpName}{equipment_filter_condition_1}'
        GROUP BY  HOUR( date_parse(logdate, '%Y-%m-%d %H:%i:%s'))"""
    
    print(query_for_eu_noncompliance_hourly_basis)    
    response = athena.start_query_execution(QueryString=query_for_eu_noncompliance_hourly_basis, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
    eu_noncompliance_hourly_basis_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    eu_noncompliance_hourly_basis_raw_data_json=eu_noncompliance_hourly_basis_raw_data.to_json(orient='split')
    
    
    return {
        'statusCode': 200,
        'Noncompliance_Agg': json.loads(df1_json),
        'non compliant_each_type_of_equipment': json.loads(df2_json),
        'non_compliant_Equipments': json.loads(df3_json),
        'Noncompliance_By_Hour': json.loads(eu_noncompliance_hourly_basis_raw_data_json)
    }
