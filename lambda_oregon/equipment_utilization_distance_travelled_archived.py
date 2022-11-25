import json
import pandas as pd
import numpy as np
import boto3
import time
# import s3fs
import io
from datetime import datetime


def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_athena_dump = "s3://aws-athena-result-zestiot/athena_dumps/"
    today = str(datetime.now().date())
    max_date = None
    min_date = None

    # OpName =event['OperationUnit']
    OpName = event['OperationUnit']
    Airline = event['Airline']
    # FlightNumber =event['FlightNumber']

    equipments = event['equipments']

    equipment_filter_condition = "" if equipments == "" else f" and regexp_like(t1.devid, '{equipments}') "

    condition2 = '' if Airline == "" else f"and t1.DevId like '%SG%' "

    if event != None and event != "":
        if ('min_date' in event.keys()) and (event.get("min_date") != None) and (event.get("min_date") != ""):
            min_date_str = event.get("min_date")
            min_date = datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date") != None and event.get("max_date") != "":
            max_date_str = event.get("max_date")
            max_date = datetime.strptime(max_date_str, '%Y-%m-%d').date()

    query_for_distance_travelled_agg = f"""SELECT t1.devid,
                 t2.operationunit,
                 t2.Entity,
                 sum(cast(t1.LegSize AS integer)/1000) AS total_distance
        FROM 
            (SELECT devid,
                 LegSize
            FROM EquipActivityLogs_parquet
            WHERE date_parse(flogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                    AND date_parse(flogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                    AND type='Movement'
                    AND cast(LegSize AS integer)<10000 )as t1
        LEFT JOIN 
            (SELECT devid,
                 operationunit,
                 entity
            FROM EquipmentMaster_parquet) AS t2
            ON t1.DevId=t2.DevId
        WHERE operationunit='{OpName}' {equipment_filter_condition} {condition2}
        GROUP BY  t1.devid,
                operationunit,
                Entity"""

    print(query_for_distance_travelled_agg)
    response = athena.start_query_execution(QueryString=query_for_distance_travelled_agg,
                                            QueryExecutionContext={'Database': 'avileap_prod'},
                                            ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))

    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']

    while status == 'RUNNING' or status == "QUEUED":
        print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

    s3 = boto3.client('s3')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/' + query_execution_id + '.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
    eu_for_distance_travelled_agg_raw_data = pd.concat([pd.read_csv(
        io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1", error_bad_lines=False)
                                                        for i in keys if i.endswith('csv')], ignore_index=True)

    df2_1_avg = round(eu_for_distance_travelled_agg_raw_data.total_distance.mean())
    df2_1_avg_json = {'Total_Distance_Travelled_avg': df2_1_avg}
    df2_1_avg_json_op = pd.DataFrame(df2_1_avg_json, index=['0']).to_json(orient='split')

    dd1 = eu_for_distance_travelled_agg_raw_data.devid.str.split("_", expand=True).drop([0, 1, 3],
                                                                                        axis=1).reset_index().drop(
        'index', axis=1).rename(columns={2: 'devname1'})
    eu_for_distance_travelled_agg_raw_data['DevName'] = dd1.devname1.str[1:]
    df2_json = eu_for_distance_travelled_agg_raw_data.to_json(orient='split')

    return {
        'statusCode': 200,
        'Total_Distance_Travelled_avg': json.loads(df2_1_avg_json_op),
        'Total_Distance_Travelled': json.loads(df2_json),
    }