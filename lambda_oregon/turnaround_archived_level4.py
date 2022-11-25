import pandas as pd
from datetime import datetime, timedelta
import time
import io
import s3fs
import json
import boto3



def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    
    max_date=(datetime.now()-timedelta(1)).date()
    #min_date=(datetime.now()-timedelta(31)).date()
    
    # YYYY-MM-DD format
    minDate_value= event['min_date']
    min_date= pd.to_datetime(minDate_value).date()
    
    
    
    #OpName =event['OperationUnit']
    OpName=event['OperationUnit']
    Airline=event['Airline']
    FlightNumber =event['FlightNumber']
    
    #min_date=(datetime.now()-timedelta(31)).date()
    #if event!=None and event!="":
    #    if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
    #        min_date_str=event.get("min_date")+" 00:00:00"
    #        min_date=datetime.strptime(min_date_str, '%Y-%m-%d %H:%M:%S').date()
    #    if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
    #       max_date_str=event.get("max_date")+" 23:59:59"
    #        max_date=datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S').date()
    turnaround_level_4_query=f"""
    select flightnumber_departure,operationname,round(avg(Duration)) as Duration,
avg(DATE_DIFF('minute', date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'), 
date_parse(flogdate, '%Y-%m-%d %H:%i:%s')))
as time_arrival,
avg(DATE_DIFF('minute',date_parse(flogdate, '%Y-%m-%d %H:%i:%s'),
date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s'))) as time_departure   
from ( 
select t1.flightnumber_departure,t1.on_block_time,t1.off_block_time,t2.flogdate,t2.operationname,t2.Duration from (
select logid,flightnumber_departure,on_block_time,off_block_time, operationunit
from avileap_prod.dailyflightschedule_merged_parquet
where 
date_parse(on_block_time, '%Y-%m-%d %H:%i:%s')>date('{min_date}')
and date_parse(on_block_time, '%Y-%m-%d %H:%i:%s')<=date('{max_date}')
and  logid != '' AND logid is not null and on_block_time!='' and  on_block_time is not null 
and off_block_time!='' and off_block_time is not null and operationunit='{OpName}'
) as t1 left join
(select flight_pk,flightno,operationname,slogdate,flogdate,cast(duration as integer) as Duration from avileap_prod.equipactivitylogs_parquet where
date_parse(slogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date}')
and date_parse(slogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date}')
 AND slogdate != ''
            AND slogdate != ''
            AND slogdate is not null
            AND slogdate is not null
            and operationname not in('ETT','PCA','CAR')
            and OperationName not like 'ac:%'
            and operationname is not null
            and operationname  != ''
            and flight_pk  != ''
            and flight_pk  is not null 
            and flightno  != ''
            and flightno  is not null 
            and flogdate !=''
            and Duration is not null
          	and Duration != ''
            -- and Assigned=1 
            ) as t2 on t1.logid=t2.flight_pk
         
) as temp1 where flightnumber_departure='{FlightNumber}' group by flightnumber_departure,operationname


    """
    print(turnaround_level_4_query)
    response = athena.start_query_execution(QueryString=turnaround_level_4_query, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
    turnaround_level_4_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    print(turnaround_level_4_raw_data.head())
    turnaround_level_4_raw_data.time_arrival.dropna(inplace=True)
    turnaround_level_4_raw_data.time_departure.dropna(inplace=True)
    
    turnaround_level_4_raw_data=round(turnaround_level_4_raw_data)
    
    turnaround_level_4_raw_data_json=turnaround_level_4_raw_data.to_json(orient='split')
    
    
    return{
        "response_code":200,
        "level_4_all_equip_flight":json.loads(turnaround_level_4_raw_data_json)
    }
        
        