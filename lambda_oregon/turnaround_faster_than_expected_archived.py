import pandas as pd
from datetime import datetime, timedelta
import time
import io
import json
import boto3



def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    max_date=(datetime.now()-timedelta(1)).date() if event['max_date']=="" else event['max_date']
    
    max_date=(datetime.now()-timedelta(1)).date()
    #min_date=(datetime.now()-timedelta(31)).date()
    
    # YYYY-MM-DD format
    minDate_value= event['min_date']
    min_date= pd.to_datetime(minDate_value).date()
    Airline=None
    
    
    OpName =event['OperationUnit']
    Airline = event['Airline']
    
    #min_date=(datetime.now()-timedelta(31)).date()
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d').date()
        if 'Airline' in event.keys() and event.get("Airline")!=None and event.get("Airline")!="":
            Airline=event.get("Airline")
            
    flights_faster_than_expected_query=f"""select t1.Airline,
    avg(t1.FTE)*100 as FTE,t2.BodyType as BodyType from (
  select Airline,AircraftRegistration_Arrival,if(date_diff('minute',date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'),date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s'))<=date_diff('minute',date_parse(STA, '%Y-%m-%d %H:%i:%s'),date_parse(STD, '%Y-%m-%d %H:%i:%s')),1,0) as FTE
  ,FlightNumber_Departure from dailyflightschedule_merged_parquet
  where OperationUnit='{OpName}' and date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s') >=date('{min_date_str}') and 
date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s') <=date('{max_date_str}')  and STA !='' and STD!='' and on_block_time!='' and off_block_time!='' and Sensor_ATA !='None' and Sensor_ATD !='None' AND on_block_time != 'None'
            AND off_block_time != 'None')
    as t1
    left join
    (select Hexcode,BodyType,replace(RegNo, '-', '') as RegNo  from FlightMaster_parquet) as t2
    on t1.AircraftRegistration_Arrival = t2.RegNo group by t1.Airline,t2.BodyType
    """ if OpName=='22' else f"""select t1.Airline as Airline,avg(t1.FTE)*100 as FTE ,t2.BodyType as BodyType from (
  select Airline,Aircraft,if(date_diff('minute',date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'),date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s'))<=date_diff('minute',date_parse(STA, '%Y-%m-%d %H:%i:%s'),date_parse(STD, '%Y-%m-%d %H:%i:%s')),1,0) as FTE
  ,FlightNumber_Departure from dailyflightschedule_merged_parquet
  where OperationUnit='{OpName}' and date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s') >=date('{min_date_str}') and 
date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s') <=date('{max_date_str}')  and STA !='' and STD!='' and on_block_time!='' and off_block_time!='' and Sensor_ATA !='None' and Sensor_ATD !='None' AND on_block_time != 'None'
            AND off_block_time != 'None')
    as t1
    left join
    (select Hexcode,BodyType from FlightMaster_parquet) as t2
    on t1.Aircraft = t2.Hexcode group by t1.Airline,t2.BodyType
    """
    print(flights_faster_than_expected_query)
    response = athena.start_query_execution(QueryString=flights_faster_than_expected_query, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))
    
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']
    
    while status=='RUNNING' or status=="QUEUED":
        print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
    
    # query_execution_id = '0b191db7-2bfd-4a9c-a977-ee39690711b7'
    s3 = boto3.client('s3')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/'+query_execution_id+'.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
    df4_parent = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    #df4_parent.FTE=df4_parent.FTE.astype(int)
    df4_parent.FTE=df4_parent.FTE.round(1)
    df4_parent.BodyType = df4_parent.BodyType.replace({'Turboprop':'Bombardier'})
    df4_parent.BodyType = df4_parent.BodyType.replace({'turboprop':'Bombardier'})
    df4_parent.BodyType = df4_parent.BodyType.replace({'narrow':'Narrow'})
    df4_parent.BodyType = df4_parent.BodyType.replace({'wide':'Wide'})
    
    df4_parent_final= df4_parent.groupby(['BodyType']).agg('mean').reset_index() if Airline=="" else df4_parent.groupby(['Airline','BodyType']).agg('mean').reset_index()
    print(df4_parent_final)
    
    dd1_json = df4_parent_final.to_json(orient='split')

    return {
       'statusCode': 200,
        'PercentFlights_Faster_Than_Expected_Airport': json.loads(dd1_json),
    }