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
    Airlines=None
    max_date=(datetime.now()-timedelta(1)).date()
    #min_date=(datetime.now()-timedelta(31)).date()
    
    # YYYY-MM-DD format
    minDate_value= event['min_date']
    min_date= pd.to_datetime(minDate_value).date()
    
    
    OpName =event['OperationUnit']
    if OpName=='4' :
        connected_bay = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
    else:
        connected_bay = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22')

    
    # OpName=4
    #min_date=(datetime.now()-timedelta(31)).date()
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")+" 00:00:00"
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d %H:%M:%S').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")+" 23:59:59"
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S').date()
        if 'Airline' in event.keys() and event.get("Airline")!=None and event.get("Airline")!="":
            Airlines=event.get("Airline")
    if OpName == '4'or OpName == '13':
        turnaround_level_1_query=f"""
    select Airline,FlightType,destination_type,BayType,bodytype,date1,avg(turnaround_time) as turnaround_time from (
 select t1.*,t2.Bodytype from (select Airline,FlightType,
 if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type,
 if(Bay IN {connected_bay},'Connected','Remote') AS BayType,
aircraft,   
date(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')) as date1,
-- AS Off_Block_TimeR,
(DATE_DIFF('minute',date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'),
if(date_parse(off_block_time,'%Y-%m-%d %H:%i:%s') is NULL, 
DATE_ADD('minute', -5, date_parse(ATD, '%Y-%m-%d %H:%i:%s')), 
date_parse(off_block_time, '%Y-%m-%d %H:%i:%s')))) AS turnaround_time
 FROM avileap_prod.dailyflightschedule_merged_parquet
 WHERE OperationUnit='{OpName}'
            AND on_block_time != ''
            AND off_block_time != ''
            AND Sensor_ATA !=''
            AND Sensor_ATD !='' and Sensor_ATA !='None' and Sensor_ATD !='None' AND on_block_time != 'None'
            AND off_block_time != 'None'and
            date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')>date('{min_date}') and date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')<=date('{max_date}')
 ) as t1
left join (sELECT Hexcode,BodyType FROM avileap_prod.flightmaster_parquet) as t2 on t1.aircraft=t2.Hexcode
)as temp1 group by Airline,FlightType,destination_type,BayType,bodytype,date1

    """ 
    else :
        turnaround_level_1_query=f"""
    select Airline,FlightType,destination_type,BayType,bodytype,date1,avg(turnaround_time) as turnaround_time from (
 select t1.*,t2.Bodytype from (select Airline,FlightType,
 if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type,
 if(TerminalArrival in ('T3'),'Connected','Remote') as BayType,
aircraft,AircraftRegistration_Arrival,   
date(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')) as date1,
-- AS Off_Block_TimeR,
(DATE_DIFF('minute',date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'),
if(date_parse(off_block_time,'%Y-%m-%d %H:%i:%s') is NULL, 
DATE_ADD('minute', -5, date_parse(ATD, '%Y-%m-%d %H:%i:%s')), 
date_parse(off_block_time, '%Y-%m-%d %H:%i:%s')))) AS turnaround_time
 FROM avileap_prod.dailyflightschedule_merged_parquet
 WHERE OperationUnit='{OpName}'
            AND on_block_time != ''
            AND off_block_time != ''
            AND ATA !=''
            AND ATD !=''and ATA !='None' and ATD !='None' AND on_block_time != 'None'
            AND off_block_time != 'None' and
            date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')>date('{min_date}') and date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')<=date('{max_date}')
 ) as t1
left join (sELECT Hexcode,BodyType,REPLACE(RegNo, '-', '') as RegNo FROM avileap_prod.flightmaster_parquet) as t2 on t1.AircraftRegistration_Arrival=t2.RegNo
)as temp1 group by Airline,FlightType,destination_type,BayType,bodytype,date1

    """
        
    print(turnaround_level_1_query)
    response = athena.start_query_execution(QueryString=turnaround_level_1_query, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
    turnaround_level_1_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    #turnaround_level_1_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    #bodytype_wise_turnaround_data=turnaround_level_1_raw_data.groupby("BodyType").agg({"turnaround_time":"mean"}).reset_index()
    #bodytype_wise_turnaround_data.loc[4]=["All", bodytype_wise_turnaround_data.turnaround_time.mean()]
    if OpName == '4' or OpName == '13':
            turnaround_level_1_raw_data['FlightType']='Domestic'
    turnaround_level_1_raw_data.turnaround_time.dropna()
    turnaround_level_1_raw_data = turnaround_level_1_raw_data.fillna("Unknown")
    turnaround_level_1_raw_data.turnaround_time=round(turnaround_level_1_raw_data.turnaround_time)
    if Airlines is not None and len(Airlines)>0:
        turnaround_level_1_raw_data=turnaround_level_1_raw_data[turnaround_level_1_raw_data.Airline.isin(Airlines)]
    
    turnaround_level_1_raw_data_json=turnaround_level_1_raw_data.to_json(orient='split')
    
    #bodytype_wise_turnaround_data_json=turnaround_level_1_raw_data.to_json(orient='split')
    
    
    
    return{
        "response_code":200,
        "level_1_all_airlines_turnaround":json.loads(turnaround_level_1_raw_data_json)
       # "level_1_bodytype_wise_turnaround":json.loads(bodytype_wise_turnaround_data_json)
    }
        
        