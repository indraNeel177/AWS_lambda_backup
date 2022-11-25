import pandas as pd
import boto3
import datetime
import time
import io
import s3fs

now=str(datetime.datetime.now().date())

def lambda_handler(event, context):
    try:
        athena = boto3.client('athena')
        s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
        otp_staging_raw_data_query="""SELECT *,
                 month(date_parse(COALESCE(ATA,Sensor_ATA),
                 '%Y-%m-%d %H:%i:%s')) AS ata_month, month(date_parse(COALESCE(ATD,Sensor_ATD), '%Y-%m-%d %H:%i:%s')) AS atd_month, week(date_parse(COALESCE(ATA,Sensor_ATA), '%Y-%m-%d %H:%i:%s')) AS ata_week, week(date_parse(COALESCE(ATD,Sensor_ATD), '%Y-%m-%d %H:%i:%s')) AS atd_week, day(date_parse(COALESCE(ATA,Sensor_ATA), '%Y-%m-%d %H:%i:%s')) AS ata_day, day(date_parse(COALESCE(ATD,Sensor_ATD), '%Y-%m-%d %H:%i:%s')) AS atd_day, hour(date_parse(COALESCE(ATA,Sensor_ATA), '%Y-%m-%d %H:%i:%s')) AS ata_hour, hour(date_parse(COALESCE(ATD,Sensor_ATD), '%Y-%m-%d %H:%i:%s')) AS atd_hour
        FROM 
            (SELECT Airline,
                 flightnumber_arrival,
                 flightnumber_departure,
                 ATA,
                 ATD,
                 Sensor_ATA,Sensor_ATD,
                 fromairport_iatacode as from_city,
                 toairport_iatacode as to_city,
                 case when TerminalArrival in ('1C','1D') then 'T1' else TerminalArrival end as TerminalArrival,OperationUnit,
                 if(COALESCE(ATA,Sensor_ATA) >STA,
                 0,
                 1) AS OTP_A0,
                 if(COALESCE(ATD,Sensor_ATD) >STD,
                 0,
                 1) AS OTP_D0,
                 if(date_parse(COALESCE(ATA,Sensor_ATA),
                 '%Y-%m-%d %H:%i:%s') >DATE_ADD('minute',15, date_parse(STA, '%Y-%m-%d %H:%i:%s')),0,1) AS OTP_A15, if(date_parse(COALESCE(ATA,Sensor_ATA), '%Y-%m-%d %H:%i:%s') >DATE_ADD('minute', 15, date_parse(STD, '%Y-%m-%d %H:%i:%s')),0,1) AS OTP_D15, if(FromAirport_IATACode IN ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') AS origin, if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') AS destination, if(length(FlightNumber_Arrival)=6,'Narrow','TurboProp') AS bodytype
            FROM dailyflightschedule_merged_parquet 
            where COALESCE(ATD,Sensor_ATD) != '' and COALESCE(ATA,Sensor_ATA)!='' and STA != '' and COALESCE(ATD,Sensor_ATD) != 'None' and COALESCE(ATA,Sensor_ATA)!='None' and STA != 'None' )"""
            
            
        response = athena.start_query_execution(QueryString=otp_staging_raw_data_query, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
        
        otp_staging_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
        
        final_output_path='s3://athena-dumps/lambda-staging/OTP/{}/otp_staging_raw_data.csv'.format(now)
        otp_staging_raw_data.to_csv(final_output_path, index=False)
        
    
    except Exception as e:
        raise e
    finally:
        otp_staging_raw_data=None
        response=None
        s3=None
        key=None
        keys=None
        athena=None
        
    
