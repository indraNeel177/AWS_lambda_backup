import pandas as pd
import pymysql
import json
import configparser
import boto3
import io
import time
import mysql.connector
from sqlalchemy import create_engine

from datetime import datetime,date, timedelta
def lambda_handler(event, context):
    
    athena = boto3.client('athena')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    
    archived_date_list = ['2020-02-05', '2020-02-06', '2020-02-07', '2020-02-08', '2020-02-09', '2020-02-10', '2020-02-11', '2020-02-12', '2020-02-13', '2020-02-14', '2020-02-15', '2020-02-16', '2020-02-17', '2020-02-18', '2020-02-19', '2020-02-20', '2020-02-21', '2020-02-22', '2020-02-23', '2020-02-24', '2020-02-25', '2020-02-26', '2020-02-27', '2020-02-28', '2020-02-29', '2020-03-01']

    
    k=0
    while k < len(archived_date_list):
        
        i=0
        OpName_list = [13]
        
        while i < len(OpName_list):
            metro_names = ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA')
            #connected_bay_names_hyd = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
            OpName =OpName_list[i]
            archived_date = archived_date_list[k]
            print(OpName)
            print('started')
            print(archived_date)
            Airline = ""
            yesterday=""
            flights=""
            equipments=""
            yesterday_condition = "- INTERVAL 2 DAY" if yesterday=="True" else ""

            #{yesterday_condition}
            s3 = boto3.resource('s3')
            obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod.ini')
            config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
            configParser = configparser.ConfigParser()
            configParser.readfp(config_file_buffer)
            hostname = configParser.get('db-creds-prod', 'hostname')
            jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
            username = configParser.get('db-creds-prod', 'username')
            password = configParser.get('db-creds-prod', 'password')
            dbname = configParser.get('db-creds-prod', 'dbname')
            conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)

            #flights = event['flights']
            airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"

            #equipments = event['equipments']
            #equipments=""
            #equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
            #equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"

            Condition1 = '' if Airline =="" else f"and Airline=\'{Airline}\'"
            # airline


            if OpName==4 :
                connected_bay = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
            else:
                connected_bay = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22')

                
                
            if OpName == 4 or OpName == 13 :
                turnaround_level_2_query=f"""
    select Airline,FlightNumber_Departure,AllocatedBay,ToAirport_IATACode,AircraftRegistration_Arrival,FlightType,destination_type,BayType,Bay,On_Block_Time,BodyType,turnaround_time from (select t1.*,t2.BodyType as BodyType from (select Airline,FlightType,
 if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type,ToAirport_IATACode,
 if(Bay IN {connected_bay},'Connected','Remote') AS BayType,
aircraft,   
(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')) as On_Block_Time,
-- AS Off_Block_TimeR,
(DATE_DIFF('minute',date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'),
if(date_parse(off_block_time,'%Y-%m-%d %H:%i:%s') is NULL, 
DATE_ADD('minute', -5, date_parse(ATD, '%Y-%m-%d %H:%i:%s')), 
date_parse(off_block_time, '%Y-%m-%d %H:%i:%s')))) AS turnaround_time,Bay,FlightNumber_Departure,AllocatedBay,AircraftRegistration_Arrival
 FROM avileap_prod.dailyflightschedule_merged_parquet
 WHERE OperationUnit='{OpName}'
            AND on_block_time != ''
            AND off_block_time != ''
            AND Sensor_ATA !=''
            AND Sensor_ATD !='' and
            date(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'))=date('{archived_date}')
 ) as t1
left join (SELECT Hexcode,BodyType FROM avileap_prod.flightmaster_parquet) as t2 on t1.aircraft=t2.Hexcode )as temp1  group by ToAirport_IATACode,Airline,FlightNumber_Departure,AllocatedBay,AircraftRegistration_Arrival,FlightType,destination_type,BayType,Bay,On_Block_Time,Bodytype,turnaround_time

    """ 
            else :
                turnaround_level_2_query=f"""
    select Airline,FlightNumber_Departure,AllocatedBay,ToAirport_IATACode,AircraftRegistration_Arrival,FlightType,destination_type,BayType,Bay,On_Block_Time,BodyType,turnaround_time from (select t1.*,t2.BodyType as BodyType from (select Airline,FlightType,
 if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM','CCU','MAA','PNQ'), 'Metro', 'Non-Metro') AS destination_type,ToAirport_IATACode,
 if(TerminalArrival in ('T3'),'Connected','Remote') as BayType,
aircraft,   
(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')) as On_Block_Time,
-- AS Off_Block_TimeR,
(DATE_DIFF('minute',date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'),
if(date_parse(off_block_time,'%Y-%m-%d %H:%i:%s') is NULL, 
DATE_ADD('minute', -5, date_parse(ATD, '%Y-%m-%d %H:%i:%s')), 
date_parse(off_block_time, '%Y-%m-%d %H:%i:%s')))) AS turnaround_time,Bay,FlightNumber_Departure,AllocatedBay,AircraftRegistration_Arrival
 FROM avileap_prod.dailyflightschedule_merged_parquet
 WHERE OperationUnit='{OpName}'
            AND on_block_time != ''
            AND off_block_time != ''
            AND Sensor_ATA !=''
            AND Sensor_ATD !='' and
            date(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s'))=date('{archived_date}')
 ) as t1
left join (SELECT Hexcode,BodyType FROM avileap_prod.flightmaster_parquet) as t2 on t1.aircraft=t2.Hexcode )as temp1  group by ToAirport_IATACode,Airline,FlightNumber_Departure,AllocatedBay,AircraftRegistration_Arrival,FlightType,destination_type,BayType,Bay,On_Block_Time,Bodytype,turnaround_time

    """    
     

            print(turnaround_level_2_query)
            response = athena.start_query_execution(QueryString=turnaround_level_2_query, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
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
            df1 = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)





            df1.iloc[:,0:6] = df1.iloc[:,0:6].fillna('unknown')
            df1 = df1[pd.notnull(df1['turnaround_time'])]
            df1['turnaround_time'] = df1['turnaround_time'].astype(float)
            df1['turnaround_time'] = df1['turnaround_time'].round(2)
            
            
            df1['On_Block_Time'] = pd.to_datetime(df1['On_Block_Time'])
            
            df_today_flight = df1[(df1.On_Block_Time.dt.date == pd.datetime.now().date()) ].reset_index(drop=True)
            df_Yesterday_lessthan_240= df1[(df1.On_Block_Time.dt.date != pd.datetime.now().date()) & (df1['turnaround_time']<=240) ].reset_index(drop=True)
            df_today_without_base_flights = df_today_flight.append(df_Yesterday_lessthan_240).drop('On_Block_Time',axis=1).reset_index(drop=True)
            df1= df_today_without_base_flights.copy()

            #df1 = df1.copy() if Airline ==''else df1[df1['Airline']==Airline]

            #Bay = df1['Bay']
            #df1.drop(labels=['Bay'], axis=1,inplace = True)
            #df1.insert(7, 'Bay', Bay)

            df1_2 = df1[['Airline', 'FlightNumber_Departure', 'FlightType', 'destination_type',
                'BayType', 'Bay', 'BodyType', 'turnaround_time','ToAirport_IATACode']]

            df_agg_1_card1= df1_2.copy()
            df_agg_1_card1['Operationunit']= f'{OpName}'
            df_agg_1_card1['archived_date']= f'{archived_date}'

            #df_1_2_3 = df1_2.to_json(orient='split')

            #airline level bay and turnaround
            if OpName == 13 or  OpName == 4:
                df1['FlightType'] = 'Domestic'
            df1.BodyType = df1.BodyType.fillna('Unknown')
            df2_dumm = df1.groupby('BodyType')['FlightNumber_Departure','turnaround_time'].agg({'FlightNumber_Departure':'count','turnaround_time':'mean'}).rename(columns={'FlightNumber_Departure':'Count_of_flights'}).reset_index()
            df2_dumm.BodyType = df2_dumm.BodyType.replace({'Turboprop':'Bombardier'})
            temp_count_flights =df2_dumm.Count_of_flights.sum()
            temp_turnaround_mean= round(pd.to_numeric(df2_dumm.turnaround_time.mean(), errors='coerce'))
            d={'BodyType':'All_flights_mean','turnaround_time':temp_turnaround_mean,'Count_of_flights':temp_count_flights}
            all_mean_1 = pd.DataFrame(data=d,index=[0])
            df2_merge= pd.DataFrame.append(df2_dumm,all_mean_1)
            df2_merge_2 = df2_merge.groupby('BodyType').agg({'Count_of_flights':'sum','turnaround_time':'mean'}).reset_index().round(2)


            df2_merge_2['Operationunit']= f'{OpName}'
            df2_merge_2['archived_date']= f'{archived_date}'



            FlightNumber_Departure_in_agg = df1.groupby(['BodyType','FlightNumber_Departure'])['turnaround_time'].agg({'FlightNumber_Departure':'count','turnaround_time':'mean'}).rename(columns={'FlightNumber_Departure':'Count_of_flights'}).reset_index()
            FlightNumber_Departure_in_agg['Operationunit']= f'{OpName}'
            FlightNumber_Departure_in_agg['archived_date']= f'{archived_date}'

            #df_agg_final_json=df2_merge_2.to_json(orient='split')


            #flight level bay and turnaround
            df_flight_group = df1.groupby(['Airline', 'FlightNumber_Departure', 'FlightType', 'destination_type','BayType', 'BodyType','Bay','ToAirport_IATACode']).mean().reset_index()

            df_flight_group_2 = df_flight_group[['Airline', 'FlightNumber_Departure', 'FlightType', 'destination_type',
                   'BayType', 'BodyType', 'Bay', 'turnaround_time', 'ToAirport_IATACode']]

            df_flight_group_2['Operationunit']= f'{OpName}'
            df_flight_group_2['archived_date']= f'{archived_date}'
            
            
            import mysql.connector
            from sqlalchemy import create_engine
            
            hostname_write='aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
            db_write='Reporting'
            username='dheeraj_tbb'
            password='DSHW6wNx7e46uSNK'
            jdbcPort =3306
            engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(username, password, hostname_write, jdbcPort, db_write), echo=False)
            #mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8

            def df_to_mysql(df, conn, tbl_name):
                df.to_sql(tbl_name, conn, if_exists='append',index=False)

            print("hhhhhhhhhhhhhhhhhhhhhhhhhhhh")
            # print(df_flight_group_2)

            df_to_mysql(df_agg_1_card1,engine,'Turnaround_first_level_1')

            df_to_mysql(FlightNumber_Departure_in_agg,engine,'Turnaround_first_level_2')

            df_to_mysql(df2_merge_2,engine,'Turnaround_first_level_2_1')

            df_to_mysql(df_flight_group_2,engine,'Turnaround_first_level_3')

            print(OpName_list[i])
            #print(today_sample[i])

            i+=1
        k+=1