import pandas as pd
import pymysql
import json
import configparser
import boto3
import io
import mysql.connector
from sqlalchemy import create_engine

from datetime import datetime,date, timedelta
def lambda_handler(event, context):
    
    archived_date = (pd.datetime.now() - pd.Timedelta(days=2)).date().strftime('%Y-%m-%d')
    OpName_list = [4,22,13]
    i=0
    while i < len(OpName_list):
        metro_names = ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA')
        #connected_bay_names_hyd = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
        OpName =OpName_list[i]
        print(OpName)
        print('started')
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
        if OpName == 4 or OpName == 13 :
            DFS_craft = 'Aircraft'
            FM_table = 'Hexcode'
        else :
            DFS_craft = 'AircraftRegistration_Arrival'
            FM_table = 'RegNo'
        if (OpName == 13 or OpName == 4) :
            df1= pd.read_sql(f"""
     select Airline,FlightNumber_Departure,AllocatedBay,AircraftRegistration_Arrival,FlightType,if(ToAirport_IATACode in ('HYD', 'DEL', 'BLR', 'BOM', 'PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as destination_type,ToAirport_IATACode,
     BayType,Bay,On_Block_Time,
     BodyType,timestampdiff(MINUTE,On_Block_Time,Off_Block_TimeR) as turnaround_time
     from (select * from (select ALS.BAYTYPE as BayType ,dt.*,if(dt.Off_Block_Time is NULL,DATE_SUB(dt.ATD, INTERVAL '5:0' MINUTE_SECOND),dt.Off_Block_Time) as Off_Block_TimeR from DailyFlightSchedule_Merged
     dt LEFT JOIN AirportLocations ALS ON dt.Bay and dt.OperationUnit = ALS.Name    where ALS.BayType is not NuLL and dt.OperationUnit={OpName}
     and (date(dt.On_Block_Time)=date('{archived_date}') 
     or
     date(dt.Off_Block_Time)=date('{archived_date}'){airline_filter_condition}
     )) as t1
     left join
     (select Hexcode,BodyType,REPLACE(RegNo, '-', '') as RegNo from FlightMaster) as t2
     on t1.{DFS_craft} = t2.{FM_table}) as t3
     where (date(On_Block_Time)=date('{archived_date}'){yesterday_condition} or
     date(Off_Block_Time)=date('{archived_date}')) 
     """,con=conn)
        else :
            df1= pd.read_sql(f"""
     select Airline,FlightNumber_Departure,AllocatedBay,AircraftRegistration_Arrival,FlightType,if(ToAirport_IATACode in ('HYD', 'DEL', 'BLR', 'BOM', 'PNQ', 'CCU', 'MAA'
    ) , 'Metro', 'Non-Metro') as destination_type,ToAirport_IATACode,
     BayType,Bay,On_Block_Time,
     BodyType,timestampdiff(MINUTE,On_Block_Time,Off_Block_TimeR) as turnaround_time
     from (select * from (select ALS.BAYTYPE as BayType ,dt.*,if(dt.Off_Block_Time is NULL,DATE_SUB(dt.ATD, INTERVAL '5:0' MINUTE_SECOND),dt.Off_Block_Time) as Off_Block_TimeR from DailyFlightSchedule_Merged
     dt LEFT JOIN AirportLocations ALS ON dt.Bay = ALS.Name    where ALS.BayType is not NuLL and dt.OperationUnit={OpName}
     and (date(dt.On_Block_Time)=date('{archived_date}')
     or
     date(dt.Off_Block_Time)=date('{archived_date}')
     )) as t1
     left join
     (select Hexcode,BodyType,REPLACE(RegNo, '-', '') as RegNo from FlightMaster) as t2
     on t1.{DFS_craft} = t2.{FM_table}) as t3
     where (date(On_Block_Time)=date('{archived_date}') or
     date(Off_Block_Time)=date('{archived_date}') )
     """,con=conn)
     
        
        
        

        if OpName == 13 or OpName == 4 :
            df1['FlightType'] = 'Domestic'
        df1.iloc[:,0:6] = df1.iloc[:,0:6].fillna('unknown')
        df1 = df1[pd.notnull(df1['turnaround_time'])]

       
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

        hostname_write='aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
        db_write='Reporting'
        username='dheeraj_tbb'
        password='DSHW6wNx7e46uSNK'
        jdbcPort =3306
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(username, password, hostname_write, jdbcPort, db_write), echo=False)
        #mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8

        def df_to_mysql(df, conn, tbl_name):
            df.to_sql(tbl_name, conn, if_exists='append',index=False)

        print(df_flight_group_2.archived_date)
        # print(df_flight_group_2)

        df_to_mysql(df_agg_1_card1,engine,'Turnaround_first_level_1')

        df_to_mysql(FlightNumber_Departure_in_agg,engine,'Turnaround_first_level_2')

        df_to_mysql(df2_merge_2,engine,'Turnaround_first_level_2_1')

        df_to_mysql(df_flight_group_2,engine,'Turnaround_first_level_3')

        print(OpName_list[i])
        #print(today_sample[i])

        i+=1