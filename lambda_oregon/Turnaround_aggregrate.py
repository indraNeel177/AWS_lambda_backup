import pandas as pd
import pymysql
import json
import configparser
import boto3
import io

session = boto3.Session(
      aws_access_key_id='AKIAUY6OUJHZIXYIII6Z',  #put your acces id\n",
     aws_secret_access_key='9+lO5pcKd4qI7+zKFTmY77gUFPijPSeExThtD2Fr' #put your secret key\n",
    )
def lambda_handler(event, context):
    #events from post call
    TypeofUser=event['TypeofUser']
    OpName  = int(event['OperationUnit'])# OperationUnit = 4
    Airline = event['Airline']
    yesterday= event['yesterday']
    metro_names = ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA')
    
    
    
    #OpName = 4
    #Airline = ""
    #yesterday=""

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""

    #{yesterday_condition}
    s3 = session.resource('s3')
    obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod_read_replica.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)
    
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    if OpName == 4 or OpName == 13 :
        DFS_craft = 'Aircraft'
        FM_table = 'Hexcode'
    else :
        DFS_craft = 'AircraftRegistration_Arrival'
        FM_table = 'RegNo'
    #equipments = event['equipments']
    #equipments=""
    #equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    #equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"

    Condition1 = '' if Airline =="" else f"and Airline=\'{Airline}\'"
    # airline
    if OpName == 4:
        connected_bay = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
    else :
        connected_bay = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22')

    
    
    df1= pd.read_sql(f"""
    select Airline,FlightNumber_Departure,FlightType,if(ToAirport_IATACode in {metro_names} , 'Metro', 'Non-Metro') as destination_type,ToAirport_IATACode,
    if(Bay in {connected_bay},'Connected','Remote') as BayType,Bay,On_Block_Time,
    BodyType,timestampdiff(MINUTE,On_Block_Time,Off_Block_TimeR) as turnaround_time
    from (select * from (select *,if(Off_Block_Time is NULL,DATE_SUB(ATD, INTERVAL '5:0' MINUTE_SECOND),Off_Block_Time) as Off_Block_TimeR from DailyFlightSchedule_Merged
    where OperationUnit={OpName}
    and (date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    or
    date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}{airline_filter_condition}
    )) as t1
    left join
    (select Hexcode,BodyType from FlightMaster) as t2
    on t1.Aircraft = t2.Hexcode) as t3
    where (date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} or
    date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}) {Condition1}
    """
    ,con=conn) if OpName==4 or OpName== 13 else pd.read_sql(f"""
    select Airline,FlightNumber_Departure,FlightType,AllocatedBay,TerminalArrival,AircraftRegistration_Arrival,
    if(ToAirport_IATACode in  {metro_names} , 'Metro', 'Non-Metro') as destination_type,On_Block_Time,
    ToAirport_IATACode,
    if(TerminalArrival in ('T3'),'Connected','Remote') as BayType,
    Bay,
    BodyType,timestampdiff(MINUTE,On_Block_Time,Off_Block_TimeR) as turnaround_time
    from (select * from (select *,if(Off_Block_Time is NULL,DATE_SUB(ATD, INTERVAL '5:0' MINUTE_SECOND),Off_Block_Time) as Off_Block_TimeR from DailyFlightSchedule_Merged
    where OperationUnit=22
    and (date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    or
    date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}{airline_filter_condition}
    )) as t1
    left join
    (select Hexcode,BodyType,REPLACE(RegNo, '-', '') as RegNo from FlightMaster) as t2
    on t1.AircraftRegistration_Arrival = t2.RegNo) as t3
    where (date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} or
    date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition})  {Condition1}
    """,con=conn)    

    df1.iloc[:,0:6] = df1.iloc[:,0:6].fillna('unknown')
    df1 = df1[pd.notnull(df1['turnaround_time'])]
    if OpName == 13:
        df1['FlightType'] = 'Domestic'
        
    if yesterday=='True':
        df_yesetrday_flight = df1[(df1.On_Block_Time.dt.date == (pd.datetime.now() - pd.Timedelta(days=1)).date())]
        df_day_before_Yesterday_lessthan_240= df1[(df1.On_Block_Time.dt.date != (pd.datetime.now() - pd.Timedelta(days=1)).date()) & (df1['turnaround_time']<=240) ].reset_index(drop=True)
        df_yesterday_without_base_flights =df_yesetrday_flight.append(df_day_before_Yesterday_lessthan_240).drop('On_Block_Time',axis=1).reset_index(drop=True)
        df1=df_yesterday_without_base_flights.copy()
    else:
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
    df_1_2_3 = df1_2.to_json(orient='split')

    #airline level bay and turnaround
    df1.BodyType = df1.BodyType.fillna('Unknown')
    df2_dumm = df1.groupby('BodyType')['FlightNumber_Departure','turnaround_time'].agg({'FlightNumber_Departure':'count','turnaround_time':'mean'}).rename(columns={'FlightNumber_Departure':'Count_of_flights'}).reset_index()
    df2_dumm.BodyType = df2_dumm.BodyType.replace({'Turboprop':'Bombardier'})
    temp_count_flights =df2_dumm.Count_of_flights.sum()
    temp_turnaround_mean= round(pd.to_numeric(df2_dumm.turnaround_time.mean(), errors='coerce'))
    d={'BodyType':'All_flights_mean','turnaround_time':temp_turnaround_mean,'Count_of_flights':temp_count_flights}
    all_mean_1 = pd.DataFrame(data=d,index=[0])
    df2_merge= pd.DataFrame.append(df2_dumm,all_mean_1)
    df2_merge_2 = df2_merge.groupby('BodyType').agg({'Count_of_flights':'sum','turnaround_time':'mean'}).reset_index().round(2)
    df_agg_final_json=df2_merge_2.to_json(orient='split')


    #flight level bay and turnaround
    df_flight_group = df1.groupby(['Airline', 'FlightNumber_Departure', 'FlightType', 'destination_type','BayType', 'BodyType','Bay','ToAirport_IATACode']).mean().reset_index()

    df_flight_group_2 = df_flight_group[['Airline', 'FlightNumber_Departure', 'FlightType', 'destination_type',
           'BayType', 'BodyType', 'Bay', 'turnaround_time', 'ToAirport_IATACode']]
    #Bay = df_flight_group['Bay']
    #df_flight_group.drop(labels=['Bay'], axis=1,inplace = True)
    #df_flight_group.insert(7, 'Bay', Bay)

    df_flight_final_json = df_flight_group_2.to_json(orient='split')



    # faster than expected 
        
    
    
    df4_parent = pd.read_sql(f"""
     select t1.{DFS_craft},t1.FTE,t2.BodyType from (
     select Aircraft,AircraftRegistration_Arrival,if(timestampdiff(MINUTE,On_Block_Time,Off_Block_Time)<=timestampdiff(MINUTE,STA,STD),1,0) as FTE 
     from DailyFlightSchedule_Merged
     where OperationUnit={OpName} and date(On_Block_Time) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition} {Condition1}
     )
     as t1
     left join
     (select Hexcode,BodyType,REPLACE(RegNo, '-', '') as RegNo from FlightMaster) as t2
     on t1.{DFS_craft} = t2.{FM_table}
     """, con=conn)
    
    #AirlineName = 'SpiceJet' #event['AirlineName']
    df4_dumm = df4_parent.copy() #if Airline =='' else df4_parent[df4_parent['Airline']==Airline]
    #df4_dumm['BodyType']=df4_dumm['FlightNumber_Arrival'].str.len()
    #df4_dumm['BodyType'] = df4_dumm['BodyType'].apply(lambda x: 'Narrow' if x==6 else 'TurboProp')
    df4_dumm.BodyType = df4_dumm.BodyType.replace({'Turboprop':'Bombardier'})
    df5_dumm = df4_dumm.groupby('BodyType')['FTE'].agg('mean').round(2).reset_index()
    data_mean = pd.DataFrame({'BodyType':['All'], 'FTE':round(df4_dumm.FTE.mean(),2)} )
    dd1=pd.DataFrame.append(df5_dumm,data_mean).reset_index(drop=True)
    dd1.FTE = dd1.FTE*100
    dd1_json = dd1.to_json(orient='split')


    #final responses
    return {
       'statusCode': 200,
       'OpName':OpName,
        'Turnaround_Level_1_2_3': json.loads(df_1_2_3),
        'Agg_Level_1':json.loads(df_agg_final_json),
        'Agg_Level_2': json.loads(df_flight_final_json),
        'PercentFlights_Faster_Than_Expected_Airport': json.loads(dd1_json)
    }
