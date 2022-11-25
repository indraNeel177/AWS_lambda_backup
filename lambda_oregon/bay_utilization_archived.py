import json
import pandas as pd
import numpy as np
import pymysql
import configparser
import boto3
import io
import time
from datetime import datetime, timedelta

def lambda_handler(event, context):
    print(pd.__version__)
    min_date_str=None
    max_date_str=None
    ref_date_str=None
    iu_min_date=None
    iu_max_date=None
    operation_unit=None
    AirlineName=None
    TypeofUser=None
    #operation_unit= '4'
    #TypeofUser='Airport'
    flights=None
    if event!=None and event!="":
        if ('ref_min_date' in event.keys()) and (event.get("ref_min_date")!=None) and (event.get("ref_min_date")!=""):
            min_date_str=event.get("ref_min_date")
        if 'ref_max_date' in event.keys() and event.get("ref_max_date")!=None and event.get("ref_max_date")!="":
            max_date_str=event.get("ref_max_date")
        if 'Airline' in event.keys() and event.get("Airline")!=None and event.get("Airline")!="":    
            AirlineName = event['Airline']
        if 'OperationUnit' in event.keys() and event.get("OperationUnit")!=None and event.get("OperationUnit")!="":    
            operation_unit= event['OperationUnit']
        if 'TypeofUser' in event.keys() and event.get("TypeofUser")!=None and event.get("TypeofUser")!="":    
            TypeofUser= event['TypeofUser']
        if 'flights' in event.keys() and event.get("flights")!=None and event.get("flights")!="":             
            flights = event['flights']
            
    # airline_filter_condition= "" if flights=="" else f" and regexp_like(FlightNumber_Arrival,'{flights}')"
    min_date=datetime.strptime(min_date_str,'%Y-%m-%d')
    max_date=datetime.strptime(max_date_str+ ' 23:59:59','%Y-%m-%d %H:%M:%S')
  
    
    days_diff=(max_date-min_date).days
    
    ts_range=pd.date_range(min_date, max_date).tolist()
    date_range=[ts.strftime('%Y%m%d') for ts in ts_range ]
    iu_min_date=(min_date+timedelta(1))#.strftime('%Y-%m-%d')
    iu_max_date=(max_date-timedelta(1))#.strftime('%Y-%m-%d %H:%M:%S')
    
    #airline condition
    Condition1 = '' if AirlineName =="" else f"and Airline=\'{AirlineName}\'"
    athena = boto3.client('athena',region_name='us-west-2')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    
    print(flights)
    airline_filter_condition= "" if flights=="" or flights==None else f" and regexp_like(FlightNumber_Arrival,'{flights}')"
    
    print("checking for the conditions")
    print(airline_filter_condition)
    
    print(iu_max_date, iu_min_date, min_date, max_date, date_range)
    
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod_read_replica.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    dev_hostname = "avileap-test.ckfsniqh1gly.us-west-2.rds.amazonaws.com"
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    dev_connection = pymysql.connect(host=dev_hostname, port=int(jdbcPort), user='avileap_admin', passwd='admin_1q2w#E$R', db='AviLeap', connect_timeout=5)
    connection = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)
    
    query_for_arrival_master="SELECT * FROM `LoggedArrivalMasterGMR` where substr(STOA,1,8) in ()"
    for date_ in date_range:
        query_for_arrival_master=query_for_arrival_master[:-1]+"'" +date_+"',"+query_for_arrival_master[-1]
    if query_for_arrival_master[-2]==',':
        query_for_arrival_master=query_for_arrival_master[:-2]+query_for_arrival_master[-1]
    
    query_for_departure_master="SELECT * FROM `LoggedDepartureMasterGMR` where substr(STOD,1,8) in ()"
    for date_ in date_range:
        query_for_departure_master=query_for_departure_master[:-1]+"'" +date_+"',"+query_for_departure_master[-1]
    if query_for_departure_master[-2]==',':
        query_for_departure_master=query_for_departure_master[:-2]+query_for_departure_master[-1]
        
    query_for_city="Select City, IATACode from Airports"
    
    
    flight_master=pd.read_sql("select * from FlightMaster", con=connection)
    flight_master['AircraftRegistration_Arrival'] = flight_master.RegNo.str.split('-').str.join("")
    arrival_master=pd.read_sql(query_for_arrival_master, con=dev_connection)
    departure_master=pd.read_sql(query_for_departure_master, con=dev_connection)
    city_airport_master=pd.read_sql(query_for_city, con=connection)
    
    scheduled_aggregates={}
    ####################################################################################
    ##                                                                                ##
    ##                         Scheduled Bay Utilization                              ##
    ##                                                                                ##
    ####################################################################################
    overlap_percent_utilization_for_remote=None
    overlap_percent_utilization_for_connected=None

    query_for_dfs_merged=f"""SELECT cast(logid as integer) AS LogId,
                                    cast(STA AS timestamp) AS STA,
                                    cast(STD AS timestamp) AS STD,
                                    cast(ATD AS timestamp) AS ATD,
                                    cast(ATA AS timestamp) AS ATA,
                                    cast(ETD AS timestamp) AS ETD,
                                    cast(ETA AS timestamp) AS ETA,
                                    cast(On_Block_Time AS timestamp) AS On_Block_Time,
                                    cast(off_block_time AS timestamp) AS Off_Block_Time,
                                    cast(Sensor_ATD AS timestamp) AS Sensor_ATD,
                                    cast(Sensor_ATA AS timestamp) AS Sensor_ATA,
                                    airline AS Airline,
                                    aircraft AS Aircraft,
                                    r_key AS R_Key,
                                    allocatedbay AS AllocatedBay,
                                    bay AS Bay,
                                    terminalarrival AS TerminalArrival,
                                    flightnumber_arrival AS FlightNumber_Arrival,
                                    flightnumber_departure AS FlightNumber_Departure,
                                    fromairport_iatacode AS FromAirport_IATACode,
                                    toairport_iatacode AS ToAirport_IATACode,
                                    flighttype AS FlightType,
                                    aircraftregistration_arrival as AircraftRegistration_Arrival,
                                    aircraftregistration_departure as AircraftRegistration_Departure
                            FROM DailyFlightSchedule_Merged_parquet
                            WHERE ((date_parse(STD, '%Y-%m-%d %H:%i:%s')>date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(STD, '%Y-%m-%d %H:%i:%s')<= date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(STA, '%Y-%m-%d %H:%i:%s')> date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(STA, '%Y-%m-%d %H:%i:%s')<=date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s')))
                                    AND OperationUnit = '{str(operation_unit)}' and sta!='' and std!='' and sta !='None' and std != 'None' {airline_filter_condition}"""

        
    
    print(query_for_dfs_merged)
    response = athena.start_query_execution(QueryString=query_for_dfs_merged, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))
    
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']
    
    while status=='RUNNING' or status=="QUEUED":
        print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
    
    s3 = boto3.client('s3',region_name='us-west-2')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/'+query_execution_id+'.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
    dfs_merged = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    print(dfs_merged.shape)
    
    # dfs_merged=pd.read_sql(f"select * from DailyFlightSchedule_Merged where ((STD>'{iu_min_date}' and STD<= '{iu_max_date}') or (STA> '{iu_min_date}' and STA<='{iu_max_date}')) and OperationUnit = {operation_unit} {airline_filter_condition}", con=connection)
    # print(f"select * from DailyFlightSchedule_Merged where ((STD>'{iu_min_date}' and STD<= '{iu_max_date}') or (STA> '{iu_min_date}' and STA<='{iu_max_date}')) and OperationUnit = {operation_unit} {airline_filter_condition}")
    if AirlineName!=None:
        dfs_merged=dfs_merged[dfs_merged.Airline==AirlineName]
    dfs_merged_with_bodytype = None
    if operation_unit=='22':
        dfs_merged_with_bodytype=pd.merge(dfs_merged,flight_master, on=['AircraftRegistration_Arrival'], how='left')  
    else:
        dfs_merged_with_bodytype=pd.merge(dfs_merged,flight_master, left_on=['Aircraft'],right_on=['Hexcode'],how='left')
    
    dfs_merged_with_bodytype.dropna(subset=["Bay"], inplace=True)
    dfs_merged_with_bodytype_psta=None
    if not operation_unit=='22':    
        arrival_departure_combined=pd.merge(arrival_master, departure_master, on='RKEY', how='inner')[["RKEY","PSTA"]]
        dfs_merged_with_bodytype.R_Key.dropna(inplace=True)
        dfs_merged_with_bodytype.R_Key=dfs_merged_with_bodytype.R_Key.astype(int)
        arrival_departure_combined.RKEY=arrival_departure_combined.RKEY.astype(int)
        dfs_merged_with_bodytype_psta=dfs_merged_with_bodytype.merge(arrival_departure_combined, left_on='R_Key', right_on='RKEY', how='left')
    elif operation_unit=='22':
        dfs_merged_with_bodytype_psta=dfs_merged_with_bodytype
        dfs_merged_with_bodytype_psta['PSTA']=dfs_merged_with_bodytype.AllocatedBay
        dfs_merged_with_bodytype_psta.TerminalArrival=np.where(dfs_merged_with_bodytype_psta.TerminalArrival.isin(['1C','1D']), 'T1', dfs_merged_with_bodytype_psta.TerminalArrival)
    dfs_merged_with_bodytype_psta.TerminalArrival=dfs_merged_with_bodytype_psta.TerminalArrival.fillna('DISABLED')
    dfs_merged_with_bodytype_psta.STA=dfs_merged_with_bodytype_psta.STA.astype(str)
    dfs_merged_with_bodytype_psta.STD=dfs_merged_with_bodytype_psta.STD.astype(str)
    dfs_merged_with_bodytype_psta.Sensor_ATA=dfs_merged_with_bodytype_psta.Sensor_ATA.astype(str)
    dfs_merged_with_bodytype_psta.Sensor_ATD=dfs_merged_with_bodytype_psta.Sensor_ATD.astype(str)
    dfs_merged_with_bodytype_psta.PSTA=np.where(dfs_merged_with_bodytype_psta.PSTA.isna(), dfs_merged_with_bodytype_psta.Bay, dfs_merged_with_bodytype_psta.PSTA)
    dfs_merged_with_bodytype_psta.STA=np.where(dfs_merged_with_bodytype_psta.STA.isna() | dfs_merged_with_bodytype_psta.STA==None, dfs_merged_with_bodytype_psta.Sensor_ATA, dfs_merged_with_bodytype_psta.STA)
    dfs_merged_with_bodytype_psta.STD=np.where(pd.to_datetime(dfs_merged_with_bodytype_psta.STD).isna() | dfs_merged_with_bodytype_psta.STD==None, dfs_merged_with_bodytype_psta.Sensor_ATD, dfs_merged_with_bodytype_psta.STD)
    dfs_merged_with_bodytype_psta.STD=np.where((pd.to_datetime(dfs_merged_with_bodytype_psta.STD)-pd.to_datetime(dfs_merged_with_bodytype_psta.STA)).dt.days>0, dfs_merged_with_bodytype_psta.Sensor_ATD, dfs_merged_with_bodytype_psta.STD)
    dfs_merged_with_bodytype_psta.STA=dfs_merged_with_bodytype_psta.STA.astype(str)
    dfs_merged_with_bodytype_psta.STD=dfs_merged_with_bodytype_psta.STD.astype(str)
    # dfs_merged_with_bodytype_psta=dfs_merged_with_bodytype_psta[~(dfs_merged_with_bodytype_psta.STA=='None')]
    dfs_merged_with_bodytype_psta.STA=np.where(pd.to_datetime(dfs_merged_with_bodytype_psta.STA).dt.day<iu_min_date.day,
                                          str(pd.datetime(iu_min_date.year, iu_min_date.month, iu_min_date.day, 0, 0, 1)),
                                          dfs_merged_with_bodytype_psta.STA)
    # dfs_merged_with_bodytype_psta=dfs_merged_with_bodytype_psta[~(dfs_merged_with_bodytype_psta.STD=='None')]
    print(dfs_merged_with_bodytype_psta.shape)
    dfs_merged_with_bodytype_psta.STD=np.where( pd.to_datetime(dfs_merged_with_bodytype_psta.STD).dt.day> iu_max_date.day ,
                                        str(pd.datetime(iu_max_date.year, iu_max_date.month, iu_max_date.day, 23, 59, 59)),
                                        dfs_merged_with_bodytype_psta.STD)
    dfs_merged_with_bodytype_psta_without_dups=dfs_merged_with_bodytype_psta.drop_duplicates(subset=['LogId'])
    dfs_merged_with_bodytype_psta_without_dups_sorted=dfs_merged_with_bodytype_psta_without_dups.sort_values(by=['PSTA', 'STA'])
    dfs_merged_with_bodytype_psta_without_dups_sorted.STA=dfs_merged_with_bodytype_psta_without_dups_sorted.STA.astype(str)
    dfs_merged_with_bodytype_psta_without_dups_sorted.STD=dfs_merged_with_bodytype_psta_without_dups_sorted.STD.astype(str)
    
    for column in dfs_merged_with_bodytype_psta_without_dups_sorted:

        dfs_merged_with_bodytype_psta_without_dups_sorted[column]=dfs_merged_with_bodytype_psta_without_dups_sorted[column].apply(lambda x : "" if x=='NaT' else x )

    
    dfs_merged_with_bodytype_psta_without_dups_sorted['buffer']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted.PSTA==dfs_merged_with_bodytype_psta_without_dups_sorted.PSTA.shift(1),
                                                                pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted.STA)-(pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted.STD).shift(1)),
                                                                pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted.STA)-pd.datetime(iu_min_date.year, iu_min_date.month, iu_min_date.day, 0, 0, 1))
    dfs_merged_with_bodytype_psta_without_dups_sorted["buffer"].dropna(inplace=True)

    dfs_merged_with_bodytype_psta_without_dups_sorted.buffer=dfs_merged_with_bodytype_psta_without_dups_sorted["buffer"].astype(int)/60000000000
    dfs_merged_with_bodytype_psta_without_dups_sorted["STD"].dropna(inplace=True)

    #dfs_merged_with_bodytype_psta_without_dups_sorted=dfs_merged_with_bodytype_psta_without_dups_sorted[dfs_merged_with_bodytype_psta_without_dups_sorted['STD']!='NaT']

    dfs_merged_with_bodytype_psta_without_dups_sorted['tail_buffer']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted.PSTA!=dfs_merged_with_bodytype_psta_without_dups_sorted.PSTA.shift(-1),
                                                                pd.datetime(iu_max_date.year, iu_max_date.month, iu_max_date.day, 23, 59, 59)-(pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted.STD)),0)
    dfs_merged_with_bodytype_psta_without_dups_sorted=dfs_merged_with_bodytype_psta_without_dups_sorted[dfs_merged_with_bodytype_psta_without_dups_sorted['tail_buffer']!='NaT']
    #dfs_merged_with_bodytype_psta_without_dups_sorted.to_csv('data.csv')
    dfs_merged_with_bodytype_psta_without_dups_sorted["tail_buffer"].dropna(inplace=True)
    dfs_merged_with_bodytype_psta_without_dups_sorted.tail_buffer=dfs_merged_with_bodytype_psta_without_dups_sorted["tail_buffer"].astype(int)/60000000000
    dfs_merged_with_bodytype_psta_without_dups_sorted['total_buffer']=dfs_merged_with_bodytype_psta_without_dups_sorted.buffer+dfs_merged_with_bodytype_psta_without_dups_sorted.tail_buffer
    dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer.isnull(),0.0, dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer)
    overall_scheduled_min_buffer=-1 if np.isnan(dfs_merged_with_bodytype_psta_without_dups_sorted[dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer>0]['total_buffer'].min()) else round(dfs_merged_with_bodytype_psta_without_dups_sorted[dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer>0]['total_buffer'].min())
    overall_scheduled_max_buffer=-1 if np.isnan(dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer.max()) else round(dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer.max())
    overall_scheduled_overlap=round(100*(dfs_merged_with_bodytype_psta_without_dups_sorted[~ (dfs_merged_with_bodytype_psta_without_dups_sorted.buffer.isna()) & (dfs_merged_with_bodytype_psta_without_dups_sorted.buffer<0)].shape[0] )/dfs_merged_with_bodytype_psta_without_dups_sorted.LogId.shape[0])
    dfs_merged_with_bodytype_psta_without_dups_sorted.STD=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted.buffer.shift(-1)<0, dfs_merged_with_bodytype_psta_without_dups_sorted.STA.shift(-1), dfs_merged_with_bodytype_psta_without_dups_sorted.STD)
    if operation_unit=='22':
        dfs_merged_with_bodytype_psta_without_dups_sorted['FlightType'] = 'Domestic'
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered=dfs_merged_with_bodytype_psta_without_dups_sorted[["PSTA","STA","STD","LogId","FlightNumber_Arrival", "FlightNumber_Departure","FlightType", "ToAirport_IATACode", "FromAirport_IATACode", "TerminalArrival"]]

        
    else :
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered=dfs_merged_with_bodytype_psta_without_dups_sorted[["PSTA","STA","STD","LogId","FlightNumber_Arrival", "FlightNumber_Departure","FlightType", "ToAirport_IATACode", "FromAirport_IATACode", "TerminalArrival"]]
    
    
    dfs_merged_with_bodytype_psta_without_dups_sorted_filtered=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered[pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.STD).dt.day.isin([ts.day for ts in ts_range[1:-1]])]
    dfs_merged_with_bodytype_psta_without_dups_sorted_filtered=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered[pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.STD)>pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.STA)]
    if operation_unit=='22':
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered[~dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.TerminalArrival.isin(['F','X','Z'])]
    long_scheduled=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
    long_scheduled.rename(columns={'PSTA':'Bay'}, inplace=True)
    long_scheduled=pd.merge(long_scheduled, city_airport_master, right_on="IATACode", left_on="ToAirport_IATACode")
    long_scheduled.rename(columns={'City':'To_City'}, inplace=True)
    long_scheduled=pd.merge(long_scheduled, city_airport_master, right_on="IATACode", left_on="FromAirport_IATACode")
    long_scheduled.rename(columns={'City':'From_City'}, inplace=True)
    long_scheduled.loc[long_scheduled.BodyType=="Bombardier",'BodyType']="Turboprop"
    long_scheduled.loc[long_scheduled.BodyType==""]=np.nan
    long_scheduled.drop(['FlightType','FromAirport_IATACode','ToAirport_IATACode','IATACode_x','IATACode_y'], axis=1, inplace=True)
    
    if operation_unit=='4' :
        long_scheduled['BayType']=np.where(long_scheduled.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
    else:
        long_scheduled['BayType']=np.where(long_scheduled.Bay.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')
    long_scheduled_json=long_scheduled.to_json(orient='split')
    
    
    # ============================================================================
    # |                                                                           |
    # |                  Scheduled Bay Overall utilization                        |
    # |                                                                           |
    # ============================================================================
    
    dfs_merged_with_bodytype_psta_without_dups_sorted_filtered["util"]=pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.STD) - pd.to_datetime(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.STA)
    dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['util']=round(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.util.dt.seconds/60)
    All_scheduled_max_buffer = 0
    All_scheduled_min_buffer = float('inf')
    All_scheduled_overlap = 0
    for terminal in dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.TerminalArrival.unique():
        if terminal == 'T1' or terminal == 'T2' or terminal == 'T3':
                    
            scheduled_total_turnaround_time=None
            scheduled_total_parking_time=None
            overlap_percent_utilization_for_remote=None
            overlap_percent_utilization_for_connected=None
            scheduled_min_buffer=None
            scheduled_max_buffer=None
            scheduled_overlap=None
            total_schedule_flights = None
            remote_flights_per = None
            connected_flights_per = None
            terminal_aggregates={}
            dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.TerminalArrival==terminal]
            dfs_merged_with_bodytype_psta_without_dups_sorted_temp=dfs_merged_with_bodytype_psta_without_dups_sorted[dfs_merged_with_bodytype_psta_without_dups_sorted.TerminalArrival==terminal]
            scheduled_min_buffer=-1 if np.isnan(dfs_merged_with_bodytype_psta_without_dups_sorted_temp[(dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer>0)]['total_buffer'].min()) else round(dfs_merged_with_bodytype_psta_without_dups_sorted_temp[(dfs_merged_with_bodytype_psta_without_dups_sorted_temp.total_buffer>0)]['total_buffer'].min())
            scheduled_max_buffer=-1 if np.isnan(dfs_merged_with_bodytype_psta_without_dups_sorted_temp['total_buffer'].max()) else round(dfs_merged_with_bodytype_psta_without_dups_sorted_temp['total_buffer'].max())
            scheduled_overlap=-1 if dfs_merged_with_bodytype_psta_without_dups_sorted_temp.LogId.shape[0]==0 else round(100*(dfs_merged_with_bodytype_psta_without_dups_sorted_temp[ ~(dfs_merged_with_bodytype_psta_without_dups_sorted_temp.buffer.isna()) & (dfs_merged_with_bodytype_psta_without_dups_sorted_temp.buffer<0)].shape[0] )/dfs_merged_with_bodytype_psta_without_dups_sorted_temp.LogId.shape[0])
        
            if scheduled_min_buffer < All_scheduled_min_buffer:
                All_scheduled_min_buffer=scheduled_min_buffer
            if scheduled_max_buffer > All_scheduled_max_buffer:
                All_scheduled_max_buffer=scheduled_max_buffer
            if scheduled_overlap > All_scheduled_overlap:
                All_scheduled_overlap=scheduled_overlap
        
        
            if operation_unit=='22':
                
                
                dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise.TerminalArrival.isin(['T3']),'Connected','Remote')
                dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
                overall_scheduled_utilization_for_terminal=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.groupby(['PSTA','BayType','FlightType','BodyType','TerminalArrival']).agg({'util':'sum'}).reset_index()
                overall_scheduled_utilization_for_terminal['percent_util']=round(100*overall_scheduled_utilization_for_terminal.util/1440)
                overlap_percent_utilization_for_remote=-1 if np.isnan(overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Remote']['percent_util'].mean()) else overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Remote']['percent_util'].mean()
                overlap_percent_utilization_for_connected=-1 if np.isnan(overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Connected']['percent_util'].mean()) else overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Connected']['percent_util'].mean()
                overall_scheduled_utilization_for_terminal.drop("BayType", axis=1, inplace=True)
                scheduled_total_turnaround_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util<240]['util'].sum()
                scheduled_total_parking_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util>240]['util'].sum()
                terminal_aggregates['scheduled_total_turnaround_time']=-1 if np.isnan(scheduled_total_turnaround_time) else scheduled_total_turnaround_time
                terminal_aggregates['scheduled_total_parking_time']= -1 if np.isnan(scheduled_total_parking_time) else scheduled_total_parking_time
                terminal_aggregates['overlap_percent_utilization_for_remote']= -1 if np.isnan(overlap_percent_utilization_for_remote) else overlap_percent_utilization_for_remote
                terminal_aggregates['overlap_percent_utilization_for_connected']= -1 if np.isnan(overlap_percent_utilization_for_connected) else overlap_percent_utilization_for_connected
                terminal_aggregates['scheduled_min_buffer']=-1 if np.isnan(scheduled_min_buffer) else scheduled_min_buffer
                terminal_aggregates['scheduled_max_buffer']=-1 if np.isnan(scheduled_max_buffer) else scheduled_max_buffer
                terminal_aggregates['scheduled_overlap']=-1 if np.isnan(scheduled_overlap) else scheduled_overlap
                total_schedule_flights = len(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'])
                terminal_aggregates['totalFlights'] = -1 if np.isnan(total_schedule_flights) else total_schedule_flights
                total_dataframe_flights = dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'].value_counts()
                if 'Remote' in total_dataframe_flights:
                    remote_flights_per = total_dataframe_flights.values[0]
                    remote_flights_per = round(remote_flights_per*100/total_schedule_flights)
                    terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
                if 'Connected' in total_dataframe_flights:
                    connected_flights_per = total_dataframe_flights.values[0]
                    connected_flights_per = round(connected_flights_per*100/total_schedule_flights)
                    terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
            #scheduled_aggregates[terminal]=terminal_aggregates
                
                
#                 dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise.PSTA.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
#                 dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
#                 overall_scheduled_utilization_for_terminal=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.groupby(['PSTA','BayType','FlightType','BodyType','TerminalArrival']).agg({'util':'sum'}).reset_index()
#                 overall_scheduled_utilization_for_terminal['percent_util']=round(100*overall_scheduled_utilization_for_terminal.util/1440)
#                 overlap_percent_utilization_for_remote=overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Remote']['percent_util'].mean()
#                 overlap_percent_utilization_for_connected=overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Connected']['percent_util'].mean()
#                 overall_scheduled_utilization_for_terminal.drop("BayType", axis=1, inplace=True)
#                 scheduled_total_turnaround_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util<240]['util'].sum()
#                 scheduled_total_parking_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util>240]['util'].sum()
#                 terminal_aggregates['scheduled_total_turnaround_time']=-1 if np.isnan(scheduled_total_turnaround_time) else scheduled_total_turnaround_time
#                 terminal_aggregates['scheduled_total_parking_time']=-1 if np.isnan(scheduled_total_parking_time) else scheduled_total_parking_time
#                 terminal_aggregates['overlap_percent_utilization_for_remote']=-1 if np.isnan(overlap_percent_utilization_for_remote) else overlap_percent_utilization_for_remote
#                 terminal_aggregates['overlap_percent_utilization_for_connected']=-1 if np.isnan(overlap_percent_utilization_for_connected) else overlap_percent_utilization_for_connected
#                 terminal_aggregates['scheduled_min_buffer']=-1 if np.isnan(scheduled_min_buffer) else scheduled_min_buffer
#                 terminal_aggregates['scheduled_max_buffer']=-1 if np.isnan(scheduled_max_buffer) else scheduled_max_buffer
#                 terminal_aggregates['scheduled_overlap']=-1 if np.isnan(scheduled_overlap) else scheduled_overlap
#                 total_schedule_flights = len(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'])
#                 terminal_aggregates['totalFlights'] = -1 if np.isnan(total_schedule_flights) else total_schedule_flights
#                 total_dataframe_flights = dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'].value_counts()
#                 if 'Remote' in total_dataframe_flights:
#                     remote_flights_per = total_dataframe_flights.values[0]
#                     remote_flights_per = round(remote_flights_per*100/total_schedule_flights)
#                     terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
#                 if 'Connected' in total_dataframe_flights:
#                     connected_flights_per = total_dataframe_flights.values[0]
#                     connected_flights_per = round(connected_flights_per*100/total_schedule_flights)
#                     terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
        else:
            scheduled_total_turnaround_time=None
            scheduled_total_parking_time=None
            overlap_percent_utilization_for_remote=None
            overlap_percent_utilization_for_connected=None
            scheduled_min_buffer=None
            scheduled_max_buffer=None
            scheduled_overlap=None
            total_schedule_flights = None
            remote_flights_per = None
            connected_flights_per = None
            terminal_aggregates={}
            dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.TerminalArrival==terminal]
            dfs_merged_with_bodytype_psta_without_dups_sorted_temp=dfs_merged_with_bodytype_psta_without_dups_sorted[dfs_merged_with_bodytype_psta_without_dups_sorted.TerminalArrival==terminal]
            scheduled_min_buffer=-1 if np.isnan(dfs_merged_with_bodytype_psta_without_dups_sorted_temp[(dfs_merged_with_bodytype_psta_without_dups_sorted.total_buffer>0)]['total_buffer'].min()) else round(dfs_merged_with_bodytype_psta_without_dups_sorted_temp[(dfs_merged_with_bodytype_psta_without_dups_sorted_temp.total_buffer>0)]['total_buffer'].min())
            scheduled_max_buffer=-1 if np.isnan(dfs_merged_with_bodytype_psta_without_dups_sorted_temp['total_buffer'].max()) else round(dfs_merged_with_bodytype_psta_without_dups_sorted_temp['total_buffer'].max())
            scheduled_overlap=-1 if dfs_merged_with_bodytype_psta_without_dups_sorted_temp.LogId.shape[0]==0 else round(100*(dfs_merged_with_bodytype_psta_without_dups_sorted_temp[ ~(dfs_merged_with_bodytype_psta_without_dups_sorted_temp.buffer.isna()) & (dfs_merged_with_bodytype_psta_without_dups_sorted_temp.buffer<0)].shape[0] )/dfs_merged_with_bodytype_psta_without_dups_sorted_temp.LogId.shape[0])
        
            if scheduled_min_buffer < All_scheduled_min_buffer:
                All_scheduled_min_buffer=scheduled_min_buffer
            if scheduled_max_buffer > All_scheduled_max_buffer:
                All_scheduled_max_buffer=scheduled_max_buffer
            if scheduled_overlap > All_scheduled_overlap:
                All_scheduled_overlap=scheduled_overlap
                
            if operation_unit=='4' or operation_unit=='13':
                print("ggggggggggggggggggggggggggggggggggggggggggggggggggg")
                if operation_unit=='4' :
                    dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise.PSTA.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
                else :
                    dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise.PSTA.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')
                    
                
                dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
                dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp['FlightType'] = 'Domestic'
                overall_scheduled_utilization_for_terminal=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.groupby(['PSTA','BayType','BodyType','FlightType','TerminalArrival']).agg({'util':'sum'}).reset_index()
                overall_scheduled_utilization_for_terminal['percent_util']=round(100*overall_scheduled_utilization_for_terminal.util/1440)
                overlap_percent_utilization_for_remote=overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Remote']['percent_util'].mean()
                overlap_percent_utilization_for_connected=overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Connected']['percent_util'].mean()
                overall_scheduled_utilization_for_terminal.drop("BayType", axis=1, inplace=True)
                scheduled_total_turnaround_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util<240]['util'].sum()
                scheduled_total_parking_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util>240]['util'].sum()
                terminal_aggregates['scheduled_total_turnaround_time']=-1 if np.isnan(scheduled_total_turnaround_time) else scheduled_total_turnaround_time
                terminal_aggregates['scheduled_total_parking_time']=-1 if np.isnan(scheduled_total_parking_time) else scheduled_total_parking_time
                terminal_aggregates['overlap_percent_utilization_for_remote']=-1 if np.isnan(overlap_percent_utilization_for_remote) else overlap_percent_utilization_for_remote
                terminal_aggregates['overlap_percent_utilization_for_connected']=-1 if np.isnan(overlap_percent_utilization_for_connected) else overlap_percent_utilization_for_connected
                terminal_aggregates['scheduled_min_buffer']=-1 if np.isnan(scheduled_min_buffer) else scheduled_min_buffer
                terminal_aggregates['scheduled_max_buffer']=-1 if np.isnan(scheduled_max_buffer) else scheduled_max_buffer
                terminal_aggregates['scheduled_overlap']=-1 if np.isnan(scheduled_overlap) else scheduled_overlap
                total_schedule_flights = len(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'])
                terminal_aggregates['totalFlights'] = -1 if np.isnan(total_schedule_flights) else total_schedule_flights
                total_dataframe_flights = dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'].value_counts()
                if 'Remote' in total_dataframe_flights:
                    remote_flights_per = total_dataframe_flights.values[0]
                    remote_flights_per = round(remote_flights_per*100/total_schedule_flights)
                    terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
                if 'Connected' in total_dataframe_flights:
                    connected_flights_per = total_dataframe_flights.values[0]
                    connected_flights_per = round(connected_flights_per*100/total_schedule_flights)
                    terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
                
                
            
#                 dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise.TerminalArrival.isin(['T3']),'Connected','Remote')
#                 dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
#                 overall_scheduled_utilization_for_terminal=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.groupby(['PSTA','BayType','FlightType','BodyType','TerminalArrival']).agg({'util':'sum'}).reset_index()
#                 overall_scheduled_utilization_for_terminal['percent_util']=round(100*overall_scheduled_utilization_for_terminal.util/1440)
#                 overlap_percent_utilization_for_remote=-1 if np.isnan(overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Remote']['percent_util'].mean()) else overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Remote']['percent_util'].mean()
#                 overlap_percent_utilization_for_connected=-1 if np.isnan(overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Connected']['percent_util'].mean()) else overall_scheduled_utilization_for_terminal[overall_scheduled_utilization_for_terminal.BayType=='Connected']['percent_util'].mean()
#                 overall_scheduled_utilization_for_terminal.drop("BayType", axis=1, inplace=True)
#                 scheduled_total_turnaround_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util<240]['util'].sum()
#                 scheduled_total_parking_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util>240]['util'].sum()
#                 terminal_aggregates['scheduled_total_turnaround_time']=-1 if np.isnan(scheduled_total_turnaround_time) else scheduled_total_turnaround_time
#                 terminal_aggregates['scheduled_total_parking_time']= -1 if np.isnan(scheduled_total_parking_time) else scheduled_total_parking_time
#                 terminal_aggregates['overlap_percent_utilization_for_remote']= -1 if np.isnan(overlap_percent_utilization_for_remote) else overlap_percent_utilization_for_remote
#                 terminal_aggregates['overlap_percent_utilization_for_connected']= -1 if np.isnan(overlap_percent_utilization_for_connected) else overlap_percent_utilization_for_connected
#                 terminal_aggregates['scheduled_min_buffer']=-1 if np.isnan(scheduled_min_buffer) else scheduled_min_buffer
#                 terminal_aggregates['scheduled_max_buffer']=-1 if np.isnan(scheduled_max_buffer) else scheduled_max_buffer
#                 terminal_aggregates['scheduled_overlap']=-1 if np.isnan(scheduled_overlap) else scheduled_overlap
#                 total_schedule_flights = len(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'])
#                 terminal_aggregates['totalFlights'] = -1 if np.isnan(total_schedule_flights) else total_schedule_flights
#                 total_dataframe_flights = dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_terminal_wise['BayType'].value_counts()
#                 if 'Remote' in total_dataframe_flights:
#                     remote_flights_per = total_dataframe_flights.values[0]
#                     remote_flights_per = round(remote_flights_per*100/total_schedule_flights)
#                     terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
#                 if 'Connected' in total_dataframe_flights:
#                     connected_flights_per = total_dataframe_flights.values[0]
#                     connected_flights_per = round(connected_flights_per*100/total_schedule_flights)
#                     terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
        scheduled_aggregates[terminal]=terminal_aggregates
    overall_sch_terminal_aggregates={}
    scheduled_total_turnaround_time=None
    scheduled_total_parking_time=None
    overlap_percent_utilization_for_remote=None
    overlap_percent_utilization_for_connected=None
    scheduled_min_buffer=None
    scheduled_max_buffer=None
    scheduled_overlap=None
    total_schedule_flights = None
    remote_flights_per = None
    connected_flights_per = None
    print("bangalore schedule",All_scheduled_min_buffer,All_scheduled_max_buffer,All_scheduled_overlap)
    if not operation_unit=='22':
        if operation_unit=='4' :
            dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.PSTA.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')

            #dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.PSTA.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
        else :
            dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.PSTA.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')

            #dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.PSTA.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')

        print("1111111111111111111111111111111111111111")
        #dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.PSTA.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
        #dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.to_csv("testing.csv")
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp['FlightType'] = 'Domestic'
        overall_scheduled_utilization_=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.groupby(['PSTA','BayType','BodyType','FlightType','TerminalArrival']).agg({'util':'sum'}).reset_index()
        overall_scheduled_utilization_['percent_util']=round(100*overall_scheduled_utilization_.util/1440)
        print("overall_scheduled_utilization_",dfs_merged_with_bodytype_psta_without_dups_sorted_filtered)
        overlap_percent_utilization_for_remote=overall_scheduled_utilization_[overall_scheduled_utilization_.BayType=='Remote']['percent_util'].mean()
        overlap_percent_utilization_for_connected=overall_scheduled_utilization_[overall_scheduled_utilization_.BayType=='Connected']['percent_util'].mean()
        overall_scheduled_utilization_.drop("BayType", axis=1, inplace=True)
        scheduled_total_turnaround_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util<240]['util'].sum()
        scheduled_total_parking_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util>240]['util'].sum()
        overall_sch_terminal_aggregates['scheduled_total_turnaround_time']=-1 if np.isnan(scheduled_total_turnaround_time) else scheduled_total_turnaround_time
        overall_sch_terminal_aggregates['scheduled_total_parking_time']=-1 if np.isnan(scheduled_total_parking_time) else scheduled_total_parking_time
        overall_sch_terminal_aggregates['overlap_percent_utilization_for_remote']=-1 if np.isnan(overlap_percent_utilization_for_remote) else overlap_percent_utilization_for_remote
        overall_sch_terminal_aggregates['overlap_percent_utilization_for_connected']=-1 if np.isnan(overlap_percent_utilization_for_connected) else overlap_percent_utilization_for_connected
        overall_sch_terminal_aggregates['scheduled_min_buffer']=-1 if np.isnan(All_scheduled_min_buffer) else All_scheduled_min_buffer
        overall_sch_terminal_aggregates['scheduled_max_buffer']=-1 if np.isnan(All_scheduled_max_buffer) else All_scheduled_max_buffer
        overall_sch_terminal_aggregates['scheduled_overlap']=-1 if np.isnan(All_scheduled_overlap) else All_scheduled_overlap
        total_schedule_flights = len(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.BayType)
        overall_sch_terminal_aggregates['totalFlights'] = -1 if np.isnan(total_schedule_flights) else total_schedule_flights
        total_dataframe_flights = dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['BayType'].value_counts()
        remote_flights_per = total_dataframe_flights[1]
        remote_flights_per = round(remote_flights_per*100/total_schedule_flights)
        connected_flights_per = total_dataframe_flights[0]
        connected_flights_per = round(connected_flights_per*100/total_schedule_flights)
        overall_sch_terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
        overall_sch_terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
    else:
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['BayType']=np.where(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.TerminalArrival.isin(['T3']),'Connected','Remote')
        dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp=pd.merge(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered, dfs_merged_with_bodytype_psta_without_dups_sorted[["LogId","BodyType"]], on="LogId")
        overall_scheduled_utilization_=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.groupby(['PSTA','BayType','FlightType','BodyType','TerminalArrival']).agg({'util':'sum'}).reset_index()
        overall_scheduled_utilization_['percent_util']=round(100*overall_scheduled_utilization_.util/1440)
        overlap_percent_utilization_for_remote=overall_scheduled_utilization_[overall_scheduled_utilization_.BayType=='Remote']['percent_util'].mean()
        overlap_percent_utilization_for_connected=overall_scheduled_utilization_[overall_scheduled_utilization_.BayType=='Connected']['percent_util'].mean()
        
        overall_scheduled_utilization_.drop("BayType", axis=1, inplace=True)
        scheduled_total_turnaround_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util<240]['util'].sum()
        scheduled_total_parking_time=dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp[dfs_merged_with_bodytype_psta_without_dups_sorted_filtered_temp.util>240]['util'].sum()
        overall_sch_terminal_aggregates['scheduled_total_turnaround_time']=-1 if np.isnan(scheduled_total_turnaround_time) else scheduled_total_turnaround_time
        overall_sch_terminal_aggregates['scheduled_total_parking_time']=-1 if np.isnan(scheduled_total_parking_time) else scheduled_total_parking_time
        overall_sch_terminal_aggregates['overlap_percent_utilization_for_remote']=-1 if np.isnan(overlap_percent_utilization_for_remote) else overlap_percent_utilization_for_remote
        overall_sch_terminal_aggregates['overlap_percent_utilization_for_connected']=-1 if np.isnan(overlap_percent_utilization_for_connected) else overlap_percent_utilization_for_connected
        overall_sch_terminal_aggregates['scheduled_min_buffer']=-1 if np.isnan(All_scheduled_min_buffer) else All_scheduled_min_buffer
        overall_sch_terminal_aggregates['scheduled_max_buffer']=-1 if np.isnan(All_scheduled_max_buffer) else All_scheduled_max_buffer
        overall_sch_terminal_aggregates['scheduled_overlap']=-1 if np.isnan(All_scheduled_overlap) else All_scheduled_overlap
        total_schedule_flights = len(dfs_merged_with_bodytype_psta_without_dups_sorted_filtered.BayType)
        overall_sch_terminal_aggregates['totalFlights'] = -1 if np.isnan(total_schedule_flights) else total_schedule_flights
        total_dataframe_flights = dfs_merged_with_bodytype_psta_without_dups_sorted_filtered['BayType'].value_counts()
        remote_flights_per = total_dataframe_flights[1]
        remote_flights_per = round(remote_flights_per*100/total_schedule_flights)
        connected_flights_per = total_dataframe_flights[0]
        connected_flights_per = round(connected_flights_per*100/total_schedule_flights)
        overall_sch_terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
        overall_sch_terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
    scheduled_aggregates['All']=overall_sch_terminal_aggregates
        
    
# #  =======================================================================================================================================================================================================================================#
    
#     ####################################################################################
#     ##                                                                                ##
#     ##                            Actual Bay Utilization                              ##
#     ##                                                                                ##
#     ####################################################################################
    overall_actual_utilization_for_remote=None
    overall_actual_utilization_for_connected=None
    actual_aggregates={}
    
  
    query_for_dfs_merged_act=f"""SELECT cast(logid as integer) AS LogId,
                                    cast(STA AS timestamp) AS STA,
                                    cast(STD AS timestamp) AS STD,
                                    cast(ATD AS timestamp) AS ATD,
                                    cast(ATA AS timestamp) AS ATA,
                                    cast(ETD AS timestamp) AS ETD,
                                    cast(ETA AS timestamp) AS ETA,
                                    cast(On_Block_Time AS timestamp) AS On_Block_Time,
                                    cast(off_block_time AS timestamp) AS Off_Block_Time,
                                    cast(Sensor_ATD AS timestamp) AS Sensor_ATD,
                                    cast(Sensor_ATA AS timestamp) AS Sensor_ATA,
                                    airline AS Airline,
                                    aircraft AS Aircraft,
                                    r_key AS R_Key,
                                    allocatedbay AS AllocatedBay,
                                    bay AS Bay,
                                    terminalarrival AS TerminalArrival,
                                    flightnumber_arrival AS FlightNumber_Arrival,
                                    flightnumber_departure AS FlightNumber_Departure,
                                    fromairport_iatacode AS FromAirport_IATACode,
                                    toairport_iatacode AS ToAirport_IATACode,
                                    flighttype AS FlightType,
                                    aircraftregistration_arrival as AircraftRegistration_Arrival,
                                    aircraftregistration_departure as AircraftRegistration_Departure
                            FROM DailyFlightSchedule_Merged_parquet
                            WHERE ((date_parse(STD, '%Y-%m-%d %H:%i:%s')>date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(STD, '%Y-%m-%d %H:%i:%s')<= date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(STA, '%Y-%m-%d %H:%i:%s')> date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(STA, '%Y-%m-%d %H:%i:%s')<=date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(ATD, '%Y-%m-%d %H:%i:%s')>date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(ATD, '%Y-%m-%d %H:%i:%s')<= date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(ATA, '%Y-%m-%d %H:%i:%s')> date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(ATA, '%Y-%m-%d %H:%i:%s')<=date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(ETD, '%Y-%m-%d %H:%i:%s')>date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(ETD, '%Y-%m-%d %H:%i:%s')<= date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(ETA, '%Y-%m-%d %H:%i:%s')> date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(ETA, '%Y-%m-%d %H:%i:%s')<=date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s')>date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s')<= date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s'))
                                    OR (date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')> date_parse('{iu_min_date}','%Y-%m-%d %H:%i:%s')
                                    AND date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')<=date_parse('{iu_max_date}','%Y-%m-%d %H:%i:%s')))
                                    AND OperationUnit = '{str(operation_unit)}' AND sta!='' and std!='' and sta !='None' and std != 'None' {airline_filter_condition}"""
                                    
    print(query_for_dfs_merged_act)
    response = athena.start_query_execution(QueryString=query_for_dfs_merged_act, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))
    
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']
    
    while status=='RUNNING' or status=="QUEUED":
        # print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
    
    s3 = boto3.client('s3',region_name='us-west-2')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/'+query_execution_id+'.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
    dfs_merged_act = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    
    # dfs_merged_act=pd.read_sql(f"select * from DailyFlightSchedule_Merged where ((Off_Block_Time>'{iu_min_date}' and Off_Block_Time<= '{iu_max_date}') or (On_Block_Time> '{iu_min_date}' and On_Block_Time<='{iu_max_date}') or (STD>'{iu_min_date}' and STD<= '{iu_max_date}') or (STA> '{iu_min_date}' and STA<='{iu_max_date}') or (STD>'{iu_min_date}' and STD<= '{iu_max_date}') or (ATA> '{iu_min_date}' and ATA<='{iu_max_date}') or (STD>'{iu_min_date}' and STD<= '{iu_max_date}') or (ETA> '{iu_min_date}' and ETA<='{iu_max_date}')) and OperationUnit = {operation_unit} {airline_filter_condition}", con=connection)
    if AirlineName!=None:
        dfs_merged_act=dfs_merged_act[dfs_merged_act.Airline==AirlineName]
    dfs_merged_act_flightmaster=None
    if operation_unit =='22':
        dfs_merged_act_flightmaster = dfs_merged_act.merge(flight_master, on=['AircraftRegistration_Arrival'], how='left')  
    else: 
        dfs_merged_act_flightmaster = dfs_merged_act.merge(flight_master, left_on=['Aircraft'],right_on=['Hexcode'],how='left')
    if not operation_unit=='22':    
        arrival_departure_combined=pd.merge(arrival_master, departure_master, on='RKEY', how='inner')[["RKEY","PSTA"]]
        dfs_merged_act_flightmaster.R_Key.dropna(inplace=True)
        dfs_merged_act_flightmaster.R_Key=dfs_merged_act_flightmaster.R_Key.astype(int)
        arrival_departure_combined.RKEY=arrival_departure_combined.RKEY.astype(int)
        dfs_merged_act_flightmaster=dfs_merged_act_flightmaster.merge(arrival_departure_combined, left_on='R_Key', right_on='RKEY', how='left')
    elif operation_unit=='22':
        dfs_merged_act_flightmaster.TerminalArrival=np.where(dfs_merged_act_flightmaster.TerminalArrival.isin(['1C','1D']), 'T1', dfs_merged_act_flightmaster.TerminalArrival)
    # print(f"Num records {str(dfs_merged_act_flightmaster[dfs_merged_act_flightmaster.Bay=='A10'].shape[0])}")
    dfs_merged_act_flightmaster.TerminalArrival=dfs_merged_act_flightmaster.TerminalArrival.fillna('DISABLED')
    dfs_merged_act_flightmaster=dfs_merged_act_flightmaster[~ dfs_merged_act_flightmaster.Bay.isna()]
    # print(f"Num records {str(dfs_merged_act_flightmaster[dfs_merged_act_flightmaster.Bay=='A10'].shape[0])}")
    # print('dfs_merged_act_flightmaster',dfs_merged_act_flightmaster)
    dfs_merged_act_flightmaster.On_Block_Time=dfs_merged_act_flightmaster.On_Block_Time.astype(str)
    dfs_merged_act_flightmaster.Off_Block_Time=dfs_merged_act_flightmaster.Off_Block_Time.astype(str)
    dfs_merged_act_flightmaster.ATA=dfs_merged_act_flightmaster.ATA.astype(str)
    dfs_merged_act_flightmaster.ATD=dfs_merged_act_flightmaster.ATD.astype(str)
    dfs_merged_act_flightmaster.Sensor_ATA=dfs_merged_act_flightmaster.Sensor_ATA.astype(str)
    dfs_merged_act_flightmaster.Sensor_ATD=dfs_merged_act_flightmaster.Sensor_ATD.astype(str)
    dfs_merged_act_flightmaster.On_Block_Time=np.where(dfs_merged_act_flightmaster.On_Block_Time.isna() | dfs_merged_act_flightmaster.On_Block_Time==None, dfs_merged_act_flightmaster.ATA, dfs_merged_act_flightmaster.On_Block_Time)
    dfs_merged_act_flightmaster.On_Block_Time=np.where(dfs_merged_act_flightmaster.On_Block_Time.isna() | dfs_merged_act_flightmaster.On_Block_Time==None, dfs_merged_act_flightmaster.Sensor_ATA, dfs_merged_act_flightmaster.On_Block_Time)
    dfs_merged_act_flightmaster.On_Block_Time=np.where(dfs_merged_act_flightmaster.On_Block_Time.isna() | dfs_merged_act_flightmaster.On_Block_Time==None, dfs_merged_act_flightmaster.STA, dfs_merged_act_flightmaster.On_Block_Time)
    dfs_merged_act_flightmaster.Off_Block_Time=np.where(dfs_merged_act_flightmaster.Off_Block_Time.isna() | dfs_merged_act_flightmaster.Off_Block_Time==None, dfs_merged_act_flightmaster.ATD, dfs_merged_act_flightmaster.Off_Block_Time)
    dfs_merged_act_flightmaster.Off_Block_Time=np.where(dfs_merged_act_flightmaster.Off_Block_Time.isna() | dfs_merged_act_flightmaster.Off_Block_Time==None, dfs_merged_act_flightmaster.Sensor_ATD, dfs_merged_act_flightmaster.Off_Block_Time)
    dfs_merged_act_flightmaster.STD=pd.to_datetime(dfs_merged_act_flightmaster.STD)
    dfs_merged_act_flightmaster.Off_Block_Time=np.where((dfs_merged_act_flightmaster.Off_Block_Time.isna()) & (dfs_merged_act_flightmaster.STD<iu_min_date), dfs_merged_act_flightmaster.STD, dfs_merged_act_flightmaster.Off_Block_Time)
    
    for column in dfs_merged_act_flightmaster:

        dfs_merged_act_flightmaster[column]=dfs_merged_act_flightmaster[column].apply(lambda x : "" if x=='NaT' or x == 'None' else x )

    
    dfs_merged_act_flightmaster.Off_Block_Time=np.where((pd.to_datetime(dfs_merged_act_flightmaster.Off_Block_Time)-pd.to_datetime(dfs_merged_act_flightmaster.On_Block_Time)).dt.days>1, dfs_merged_act_flightmaster.Sensor_ATD, dfs_merged_act_flightmaster.Off_Block_Time)
    dfs_merged_act_flightmaster.Off_Block_Time=np.where((pd.to_datetime(dfs_merged_act_flightmaster.Off_Block_Time)-pd.to_datetime(dfs_merged_act_flightmaster.On_Block_Time)).dt.days>1, dfs_merged_act_flightmaster.ATD, dfs_merged_act_flightmaster.Off_Block_Time)
    # dfs_merged_act_flightmaster=dfs_merged_act_flightmaster[~(dfs_merged_act_flightmaster.On_Block_Time=='None')]
    dfs_merged_act_flightmaster.On_Block_Time=np.where(pd.to_datetime(dfs_merged_act_flightmaster.On_Block_Time).dt.day<iu_min_date.day,
                                          pd.datetime(iu_min_date.year, iu_min_date.month, iu_min_date.day, 0, 0, 1),
                                          dfs_merged_act_flightmaster.On_Block_Time)
    # dfs_merged_act_flightmaster=dfs_merged_act_flightmaster[~(dfs_merged_act_flightmaster.Off_Block_Time=='None')]
    dfs_merged_act_flightmaster.Off_Block_Time=np.where(pd.to_datetime(dfs_merged_act_flightmaster.Off_Block_Time).dt.day>iu_max_date.day,
                                        pd.datetime(iu_max_date.year, iu_max_date.month, iu_max_date.day, 23, 59, 59),
                                        dfs_merged_act_flightmaster.Off_Block_Time)
    dfs_merged_act_flightmaster=dfs_merged_act_flightmaster.drop_duplicates(subset=['LogId'])
    # print(f"Num records {str(dfs_merged_act_flightmaster[dfs_merged_act_flightmaster.Bay=='A10'].shape[0])}")
    dfs_merged_act_flightmaster=dfs_merged_act_flightmaster.sort_values(by=['Bay', 'On_Block_Time'])
    dfs_merged_act_flightmaster.On_Block_Time=pd.to_datetime(dfs_merged_act_flightmaster.On_Block_Time)
    dfs_merged_act_flightmaster.Off_Block_Time=pd.to_datetime(dfs_merged_act_flightmaster.Off_Block_Time)
    
    
    
    
    
    
    
    dfs_merged_act_flightmaster['buffer']=np.where(dfs_merged_act_flightmaster.Bay==dfs_merged_act_flightmaster.Bay.shift(1),
                                        dfs_merged_act_flightmaster.On_Block_Time-dfs_merged_act_flightmaster.Off_Block_Time.shift(1),
                                        dfs_merged_act_flightmaster.On_Block_Time-pd.datetime(iu_min_date.year, iu_min_date.month, iu_min_date.day, 0, 0, 1))
    dfs_merged_act_flightmaster["buffer"].dropna(inplace=True)

    
    dfs_merged_act_flightmaster.buffer=dfs_merged_act_flightmaster["buffer"].astype(int)/60000000000
    dfs_merged_act_flightmaster['tail_buffer']=np.where(dfs_merged_act_flightmaster.Bay!=dfs_merged_act_flightmaster.Bay.shift(-1),pd.datetime(iu_max_date.year, iu_max_date.month, iu_max_date.day, 23, 59, 59)-(pd.to_datetime(dfs_merged_act_flightmaster.Off_Block_Time)),0)
    
    dfs_merged_act_flightmaster["tail_buffer"].dropna(inplace=True)


    dfs_merged_act_flightmaster.tail_buffer=dfs_merged_act_flightmaster["tail_buffer"].astype(int)/60000000000
    
    dfs_merged_act_flightmaster_with_buffers=dfs_merged_act_flightmaster
    
    dfs_merged_act_flightmaster_with_buffers["total_buffer"]=dfs_merged_act_flightmaster.buffer+dfs_merged_act_flightmaster.tail_buffer
    overall_actual_min_buffer=round(dfs_merged_act_flightmaster_with_buffers[dfs_merged_act_flightmaster_with_buffers.total_buffer>0]['total_buffer'].min())
    overall_actual_max_buffer=round(dfs_merged_act_flightmaster_with_buffers.total_buffer.max())
    overall_actual_overlap=round(100*len(dfs_merged_act_flightmaster_with_buffers[~ (dfs_merged_act_flightmaster_with_buffers.buffer.isna()) & (dfs_merged_act_flightmaster_with_buffers.buffer<0)] )/len(dfs_merged_act_flightmaster_with_buffers.LogId))
    dfs_merged_act_flightmaster_with_buffers.Off_Block_Time=np.where(dfs_merged_act_flightmaster_with_buffers.buffer.shift(-1)<0, dfs_merged_act_flightmaster_with_buffers.On_Block_Time.shift(-1), dfs_merged_act_flightmaster_with_buffers.Off_Block_Time)
    if not operation_unit=='22':
        dfs_merged_act_flightmaster_with_buffers['FlightType'] = 'Domestic'
        dfs_merged_act_flightmaster_with_buffers_filtered=dfs_merged_act_flightmaster_with_buffers[['Bay','On_Block_Time', 'Off_Block_Time','LogId','FlightNumber_Arrival', 'FlightNumber_Departure','FlightType',"ToAirport_IATACode", "FromAirport_IATACode", "TerminalArrival"]]
    else :
        dfs_merged_act_flightmaster_with_buffers_filtered=dfs_merged_act_flightmaster_with_buffers[['Bay','On_Block_Time', 'Off_Block_Time','LogId','FlightNumber_Arrival', 'FlightNumber_Departure','FlightType',"ToAirport_IATACode", "FromAirport_IATACode", "TerminalArrival"]]   
        
    #dfs_merged_act_flightmaster_with_buffers_filtered=dfs_merged_act_flightmaster_with_buffers[['Bay','On_Block_Time', 'Off_Block_Time','LogId','FlightNumber_Arrival', 'FlightNumber_Departure','FlightType',"ToAirport_IATACode", "FromAirport_IATACode", "TerminalArrival"]]
    #print('dat333',dfs_merged_act_flightmaster_with_buffers)
    dfs_merged_act_flightmaster_with_buffers_filtered=dfs_merged_act_flightmaster_with_buffers_filtered[pd.to_datetime(dfs_merged_act_flightmaster_with_buffers_filtered.Off_Block_Time).dt.day.isin([ts.day for ts in ts_range[1:-1]])]
    #print('dat334',dfs_merged_act_flightmaster_with_buffers)
    # print(f"Num records {str(dfs_merged_act_flightmaster_with_buffers_filtered[dfs_merged_act_flightmaster_with_buffers_filtered.Bay=='A10'].shape[0])}")
    dfs_merged_act_flightmaster_with_buffers_filtered=dfs_merged_act_flightmaster_with_buffers_filtered[pd.to_datetime(dfs_merged_act_flightmaster_with_buffers_filtered.Off_Block_Time)>pd.to_datetime(dfs_merged_act_flightmaster_with_buffers_filtered.On_Block_Time)]
    # print(f"Num records {str(dfs_merged_act_flightmaster_with_buffers_filtered[dfs_merged_act_flightmaster_with_buffers_filtered.Bay=='A10'].shape[0])}")
    if operation_unit=='22':
        dfs_merged_act_flightmaster_with_buffers_filtered=dfs_merged_act_flightmaster_with_buffers_filtered[~dfs_merged_act_flightmaster_with_buffers_filtered.TerminalArrival.isin(['F','X','Z'])]
    # print(f"Num records {str(dfs_merged_act_flightmaster_with_buffers_filtered[dfs_merged_act_flightmaster_with_buffers_filtered.Bay=='A10'].shape[0])}")
    long_actual=pd.merge(dfs_merged_act_flightmaster_with_buffers_filtered, dfs_merged_act_flightmaster_with_buffers[["LogId","BodyType"]], on="LogId")
    long_actual=pd.merge(long_actual, city_airport_master, right_on="IATACode", left_on="ToAirport_IATACode")
    # print(f"Num records {str(long_actual[long_actual.Bay=='A10'].shape[0])}")
    long_actual.rename(columns={'City':'To_City'}, inplace=True)
    long_actual=pd.merge(long_actual, city_airport_master, right_on="IATACode", left_on="FromAirport_IATACode")
    # print(f"Num records {str(long_actual[long_actual.Bay=='A10'].shape[0])}")
    long_actual.rename(columns={'City':'From_City'}, inplace=True)
    long_actual.On_Block_Time=long_actual.On_Block_Time.astype(str)
    long_actual.Off_Block_Time=long_actual.Off_Block_Time.astype(str)
    long_actual.rename(columns={'On_Block_Time':'ATA','Off_Block_Time':'ATD'}, inplace=True)
    long_actual.loc[long_actual.BodyType=="Bombardier",'BodyType']="Turboprop"
    long_actual.loc[long_actual.BodyType=="", "BodyType"]=np.nan
    long_actual.drop(['FlightType','FromAirport_IATACode','ToAirport_IATACode','IATACode_x','IATACode_y'], axis=1, inplace=True)
    if operation_unit=='4' :
        long_actual['BayType']=np.where(long_actual.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
    else:
        long_actual['BayType']=np.where(long_actual.Bay.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')

        
    
    
#     if not operation_unit=='22':
#         long_actual['BayType']=np.where(long_actual.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
    print("testiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",long_actual)
    long_actual_json=long_actual.to_json(orient='split')
    
    
    # # ============================================================================
    # # |                                                                           |
    # # |                      Actual Bay Overall utilization                       |
    # # |                                                                           |
    # # ============================================================================
    dfs_merged_act_flightmaster_with_buffers_filtered["util"]=pd.to_datetime(dfs_merged_act_flightmaster_with_buffers_filtered.Off_Block_Time) - pd.to_datetime(dfs_merged_act_flightmaster_with_buffers_filtered.On_Block_Time)
    dfs_merged_act_flightmaster_with_buffers_filtered.util=round(dfs_merged_act_flightmaster_with_buffers_filtered.util.dt.seconds/60)
    All_actual_max_buffer = 0
    All_actual_min_buffer = float('inf')
    All_actual_overlap = 0
    for terminal in dfs_merged_act_flightmaster_with_buffers_filtered.TerminalArrival.unique():
        if terminal == 'T1' or terminal == 'T2' or terminal == 'T3':
            total_flights = None
            remote_flights_per = None
            connected_flights_per = None
            terminal_aggregates={}
            dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise=dfs_merged_act_flightmaster_with_buffers_filtered[dfs_merged_act_flightmaster_with_buffers_filtered.TerminalArrival==terminal]
            dfs_merged_act_flightmaster_with_buffers_temp=dfs_merged_act_flightmaster_with_buffers[dfs_merged_act_flightmaster_with_buffers.TerminalArrival==terminal]
        
            
            actual_min_buffer= 0 if np.isnan(dfs_merged_act_flightmaster_with_buffers_temp[(dfs_merged_act_flightmaster_with_buffers_temp.total_buffer>0)]['total_buffer'].min()) else round(dfs_merged_act_flightmaster_with_buffers_temp[(dfs_merged_act_flightmaster_with_buffers_temp.total_buffer>0)]['total_buffer'].min())
            if actual_min_buffer < All_actual_min_buffer:
                All_actual_min_buffer = actual_min_buffer
            actual_max_buffer= 0 if np.isnan(dfs_merged_act_flightmaster_with_buffers_temp['total_buffer'].max()) else round(dfs_merged_act_flightmaster_with_buffers_temp['total_buffer'].max())
        
            if actual_max_buffer > All_actual_max_buffer:
                All_actual_max_buffer=actual_max_buffer
            actual_overlap= 0 if dfs_merged_act_flightmaster_with_buffers_temp.LogId.shape[0]==0 else round(100*len(dfs_merged_act_flightmaster_with_buffers_temp[~ (dfs_merged_act_flightmaster_with_buffers_temp.buffer.isna()) & (dfs_merged_act_flightmaster_with_buffers_temp.buffer<0)] )/dfs_merged_act_flightmaster_with_buffers_temp.LogId.shape[0])
            if actual_overlap > All_actual_overlap:
                All_actual_overlap=actual_overlap
                
            
            print("fffffffffffffff",actual_max_buffer ,actual_min_buffer, actual_overlap)
            if operation_unit=='22':
                dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise.TerminalArrival.isin(['T3']),'Connected','Remote')
                dfs_merged_act_flightmaster_with_buffers_filtered_temp=pd.merge(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise, dfs_merged_act_flightmaster_with_buffers[["LogId","BodyType"]], on="LogId")
                overall_actual_utilization=dfs_merged_act_flightmaster_with_buffers_filtered_temp.groupby(['Bay','BayType','BodyType','FlightType', "TerminalArrival"]).agg({'util':'sum'}).reset_index()
                overall_actual_utilization['percent_util']=round(100*overall_actual_utilization.util/1440)
                overall_actual_utilization_for_remote=-1 if np.isnan(overall_actual_utilization[overall_actual_utilization.BayType=='Remote']['percent_util'].mean()) else overall_actual_utilization[overall_actual_utilization.BayType=='Remote']['percent_util'].mean()
                overall_actual_utilization_for_connected=-1 if np.isnan(overall_actual_utilization[overall_actual_utilization.BayType=='Connected']['percent_util'].mean()) else overall_actual_utilization[overall_actual_utilization.BayType=='Connected']['percent_util'].mean()
                overall_actual_utilization.drop("BayType", axis=1, inplace=True)
                actual_total_turnaround_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util<240]['util'].sum()
                actual_total_parking_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util>240]['util'].sum()
                terminal_aggregates['actual_total_turnaround_time']=-1 if np.isnan(actual_total_turnaround_time) else actual_total_turnaround_time
                terminal_aggregates['actual_total_parking_time']= -1 if np.isnan(actual_total_parking_time) else actual_total_parking_time
                terminal_aggregates['overall_actual_utilization_for_remote']= -1 if np.isnan(overall_actual_utilization_for_remote) else overall_actual_utilization_for_remote
                terminal_aggregates['overall_actual_utilization_for_connected']=-1 if np.isnan(overall_actual_utilization_for_connected) else overall_actual_utilization_for_connected
                terminal_aggregates['actual_min_buffer']=-1 if np.isnan(actual_min_buffer) else actual_min_buffer
                terminal_aggregates['actual_max_buffer']=-1 if np.isnan(actual_max_buffer) else actual_max_buffer
                terminal_aggregates['actual_overlap']=-1 if np.isnan(actual_overlap) else actual_overlap
                total_flights = len(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType'])
                terminal_aggregates['totalFlights'] = -1 if np.isnan(total_flights) else total_flights
                total_dataframe_flights = dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType'].value_counts()
                if 'Remote' in total_dataframe_flights:
                    remote_flights_per = total_dataframe_flights.values[0]
                    remote_flights_per = round(remote_flights_per*100/total_flights)
                    terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
                if 'Connected' in total_dataframe_flights:
                    connected_flights_per = total_dataframe_flights.values[0]
                    connected_flights_per = round(connected_flights_per*100/total_flights)
                    terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
            
        else:
            
            total_flights = None
            remote_flights_per = None
            connected_flights_per = None
            terminal_aggregates={}
            dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise=dfs_merged_act_flightmaster_with_buffers_filtered[dfs_merged_act_flightmaster_with_buffers_filtered.TerminalArrival==terminal]
            dfs_merged_act_flightmaster_with_buffers_temp=dfs_merged_act_flightmaster_with_buffers[dfs_merged_act_flightmaster_with_buffers.TerminalArrival==terminal]
            #dfs_merged_act_flightmaster_with_buffers_temp.to_csv("data4.csv")
            
            actual_min_buffer= 0 if np.isnan(dfs_merged_act_flightmaster_with_buffers_temp[(dfs_merged_act_flightmaster_with_buffers_temp.total_buffer>0)]['total_buffer'].min()) else round(dfs_merged_act_flightmaster_with_buffers_temp[(dfs_merged_act_flightmaster_with_buffers_temp.total_buffer>0)]['total_buffer'].min())
            if actual_min_buffer < All_actual_min_buffer:
                All_actual_min_buffer = actual_min_buffer
            actual_max_buffer= 0 if np.isnan(dfs_merged_act_flightmaster_with_buffers_temp['total_buffer'].max()) else round(dfs_merged_act_flightmaster_with_buffers_temp['total_buffer'].max())
        
            if actual_max_buffer > All_actual_max_buffer:
                All_actual_max_buffer=actual_max_buffer
            actual_overlap= 0 if dfs_merged_act_flightmaster_with_buffers_temp.LogId.shape[0]==0 else round(100*len(dfs_merged_act_flightmaster_with_buffers_temp[~ (dfs_merged_act_flightmaster_with_buffers_temp.buffer.isna()) & (dfs_merged_act_flightmaster_with_buffers_temp.buffer<0)] )/dfs_merged_act_flightmaster_with_buffers_temp.LogId.shape[0])
            if actual_overlap > All_actual_overlap:
                All_actual_overlap=actual_overlap
            
            
            print("bangalore",actual_max_buffer ,actual_min_buffer, actual_overlap)
            if operation_unit=='13' or operation_unit == '4' :
                
                if operation_unit=='4' :
                    dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
                else :
                    dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise.Bay.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')

                
                
               # dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
                dfs_merged_act_flightmaster_with_buffers_filtered_temp=pd.merge(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise, dfs_merged_act_flightmaster_with_buffers[["LogId","BodyType"]], on="LogId")
                dfs_merged_act_flightmaster_with_buffers_filtered_temp['FlightType'] = 'Domestic'
                overall_actual_utilization=dfs_merged_act_flightmaster_with_buffers_filtered_temp.groupby(['Bay','BayType','BodyType','FlightType', "TerminalArrival"]).agg({'util':'sum'}).reset_index()
                overall_actual_utilization['percent_util']=round(100*overall_actual_utilization.util/1440)
                overall_actual_utilization_for_remote=overall_actual_utilization[overall_actual_utilization.BayType=='Remote']['percent_util'].mean()
                overall_actual_utilization_for_connected=overall_actual_utilization[overall_actual_utilization.BayType=='Connected']['percent_util'].mean()
                overall_actual_utilization.drop("BayType", axis=1, inplace=True)
                actual_total_turnaround_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util<240]['util'].sum()
                actual_total_parking_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util>240]['util'].sum()
                terminal_aggregates['actual_total_turnaround_time']=-1 if np.isnan(actual_total_turnaround_time) else actual_total_turnaround_time
                terminal_aggregates['actual_total_parking_time']=-1 if np.isnan(actual_total_parking_time) else actual_total_parking_time
                terminal_aggregates['overall_actual_utilization_for_remote']=-1 if np.isnan(overall_actual_utilization_for_remote) else overall_actual_utilization_for_remote
                terminal_aggregates['overall_actual_utilization_for_connected']=-1 if np.isnan(overall_actual_utilization_for_connected) else overall_actual_utilization_for_connected
                terminal_aggregates['actual_min_buffer']=-1 if np.isnan(actual_min_buffer) else actual_min_buffer
                terminal_aggregates['actual_max_buffer']=-1 if np.isnan(actual_max_buffer) else actual_max_buffer
                terminal_aggregates['actual_overlap']=-1 if np.isnan(actual_overlap) else actual_overlap
                total_flights = len(dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType'])
                terminal_aggregates['totalFlights'] = -1 if np.isnan(total_flights) else total_flights
                total_dataframe_flights = dfs_merged_act_flightmaster_with_buffers_filtered_terminal_wise['BayType'].value_counts()
                if 'Remote' in total_dataframe_flights:
                    remote_flights_per = total_dataframe_flights.values[0]
                    remote_flights_per = round(remote_flights_per*100/total_flights)
                    terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
                if 'Connected' in total_dataframe_flights:
                    connected_flights_per = total_dataframe_flights.values[0]
                    connected_flights_per = round(connected_flights_per*100/total_flights)
                    terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per

        
        actual_aggregates[terminal]=terminal_aggregates
        
    overall_act_terminal_aggregates={}
    if not operation_unit=='22':
        
        if operation_unit=='4' :
            dfs_merged_act_flightmaster_with_buffers_filtered['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')

        else :
            dfs_merged_act_flightmaster_with_buffers_filtered['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered.Bay.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')

        #dfs_merged_act_flightmaster_with_buffers_filtered['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
        dfs_merged_act_flightmaster_with_buffers_filtered_temp=pd.merge(dfs_merged_act_flightmaster_with_buffers_filtered, dfs_merged_act_flightmaster_with_buffers[["LogId","BodyType"]], on="LogId")
        dfs_merged_act_flightmaster_with_buffers_filtered_temp['FlightType'] = 'Domestic'
        overall_actual_utilization=dfs_merged_act_flightmaster_with_buffers_filtered_temp.groupby(['Bay','BayType','BodyType','FlightType', "TerminalArrival"]).agg({'util':'sum'}).reset_index()
        overall_actual_utilization['percent_util']=round(100*overall_actual_utilization.util/1440)
        overall_actual_utilization_for_remote=overall_actual_utilization[overall_actual_utilization.BayType=='Remote']['percent_util'].mean()
        overall_actual_utilization_for_connected=overall_actual_utilization[overall_actual_utilization.BayType=='Connected']['percent_util'].mean()
        overall_actual_utilization.drop("BayType", axis=1, inplace=True)
        actual_total_turnaround_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util<240]['util'].sum()
        actual_total_parking_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util>240]['util'].sum()
        overall_act_terminal_aggregates['actual_total_turnaround_time']=-1 if np.isnan(actual_total_turnaround_time) else actual_total_turnaround_time
        overall_act_terminal_aggregates['actual_total_parking_time']=-1 if np.isnan(actual_total_parking_time) else actual_total_parking_time
        overall_act_terminal_aggregates['overall_actual_utilization_for_remote']= -1 if np.isnan(overall_actual_utilization_for_remote) else overall_actual_utilization_for_remote
        overall_act_terminal_aggregates['overall_actual_utilization_for_connected']=-1 if np.isnan(overall_actual_utilization_for_connected) else overall_actual_utilization_for_connected
        overall_act_terminal_aggregates['actual_min_buffer']=-1 if np.isnan(All_actual_min_buffer) else All_actual_min_buffer
        overall_act_terminal_aggregates['actual_max_buffer']=-1 if np.isnan(All_actual_max_buffer) else All_actual_max_buffer
        overall_act_terminal_aggregates['actual_overlap']=-1 if np.isnan(All_actual_overlap) else All_actual_overlap
        total_flights = len(dfs_merged_act_flightmaster_with_buffers_filtered.BayType)
        overall_act_terminal_aggregates['totalFlights'] = -1 if np.isnan(total_flights) else total_flights
        total_dataframe_flights = dfs_merged_act_flightmaster_with_buffers_filtered['BayType'].value_counts()
        remote_flights_per = total_dataframe_flights[0]
        remote_flights_per = round(remote_flights_per*100/total_flights)
        connected_flights_per = total_dataframe_flights[1]
        connected_flights_per = round(connected_flights_per*100/total_flights)
        overall_act_terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
        overall_act_terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
    else:
        dfs_merged_act_flightmaster_with_buffers_filtered['BayType']=np.where(dfs_merged_act_flightmaster_with_buffers_filtered.TerminalArrival.isin(['T3']),'Connected','Remote')
        dfs_merged_act_flightmaster_with_buffers_filtered_temp=pd.merge(dfs_merged_act_flightmaster_with_buffers_filtered, dfs_merged_act_flightmaster_with_buffers[["LogId","BodyType"]], on="LogId")
        overall_actual_utilization=dfs_merged_act_flightmaster_with_buffers_filtered_temp.groupby(['Bay','BayType','BodyType','FlightType', "TerminalArrival"]).agg({'util':'sum'}).reset_index()
        overall_actual_utilization['percent_util']=round(100*overall_actual_utilization.util/1440)
        overall_actual_utilization_for_remote=overall_actual_utilization[overall_actual_utilization.BayType=='Remote']['percent_util'].mean()
        overall_actual_utilization_for_connected=overall_actual_utilization[overall_actual_utilization.BayType=='Connected']['percent_util'].mean()
        overall_actual_utilization.drop("BayType", axis=1, inplace=True)
        actual_total_turnaround_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util<240]['util'].sum()
        actual_total_parking_time=dfs_merged_act_flightmaster_with_buffers_filtered_temp[dfs_merged_act_flightmaster_with_buffers_filtered_temp.util>240]['util'].sum()
        overall_act_terminal_aggregates['actual_total_turnaround_time']=-1 if np.isnan(actual_total_turnaround_time) else actual_total_turnaround_time
        overall_act_terminal_aggregates['actual_total_parking_time']=-1 if np.isnan(actual_total_parking_time) else actual_total_parking_time
        overall_act_terminal_aggregates['overall_actual_utilization_for_remote']= -1 if np.isnan(overall_actual_utilization_for_remote) else overall_actual_utilization_for_remote
        overall_act_terminal_aggregates['overall_actual_utilization_for_connected']=-1 if np.isnan(overall_actual_utilization_for_connected) else overall_actual_utilization_for_connected
        overall_act_terminal_aggregates['actual_min_buffer']=-1 if np.isnan(All_actual_min_buffer) else All_actual_min_buffer
        overall_act_terminal_aggregates['actual_max_buffer']=-1 if np.isnan(All_actual_max_buffer) else All_actual_max_buffer
        overall_act_terminal_aggregates['actual_overlap']=-1 if np.isnan(All_actual_overlap) else All_actual_overlap
        total_flights = len(dfs_merged_act_flightmaster_with_buffers_filtered.BayType)
        overall_act_terminal_aggregates['totalFlights'] = -1 if np.isnan(total_flights) else total_flights
        total_dataframe_flights = dfs_merged_act_flightmaster_with_buffers_filtered['BayType'].value_counts()
        remote_flights_per = total_dataframe_flights[0]
        remote_flights_per = round(remote_flights_per*100/total_flights)
        connected_flights_per = total_dataframe_flights[1]
        connected_flights_per = round(connected_flights_per*100/total_flights)
        overall_act_terminal_aggregates['remote_flights_per']=-1 if np.isnan(remote_flights_per) else remote_flights_per
        overall_act_terminal_aggregates['connected_flights_per']=-1 if np.isnan(connected_flights_per) else connected_flights_per
    actual_aggregates['All']=overall_act_terminal_aggregates
    
    
    # #=======================Merging overall utilization==========================#
    #print("hfhfhfhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
    merged_utilization=pd.merge(overall_scheduled_utilization_, overall_actual_utilization, left_on="PSTA", right_on="Bay")
    #print("jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj",merged_utilization.columns)
    #merged_utilization.to_csv("data5.csv")
    if operation_unit=='4':
        #merged_utilization['FlightType'] = 'Domestic'
        merged_utilization.rename(columns={'util_x':'scheduled_utilization', 'util_y':'actual_utilization',
                                  'percent_util_x':'percent_scheduled_utilization', 'percent_util_y':'percent_actual_utilization','BodyType_y':'BodyType','FlightType_y':'FlightType','TerminalArrival_y':'TerminalArrival'}, inplace=True)
        merged_utilization.drop(["FlightType_x","FlightType_y","PSTA", "TerminalArrival_x","BodyType_x"], axis=1, inplace=True)
        merged_utilization['BayType']=np.where(merged_utilization.Bay.isin(['54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58']),'Connected','Remote')
    elif operation_unit=='13' :
        #merged_utilization['FlightType'] = 'Domestic'
        merged_utilization.rename(columns={'util_x':'scheduled_utilization', 'util_y':'actual_utilization',
                                  'percent_util_x':'percent_scheduled_utilization', 'percent_util_y':'percent_actual_utilization','BodyType_y':'BodyType','FlightType_y':'FlightType','TerminalArrival_y':'TerminalArrival'}, inplace=True)
        merged_utilization.drop(["FlightType_x","PSTA", "TerminalArrival_x","BodyType_x"], axis=1, inplace=True)
        merged_utilization['BayType']=np.where(merged_utilization.Bay.isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22']),'Connected','Remote')
        #merged_utilization.to_csv("data5.csv")
    else:
        merged_utilization.rename(columns={'util_x':'scheduled_utilization', 'util_y':'actual_utilization',
                                  'percent_util_x':'percent_scheduled_utilization', 'percent_util_y':'percent_actual_utilization','BodyType_y':'BodyType','FlightType_y':'FlightType','TerminalArrival_y':'TerminalArrival'}, inplace=True)
        merged_utilization.drop(["FlightType_x","PSTA", "TerminalArrival_x","BodyType_x"], axis=1, inplace=True)
        merged_utilization['BayType']=np.where(merged_utilization.TerminalArrival.isin(['T3']),'Connected','Remote')

        
    # merged_utilization.percent_actual_utilization=round(merged_utilization.percent_actual_utilization/float(days_diff))
    # merged_utilization.percent_scheduled_utilization=round(merged_utilization.percent_scheduled_utilization/float(days_diff))
    merged_utilization_json=merged_utilization.to_json(orient='split')
    
    # ####################################################################################
    # ##                                                                                ##
    # ##                               COUNT OF FLIGHTS                                 ##
    # ##                                                                                ##
    # ####################################################################################
    
    num_flights=dfs_merged_act_flightmaster_with_buffers_filtered.groupby("Bay").count().reset_index()
    num_flights=num_flights[['Bay', 'LogId']]
    num_flights.rename(columns={'LogId':'num_flights'}, inplace=True)
    num_flights=num_flights[~num_flights.Bay.isna()]
    num_flights_json=num_flights.to_json(orient='split')
    
    
    return {
        'Schedule_Bay_Util':json.loads(long_scheduled_json),
        'Actual_Bay_Util':json.loads(long_actual_json),
        'Overall_Bay_Util':json.loads(merged_utilization_json),
        'Num_Flights':json.loads(num_flights_json),
        'actual_bu_aggregates':actual_aggregates,
        'scheduled_bu_aggregates':scheduled_aggregates
    }
