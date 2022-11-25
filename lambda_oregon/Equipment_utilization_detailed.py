import json
import boto3
import pymysql
import pandas as pd
import datetime
import configparser
import boto3
import io
import time

#client = boto3.client('rds')
def lambda_handler(event, context):
    s3 = boto3.resource('s3')
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
    
    TypeofUser = event['TypeofUser']
    OpName = event['OperationUnit']
    Airline=event['Airline']
    
    #OpName=4
    #Airline = ""
    yesterday= event['yesterday']
    #OpName=4
    #Airline=""
    #yesterday = "True"
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    
    condition1 = '' if Airline =="" else f"Airline=\'{Airline}\' and "
    
    flights = event['flights']
    
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    equipments = event['equipments']
    #equipments=""
    
    equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    
    equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"
    
    ############################################################Total_Number_Of_Flights#####################################################################################
    df1= pd.read_sql(f"""
    select arrival_hour.hour as hour_of_day,arrival_hour.arrival_flights_count ,depart_flights_count  from (select hour(On_Block_Time) as hour, count(FlightNumber_Arrival) as arrival_flights_count 
    from AviLeap.DailyFlightSchedule_Merged where OperationUnit={OpName} and {condition1} date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition} group by hour(On_Block_Time))
    as arrival_hour left join 
    (select hour(Off_Block_Time) as hour, count(FlightNumber_Departure) as depart_flights_count from AviLeap.DailyFlightSchedule_Merged 
    where {condition1} OperationUnit={OpName} and date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} and Off_Block_Time is not null group by hour(Off_Block_Time))
    as depart_hour on arrival_hour.hour= depart_hour.hour
    """,con=conn)
    
    print(f"""0
    select arrival_hour.hour as hour_of_day,arrival_hour.arrival_flights_count ,depart_flights_count  from (select hour(On_Block_Time) as hour, count(FlightNumber_Arrival) as arrival_flights_count 
    from AviLeap.DailyFlightSchedule_Merged where OperationUnit={OpName} and {condition1} date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition} group by hour(On_Block_Time))
    as arrival_hour left join 
    (select hour(Off_Block_Time) as hour, count(FlightNumber_Departure) as depart_flights_count from AviLeap.DailyFlightSchedule_Merged 
    where {condition1} OperationUnit={OpName} and date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} and Off_Block_Time is not null group by hour(Off_Block_Time))
    as depart_hour on arrival_hour.hour= depart_hour.hour
    """)
    df1_json = df1.to_json(orient='split')
    
    condition2 = '' if Airline =="" else f"and t1.Airline=\'{Airline}\' "
    dd1= pd.read_sql(f"""
    select t2.DevId ,t1.Airline,hour(t2.FLogDate) as hour_of_day, t2.Flight_PK as FlightServed from  (select Airline, logid, OperationUnit 
    from DailyFlightSchedule_Merged) as t1 join      (select distinct Devid,Flight_PK,FLogDate, duration,
    operationname from EquipActivityLogs where FLogDate is not null and Assigned=1 and       date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
    and duration >=30 and operationname not in ('PCG') ) as t2 on      t1.logid= t2.Flight_PK where t1.OperationUnit={OpName}  {equipment_filter_condition} {condition2}
    """,con=conn)
    
    print(f"""
    select t2.DevId ,t1.Airline,t1.OperationUnit,hour(t2.FLogDate) as hour_of_day, t2.Flight_PK as FlightServed 
    from DailyFlightSchedule_Merged as t1 left join 
    (select Devid,Flight_PK,FLogDate, duration,operationname,Assigned from EquipActivityLogs) as t2 on 
    t1.logid= t2.Flight_PK where t2.FLogDate is not null and t2.Assigned=1 and 
     date(t2.FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
     and t2.duration >=30 and t2.operationname not in ('PCG')
     and t1.OperationUnit={OpName} {equipment_filter_condition} {condition2}
    """)

    if dd1.empty:
        dd3_dict = {'DeviceName':['NA'],'hour_of_day':['NA'],'FlightServed':['NA']}
        dd4_dict = {'hour_of_day':['NA'],'DeviceName':['NA'],'DevId':['NA']}
        dd5_dict = {'DeviceName':['NA'],'Eqip_Count':['NA']}
        flightserved_cumsum_dict={'DevId':['NA'],'hour_of_day':['NA'],'FlightServed':['NA'],'cumsum':['NA']}
        dd3_json = pd.DataFrame(dd3_dict).to_json(orient='split')
        dd4_json = pd.DataFrame(dd4_dict).to_json(orient='split')
        dd5_json = pd.DataFrame(dd5_dict).to_json(orient='split')
        flightserved_cumsum_dict_json = pd.DataFrame(flightserved_cumsum_dict).to_json(orient='split')
    else:
        dd2= dd1.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
        dd1['DeviceName'] =dd2.devname1.str[1:]
    
        Devices_to_drop =['BAY','SCHE']
        #'DeviceName','hour_of_day','Airline' can be added to airport data as well for filters
    
        ####################################################Total_Flights_Served_By_Equp_By_Hour############################################################################
        dfFlightServed = dd1[['Airline', 'hour_of_day','FlightServed','DeviceName']].drop_duplicates()
        dd3 = dfFlightServed.groupby(['DeviceName','hour_of_day'])['FlightServed'].agg({'FlightServed':lambda x: x.nunique()}).reset_index() if Airline=="" else \
        dfFlightServed.groupby(['DeviceName','hour_of_day','Airline'])['FlightServed'].agg('count').reset_index()
        #cumilative sum flight served
        flightserved_cumsum = dd1.groupby(['DevId','hour_of_day'])['FlightServed'].agg({'FlightServed':lambda x: x.nunique()}).reset_index() if Airline=="" else \
        dd1.groupby(['DevId','hour_of_day','Airline'])['FlightServed'].agg('count').reset_index()
        flightserved_cumsum['cumsum'] = flightserved_cumsum.groupby(['DevId'])['FlightServed'].transform(lambda x: x.cumsum())
        flightserved_cumsum = flightserved_cumsum.sort_values(by=['hour_of_day','DevId'])
        flightserved_cumsum = flightserved_cumsum[['hour_of_day','DevId','FlightServed', 'cumsum']]
        flightserved_cumsum_dict_json =flightserved_cumsum.to_json(orient='split')
        
        #dd3_final = dd3[['DeviceName','hour_of_day','FlightServed']]
        dd3 = dd3[~dd3.DeviceName.isin(Devices_to_drop)]
        dd3 = dd3.reindex(columns=['DeviceName','hour_of_day','FlightServed','Airline'])
        dd3_json = dd3.to_json(orient='split')
    
        ####################################################Total_Eqip_In_Use#############################################################################################
        dd1_sub = dd1.drop('FlightServed', axis=1)
        # dd4= dd1_sub.groupby(['hour_of_day','DeviceName'])['DevId'].agg({'Eqip_Count':'count'}).reset_index() if Airline=="" else \
        # dd1_sub.groupby(['hour_of_day','DeviceName','Airline'])['DevId'].agg({'Eqip_Count':'count'}).reset_index()
        dd4= dd1_sub.groupby(['hour_of_day','DeviceName'])['DevId'].nunique().reset_index() if Airline=="" else \
        dd1_sub.groupby(['hour_of_day','DeviceName','Airline'])['DevId'].nunique().reset_index()
    
        dd4 = dd4[~dd4.DeviceName.isin(Devices_to_drop)]
        dd4 = dd4.reindex(columns=['DeviceName','hour_of_day','DevId','Airline'])
        dd4_json = dd4.to_json(orient='split')
    
        ####################################################################Equip_Being_Tracked####################################################################3
    
        dd5 = dd1.groupby('DeviceName')['DevId'].nunique().reset_index() if Airline=="" else \
        dd1.groupby(['DeviceName','Airline'])['DevId'].nunique().reset_index()
        dd5.rename(columns={'DevId':'Eqip_Count'},inplace=True)
    
        dd5 = dd5[~dd5.DeviceName.isin(Devices_to_drop)]
        dd5 = dd5.reindex(columns=['DeviceName','Eqip_Count','Airline'])
        dd5_json = dd5.to_json(orient='split')
        
        
    df_ig_on_hour= pd.read_sql(f"""
    select t1.hour_of_day as hour_of_day , t1.devid as Devid,t1.ts from (
    select hour(FLogDate) as hour_of_day, DevId,(TLogDate-FLogDate)/3600 as ts from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Ig_ON' group by hour(FLogDate),DevId order by hour_of_day
    ) as t1 left join 
    (
    select devid,OperationUnit from EquipmentMaster
    )as t2 on t1.devid=t2.devid where t2.operationunit = {OpName} {equipment_filter_condition}
    """,con=conn)
    print(f"""
    select t1.hour_of_day as hour_of_day , t1.devid as Devid,t1.ts from (
    select hour(FLogDate) as hour_of_day, DevId,(TLogDate-FLogDate)/3600 as ts from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Ig_ON' group by hour(FLogDate),DevId order by hour_of_day
    ) as t1 left join 
    (
    select devid,OperationUnit from EquipmentMaster
    )as t2 on t1.devid=t2.devid where t2.operationunit = {OpName} {equipment_filter_condition}
    """)
    if df_ig_on_hour.empty:
        Ignition_heatmap = {'hour_of_day':['NA'],'DevName':['NA'],'count':['NA']}
        ignition_heatmap_json = pd.DataFrame(Ignition_heatmap).to_json(orient='split')
        ignition_cumsum_det={'hour_of_day':['NA'],'Devid':['NA'],'count':['NA'],'cumsum':['NA']}
        ignition_cumsum_det_json = pd.DataFrame(ignition_cumsum_det).to_json(orient='split')
    else:
        #cummilative sum ignition
        df_ig_on_hour['devices'] = df_ig_on_hour.Devid.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
        df_ig_on_hour['DevName'] =df_ig_on_hour.devices.str[1:]
        ignition_cumsum=df_ig_on_hour[['hour_of_day','Devid','ts']]
        ignition_cumsum= ignition_cumsum.sort_values(by='hour_of_day')
        #ignition_cumsum_det=ignition_cumsum.groupby(['hour_of_day','Devid']).agg({'Devid':'count'}).rename(columns={'Devid':'count'}).reset_index()
        ignition_cumsum['cumsum'] = ignition_cumsum.groupby(['Devid'])['ts'].transform(lambda x: x.cumsum()).round(1)
        ignition_cumsum_det=ignition_cumsum.copy()
        ignition_cumsum_det.rename(columns={'ts':'count'},inplace=True)
        ignition_cumsum_det_json = ignition_cumsum_det.to_json(orient='split')
        #ignition heatmap
        df_ig_on_hour = df_ig_on_hour.drop(['Devid','devices'],axis=1)
        df_ig_on_hour=df_ig_on_hour[['hour_of_day','DevName','ts']]
        df_ig_on_hour = df_ig_on_hour.sort_values(by='hour_of_day')
        Ignition_heatmap=df_ig_on_hour.groupby(['hour_of_day','DevName']).agg({'ts':'sum'}).reset_index().round(1)
        Ignition_heatmap.rename(columns={'ts':'count'},inplace=True)
        ignition_heatmap_json = Ignition_heatmap.to_json(orient='split')

    
    
    return {
        'statusCode': 200,
        #"headers": {"Access-Control-Allow-Origin": "*" },
        'Total_Number_Of_Flights': json.loads(df1_json),
        'Total_Flights_Served_By_Equp_By_Hour': json.loads(dd3_json),
        'Total_Eqip_In_Use': json.loads(dd4_json),
        'Equip_Being_Tracked': json.loads(dd5_json),
        'Ignition_heatmap':json.loads(ignition_heatmap_json),
        'Ignition_cumsum':json.loads(ignition_cumsum_det_json),
        'flightserved_cumsum':json.loads(flightserved_cumsum_dict_json)
    }
    
    
    
    # TODO implement
   