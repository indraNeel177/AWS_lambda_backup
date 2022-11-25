import json
import pandas as pd
import pymysql
import datetime
import numpy as np
import configparser
import boto3
import io


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
    start=datetime.datetime.now() 


    OpName=event['OperationUnit']
    Airline= event['Airline']
    yesterday= event['yesterday']
    #yesterday = "True"

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    
    
    equipments = event['equipments']
    #equipments=""
    
    equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    
    equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"
    
    flights = event['flights']
    
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"

    condition1 = '' if Airline=="" else f" where devid like 'SG%'"
    data= pd.read_sql(f"""
    select * from (
    select t1.*,t2.OperationUnit from (
    select Flight_PK,FLogDate,Devid,Type,FRefId,TRefId, 
    operationName,assigned
    from EquipActivityLogs where date(FLogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} {equipment_filter_condition_1}
    and type = 'Movement' and TIMESTAMPDIFF(SECOND ,FLogDate,TLogDate) > 30) as t1 left join 
    (select devid, OperationUnit from EquipmentMaster) as t2 on t1.Devid = t2.DevId where t2.OperationUnit={OpName}
    )as t3 left join (select LogId,Airline from DailyFlightSchedule_Merged where date(On_block_time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    {airline_filter_condition}) as t4 
    on t3.Flight_PK=t4.LogId {condition1}
    """,con=conn)
    print("Time reqd for query execution is {}".format(str(datetime.datetime.now()-start)))
    ####################################################################################################################
    #                                                                                                                  #
    #                                                   Movement_Trips                                                 #
    #                                                                                                                  #
    #################################################################################################################### 
    
    start=datetime.datetime.now() 
    data["FLogDate"]=pd.to_datetime(data.FLogDate)
    data["hour_of_day"]=data.FLogDate.dt.hour
    #data["DeviceName"]=data.Devid.str.slice(8,11)
    #data["param"]=np.where((data.DeviceName=='COA') & (data.Type=='Move') & (((data.FRefId.str.startswith('PickUp')) & ~(data.TRefId.str.startswith('PickUp'))) | ((data.FRefId.str.startswith('Drop')) & ~(data.TRefId.str.startswith('Drop'))) | ((data.TRefId.str.startswith('Drop')) & ~(data.FRefId.str.startswith('Drop'))) | ((data.TRefId.str.startswith('PickUp')) & ~(data.FRefId.str.startswith('PickUp')))),1,np.where((data.DeviceName=='ETT') & (data.operationName.isin(['BBF','BBL','FTD','LTD','ITD','BBI'])),1,np.where((data.DeviceName!='COA') & (data.DeviceName!='ETT') & (data.assigned==1) & (data.Duration >30),1,0)))
    dd1 = data.Devid.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
    data['DeviceName'] =dd1.devname1.str[1:]
    data.TRefId=data.TRefId.fillna('unknown')
    data.FRefId=data.TRefId.fillna('unknown')
    grouped_data=data.groupby(['hour_of_day', 'DeviceName'])['Devid'].agg({'Total':'count'}).reset_index()
    gg1= grouped_data.replace(r'^\s*$', np.nan, regex=True).fillna('unknown')
    Movement_Trips_JSON=gg1.to_json(orient='split')
    print(data.columns)
    grouped_data_cumsum=data.groupby(['hour_of_day', 'Devid'])['Devid'].agg({'count':'count'}).reset_index()
    grouped_data_cumsum['cumsum'] = grouped_data_cumsum.groupby(['Devid'])['count'].transform(lambda x: x.cumsum())
    grouped_data_cumsum= grouped_data_cumsum.sort_values(by=['hour_of_day', 'Devid'])
    movement_trips_cumsum_json = grouped_data_cumsum.to_json(orient='split')
    
    heatmap_Equip=gg1.groupby(['hour_of_day','DeviceName']).agg('sum').reset_index()
    heatmap_Equip_json= heatmap_Equip.to_json(orient='split')

    

    return {
        'statusCode': 200,
        'Movement_Trips_Airport':json.loads(Movement_Trips_JSON),
        'heatmap_Equip':json.loads(heatmap_Equip_json),
        'movement_trips_cumsum':json.loads(movement_trips_cumsum_json)
        #'Avg_Ignition_ON_time_Airport':Avg_Ignition_ON_time_JSON
    }