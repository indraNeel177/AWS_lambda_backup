import json
import pandas as pd
import pymysql
import configparser
from datetime import datetime
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
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=8)
    
    OpName=event['OperationUnit']
    Airline= event['Airline']
    yesterday= event['yesterday']
    #yesterday = "True"

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""

    #OpName=4
    #Airline=""
    #spicejet hardcoded
    entity=2

    condition1 = '' if Airline=="" else f" and entity='{entity}'"

    equipments = event['equipments']
    #equipments=""
    
    equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    equipment_filter_condition= "" if equipments=="" else f" and t1.devid REGEXP '{equipments}'"
    
    
    #devid ignition on time
    df2 = pd.read_sql(f"""
    select t2.ShortDevName as DevName,t1.hrs,t1.type,t2.operationunit, t2.entity from 
    (select DevId,sum(Duration)/3600 as hrs,if(DevId like '%EBT%','Battery','Diesel') as type from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Ig_ON' group by DevId) as t1 inner join 
    (select ShortDevName, DevId,operationunit,entity from EquipmentMaster) as t2 on t1.devid=t2.devid
    where operationunit={OpName} {equipment_filter_condition} {condition1}
    """,con=conn)
    print(f"""
    select t2.ShortDevName as DevName,t1.hrs,t1.type,t2.operationunit, t2.entity from 
    (select DevId,sum(Duration)/3600 as hrs,if(DevId like '%EBT%','Battery','Diesel') as type from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Ig_ON' group by DevId) as t1 inner join 
    (select ShortDevName, DevId,operationunit,entity from EquipmentMaster) as t2 on t1.devid=t2.devid
    where operationunit={OpName} {equipment_filter_condition} {condition1}
    """)
    df2_json = df2.to_json(orient='split')
    #print("Time taken is {}".format(datetime.datetime.now()-start))
    
    
    df2_movement = pd.read_sql(f"""
    select t1.DevId, t2.ShortDevName as DevName,t1.hrs,t2.operationunit, t2.entity,t1.legsize from 
    (select DevId,sum(TLogDate-FLogDate)/3600 as hrs,legsize from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Movement' group by DevId) as t1 inner join 
    (select ShortDevName, DevId,operationunit,entity from EquipmentMaster) as t2 on t1.devid=t2.devid
    where operationunit={OpName} {equipment_filter_condition} {condition1}
    """,con=conn)
    
    print(f"""
    select t1.DevId, t2.ShortDevName as DevName,t1.hrs,t2.operationunit, t2.entity,t1.legsize from 
    (select DevId,sum(TLogDate-FLogDate)/3600 as hrs,legsize from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Movement' group by DevId) as t1 inner join 
    (select ShortDevName, DevId,operationunit,entity from EquipmentMaster) as t2 on t1.devid=t2.devid
    where operationunit={OpName} {equipment_filter_condition} {condition1}
    """)
    df_movement_ignition = pd.merge(df2,df2_movement, how='inner', on=['DevName','operationunit','entity'])
    print(df_movement_ignition.columns)
    df_movement_ignition['mov_igon_ratio']= df_movement_ignition.hrs_y/(df_movement_ignition.hrs_x + df_movement_ignition.hrs_y)
    df_movement_ignition = df_movement_ignition.round(2)
    df_movement_ignition = df_movement_ignition.rename(columns={'hrs_x':'Ig_on_hrs','hrs_y':'Movement_hrs'})
    #df_final = pd.merge(df_movement_ignition,df3_1,on='DevName',how='inner')
    df_movement_ignition.drop('DevId', axis=1, inplace=True)
    df_movement_ignition['Ig_on_hrs_time']= pd.to_timedelta(round(df_movement_ignition.Ig_on_hrs,1), unit='h')
    #Movement_hrs
    df_movement_ignition['Movement_hrs_time']= pd.to_timedelta(round(df_movement_ignition.Movement_hrs,1), unit='h')
    print("''''''''''''''''''''''''''''''''''''''''")
    print("ignition is ", df_movement_ignition.head())
    df_movement_ignition.mov_igon_ratio = df_movement_ignition.mov_igon_ratio*100
    df_movement_ignition.drop([ 'Ig_on_hrs', 'type', 'operationunit', 'entity','Movement_hrs', 'Ig_on_hrs_time','Movement_hrs_time'], axis=1, inplace=True)
    print(df_movement_ignition.columns)
    df_movement_ignition[(df_movement_ignition.legsize-df_movement_ignition.legsize.mean()).abs() <= (3*df_movement_ignition.legsize.std())]
    
    df_movement_ignition['legsize']=df_movement_ignition['legsize']/1000
    
    df_movement_ignition_json = df_movement_ignition.to_json(orient='split')
    
    
    # print("Time taken is {}".format(datetime.datetime.now()-start))


    #devid and TimeServed
    
    condition2 = '' if Airline=="" else f" and t2.entity='{entity}'"
    df3_1 = pd.read_sql(f"""
     select t2.ShortDevName as DevName,t1.TimeServed from (select sum(Duration)/3600 as TimeServed ,DevId from EquipActivityLogs
    where date(FLogDate) =date(ADDTIME(now(), '05:30:00')){yesterday_condition}
    and type='Ig_ON'
    group by DevId) as t1 left join (select ShortDevName, DevId, OperationUnit,entity from EquipmentMaster) as t2 on t1.DevId=t2.DevId 
    where OperationUnit={OpName}{equipment_filter_condition}  {condition2}
    """,con=conn)
  
    df4 = pd.merge(df2,df3_1,on='DevName',how='inner')
    df4['hrs_time'] = pd.to_timedelta(round(df4.hrs,1), unit='h')
    df4['TimeServed_time']= pd.to_timedelta(round(df4.TimeServed,1), unit='h')
    df4 = round(df4,1)
    df4_json = df4.to_json(orient='split')


    df1= df4.hrs.mean()
    minutes = round(df1*60)
    hour= pd.to_datetime(minutes, unit='m').hour
    minute =pd.to_datetime(minutes, unit='m').minute
    final = pd.DataFrame({'hours':hour, 'minutes':minute}, index=[0])
    df1_json= final.to_json(orient='split')
    # print("Time taken is {}".format(datetime.datetime.now()-start))
    #df3_json = df3.to_json(orient='split')

    return {
      'statusCode': 200,
      'Avg_Ignition_ON_time_Airport':json.loads(df1_json),
      'Ignition_ON_by_device':json.loads(df4_json),
      'Ignition_ON_by_Fuel_Type': json.loads(df2_json),
      'Ignition_Movement_trips':json.loads(df_movement_ignition_json)

    }
