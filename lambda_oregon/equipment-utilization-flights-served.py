import json
import boto3
import pymysql
import pandas as pd
import io
import configparser

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
    devid_to_drop=['BLOCK','Block','BAY 45']
    
    OpName=event['OperationUnit']
    Airline=event['Airline']
    
    yesterday= event['yesterday']
  
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    # /*3.1.1.sql Airport*/
    #/*Total flights served by each equipment*/
    #
    condition1 = '' if Airline =="" or Airline==None else f"and Airline=\'{Airline}\' "
    
    #entity condition added and hard coded for spicejet
    condition2 = '' if Airline =="" or Airline==None else f"and entity=2 "
    
    
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    equipments = event['equipments']
    #equipments=""
    
    equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"
    
    df1= pd.read_sql(f"""
    select t3.*,t4.Entity,t4.ShortDevName from (
select * from (
select DevId,Flight_PK as FlightServed from EquipActivityLogs 
where date(ADDTIME(FLogDate, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {equipment_filter_condition_1} and Assigned=1
and duration >=30 and operationname not in ('PCG','ETT') and operationname not like 'ac:%'
) as t1 left join 
(
select LogId, OperationUnit, Airline from DailyFlightSchedule_Merged WHERE
OperationUnit= {OpName} and date(ADDTIME(On_Block_time, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition}
)  as t2 on t1.FlightServed =t2.LogId  where t2.OperationUnit={OpName}
) as t3 left join (select DevId,Entity,ShortDevName from EquipmentMaster) as t4
on t3.DevId=t4.DevId where ShortDevName!='BLOCK'
 {condition2}
    """, con=conn)
    print(f"""
    select t3.*,t4.Entity,t4.ShortDevName from (
select * from (
select DevId,Flight_PK as FlightServed from EquipActivityLogs 
where date(ADDTIME(FLogDate, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {equipment_filter_condition_1} and Assigned=1
and duration >=30 and operationname not in ('PCG')
) as t1 left join 
(
select LogId, OperationUnit, Airline from DailyFlightSchedule_Merged WHERE
OperationUnit= {OpName} and date(ADDTIME(On_Block_time, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition}
)  as t2 on t1.FlightServed =t2.LogId  where t2.OperationUnit={OpName}
) as t3 left join (select DevId,Entity,ShortDevName from EquipmentMaster) as t4
on t3.DevId=t4.DevId where ShortDevName!='BLOCK'
 {condition2}
    """)
    if df1.empty == False:
    
        df1=df1.drop(columns={'LogId','OperationUnit','Airline','Entity'},axis=1)
        #print("ffffffffffffff",df1['DevId'].unique())transform('nunique')
        #df1=df1.drop_duplicates(['FlightServed']).reset_index(drop=True)
        dd2= df1.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
        df1['DevType'] =dd2.devname1.str[1:]
        df1['FlightServed1']=df1.groupby(['DevId','DevType'])['FlightServed'].transform('nunique')
        df1['EachFlightequip']=df1.groupby(['DevType'])['FlightServed'].transform('nunique')
        
        print("ffffffffffffff",df1['FlightServed1'])
        df2=df1.drop_duplicates(['DevId']).reset_index(drop=True)
        df2.drop('FlightServed',axis=1,inplace=True)
        df2.rename(columns={'FlightServed1':'FlightServed'},inplace=True)
        
        #dd2= df2.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
        #df2['DevType'] =dd2.devname1.str[1:]
        
        df2= df2[['DevId','DevType','FlightServed','ShortDevName','EachFlightequip']]
        df2_json = df2.to_json(orient='split')
        
    else:
        datadf=pd.DataFrame()
        df2_json = datadf.to_json(orient='split')

        
    
    return {
        'statusCode': 200,
        'Flights_By_Each_EQP_Airport': json.loads(df2_json)
        
        }
