import json
import boto3
import pymysql
import pandas as pd
import datetime
import configparser
import io

#client = boto3.client('rds')
def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod_read_replica.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    #hostname= 'avileap-read.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
    #avileapuat.ckfsniqh1gly.us-west-2.rds.amazonaws.com
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    #hostname ='avileap-dev-readonly.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)

    TypeofUser = event['TypeofUser']
    OpName = event['OperationUnit']
    Airline=event['Airline']
    
    #spicejet hardcoded
    entity=2
  
    condition1 = '' if Airline=="" else f" and Entity={entity}"

    #OpName=4
    #Airline=""
    
    yesterday= event['yesterday']
    #yesterday = "True"

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Departure REGEXP '{flights}'"
    
    equipments = event['equipments']
    equipments_filter_condition = "" if equipments=="" else f" where t2.DevId REGEXP '{equipments}'"
    
    
    #-- Airport={OpName} and 
    
    df1= pd.read_sql(f"""
   select t1.*,t2.ShortDevName from (
  select DevId,Entity from NonCompliance where 
    date(LogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} and lower(type)='overspeed'{condition1} and Airport={OpName}
)    as t1 left join 
    (select ShortDevName, DevId from EquipmentMaster where OperationUnit={OpName}) as t2
    on t1.DevId=t2.DevId {equipments_filter_condition}
    """,con=conn)# if OpName ==4 
    df1_1 =pd.DataFrame({'NonCompliance Devices':df1.DevId.nunique()},index=[0])
    df1_json = df1_1.to_json(orient='split')
    
    print(f"""
   select t1.*,t2.ShortDevName from (
  select DevId,Entity from NonCompliance where 
    date(LogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} and lower(type)='overspeed'{condition1} and Airport={OpName}
)    as t1 left join 
    (select ShortDevName, DevId from EquipmentMaster where OperationUnit={OpName}) as t2
    on t1.DevId=t2.DevId {equipments_filter_condition}
    """)
    
    
    dd1= df1.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
    df1['DevShortName'] =dd1.devname1.str[1:]
    
    #Number of equipments that were non compliant for each type of equipment
    df2 = df1.groupby('DevShortName')['DevId'].nunique().reset_index()
    df2_json = df2.to_json(orient='split')
    
    
    #Number of times each equipment was non-compliant
    
    df3= df1.sort_values(by='DevShortName')
    df3_1 = df3.groupby(['DevShortName','ShortDevName']).agg({'ShortDevName':'count'}).rename(columns={'ShortDevName':'Devid_count'}).reset_index()
    df3_json = df3_1.to_json(orient='split')
    
    
    #Non compliance on hourly basis
    
    equipments_filter_condition_2 = "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    # Airport={OpName} and
    df4 = pd.read_sql(f"""
    select COUNT(distinct DevId) as NonCompliance_devices,HOUR(LogDate) as hour_of_day,Entity from NonCompliance where 
    date(LogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} and 
    lower(type)='overspeed' {condition1} and airport= {OpName} {equipments_filter_condition_2} group by HOUR(LogDate) 
    """,con=conn)
    df4_json = df4.to_json(orient='split')

    
    
    return {
    'statusCode': 200,
    'Noncompliance_Agg': json.loads(df1_json),
    'non compliant_each_type_of_equipment': json.loads(df2_json),
    'non_compliant_Equipments': json.loads(df3_json),
    'Noncompliance_By_Hour': json.loads(df4_json)
    }