import json
import pandas as pd
import pymysql
import boto3
import io
import configparser
import time, datetime

def lambda_handler(event, context):
    #client = boto3.client('rds')
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


    Airline = event['Airline']
    OpName=event['OperationUnit']
    yesterday= event['yesterday']
    #yesterday = "True"
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""

    #AirlineName='SpiceJet'
    #OpName=4
    #start=datetime.datetime.now()
    #condition for spicejet
    
    
    equipments = event['equipments']
    #equipments=""
    equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    equipment_filter_condition= "" if equipments=="" else f" and t1.devid REGEXP '{equipments}'"
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    #entity condition added and hard coded for spicejet
    condition2 = '' if Airline =="" or Airline==None else f"and entity=2 "
    
    df1= pd.read_sql(f"""
    select t3.*,t4.Entity,t4.ShortDevName from (
    select * from (
    select DevId,Flight_PK as FlightServed from EquipActivityLogs 
    where date(ADDTIME(FLogDate, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {equipment_filter_condition_1} and Assigned=1
    and duration >=30 and operationname not in ('PCG')
    ) as t1 left join 
    (
    select LogId, OperationUnit, Airline from DailyFlightSchedule_Merged WHERE
    OperationUnit= {OpName} and date(ADDTIME(On_Block_time, '05:30:00')) =date(ADDTIME(now(), '05:30:00')) {yesterday_condition} {airline_filter_condition}
    )  as t2 on t1.FlightServed =t2.LogId  where t2.OperationUnit={OpName}
    ) as t3 left join (select DevId,Entity,ShortDevName from EquipmentMaster) as t4
    on t3.DevId=t4.DevId where ShortDevName!='BLOCK'
    {condition2}
    """, con=conn)
    
    df1=df1.drop(columns={'LogId','OperationUnit','Airline','Entity'},axis=1)
    
    dd2= df1.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
    df1['DevType'] =dd2.devname1.str[1:]
   
    df1['FlightServed1']=df1.groupby('DevType')['FlightServed'].transform('nunique')
    
    
    
    df2=df1.drop_duplicates(['DevType']).reset_index(drop=True)
    df2.drop('FlightServed',axis=1,inplace=True)
    df2.rename(columns={'FlightServed1':'FlightServed'},inplace=True)
    
    df2= df2[['DevType','FlightServed']]
    
    

    df2_json = df2.to_json(orient='split')
    ####################################################################################################################
    #                                                                                                                  #
    #                                           Flights_By_Each_Type_EQP                                               #
    #                                                                                                                  #
    ####################################################################################################################
    #Flights_By_Each_Type_EQP=df2.groupby('DevType')['FlightServed'].transform('sum')
   # print(Flights_By_Each_Type_EQP
   # Flights_By_Each_Type_EQP.rename(columns={'DevType':'DevName','FlightServed':'Flight_PK'},inplace=True)
   # Flights_By_Each_Type_EQP_json = Flights_By_Each_Type_EQP.to_json(orient='split')



    #data3 = data.copy() if AirlineName =="" else data[data['Airline']==AirlineName]



    return{
        'statusCode': 200,
        
        'Airline_By_Each_Type_EQP': json.loads(df2_json)
    }

