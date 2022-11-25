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
    obj = s3.Object('tbb-temp-research-bucket', 'db_creds_prod.ini')
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
    OpName = 4
    data= pd.read_sql(f"""
     select t1.* from (
select FLogDate, Devid, Type, FRefId, TRefId, operationName, Duration, assigned
     from EquipActivityLogs where date(FLogDate)=date(ADDTIME(now(), '05:30:00'))
)
  as t1 left join 
(select * from EquipmentMaster) as t2 on t1.Devid=t2.DevId where t2.OperationUnit={OpName}
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
    dd2= data.Devid.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
    data['DeviceName'] =dd2.devname1.str[1:]
    data.TRefId=data.TRefId.fillna('unknown')
    data.FRefId=data.TRefId.fillna('unknown')
    data["param"]=np.where((data.DeviceName=='COA') & (data.Type=='Movement') & (((data.FRefId.str.startswith('PickUp')) & ~(data.TRefId.str.startswith('PickUp'))) | ((data.FRefId.str.startswith('Drop')) & ~(data.TRefId.str.startswith('Drop'))) | ((data.TRefId.str.startswith('Drop')) & ~(data.FRefId.str.startswith('Drop'))) | ((data.TRefId.str.startswith('PickUp')) & ~(data.FRefId.str.startswith('PickUp')))),1,np.where((data.DeviceName=='ETT') & (data.operationName.isin(['BBF','BBL','FTD','LTD','ITD','BBI'])),1,np.where((data.DeviceName!='COA') & (data.DeviceName!='ETT') & (data.assigned==1) & (data.Duration >30),1,0)))
    grouped_data=data.groupby(['hour_of_day', 'DeviceName']).agg({'param':'sum'}).reset_index()
    grouped_data.rename(columns={'param':'Total'}, inplace=True)
    Movement_Trips_JSON=grouped_data.to_json(orient='split')
    print("Time reqd for movement trips is {}".format(str(datetime.datetime.now()-start)))
    ####################################################################################################################
    #                                                                                                                  #
    #                                               Avg_Ignition_ON_time                                               #
    #                                                                                                                  #
    ####################################################################################################################
    start=datetime.datetime.now()
    data1=data[data.Type=='ig_on']
    grouped_data1=data.groupby(['Devid']).agg({'Duration':'sum'}).reset_index()
    grouped_data1.Duration=grouped_data1.Duration/3600
    avg_ignition_on_time=grouped_data1.Duration.sum()/grouped_data1.Devid.nunique()
    print("Time reqd till final avg ignition time calculation  is {}".format(str(datetime.datetime.now()-start)))
    Avg_Ignition_ON_time_JSON={
            "columns": [
              "avg_ignition_on_time"
            ],
            "index": [
              0
            ],
            "data": [
              [
                avg_ignition_on_time
              ]
            ]
          }


    return {
        'statusCode': 200,
        'Movement_Trips':json.loads(Movement_Trips_JSON),
        'Avg_Ignition_ON_time':Avg_Ignition_ON_time_JSON
    }
