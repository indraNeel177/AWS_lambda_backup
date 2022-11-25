import json
import boto3
import pymysql
import pandas as pd
user = 'dheeraj_tbb'
password = 'DSHW6wNx7e46uSNK'
client = boto3.client('rds')
def lambda_handler(event, context):
    conn = pymysql.connect(host='avileapuat.ckfsniqh1gly.us-west-2.rds.amazonaws.com', port=3306, user=user, passwd=password, db='AviLeap', connect_timeout=5)
     
    df1= pd.read_sql("""
     /*3.1.1.sql Airport*/
    /*Total flights served by each equipment*/
    select DevId,Flight_PK as FlightServed from EquipActivityLogs 
    where date(ADDTIME(SLogDate, '05:30:00')) =date(ADDTIME(now(), '05:30:00')) and Assigned=1""", con=conn)
     
    df1['DevType'] = df1['DevId'].str.slice(8,11)
    df1=df1.groupby(['DevId','DevType']).agg({"FlightServed": "nunique"}).reset_index()
     
    df2= pd.read_sql("select ShortDevName, DevId from EquipmentMaster", con=conn)
    df3 = pd.merge(df1, df2, on='DevId', how='left').to_json(orient='split')
     
    return {
        'statusCode': 200,
        'Flights_By_Each_EQP_Airport': json.loads(df3)
        }
