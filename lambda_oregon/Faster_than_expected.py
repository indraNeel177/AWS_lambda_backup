import json
import pandas as pd
import pymysql
import configparser
import boto3
import io

def lambda_handler(event, context):
    TypeofUser=event['TypeofUser']
    OpName  = int(event['OperationUnit'])# OperationUnit = 4
    Airline = event['Airline']
    yesterday= event['yesterday']
    #yesterday = "True"

    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    
    
    metro_names_hyd = ('HYD','DEL','BLR','BOM')
    connected_bay_names_hyd = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
    #OpName = 4
    #Airline = ""
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Departure REGEXP '{flights}'"
    
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
    # faster than expected
    if OpName == 4:
        DFS_craft = 'Aircraft'
        FM_table = 'Hexcode'
    else :
        DFS_craft = 'AircraftRegistration_Arrival'
        FM_table = 'RegNo'
    df4_parent = pd.read_sql(f"""
    select t1.Airline,t1.{DFS_craft},t1.FTE,t1.FlightNumber_Departure,t2.BodyType from (
    select Airline,{DFS_craft},if(timestampdiff(MINUTE,On_Block_Time,Off_Block_Time)<=timestampdiff(MINUTE,STA,STD),1,0) as FTE
    ,FlightNumber_Departure from DailyFlightSchedule_Merged
    where OperationUnit={OpName} and date(On_Block_Time) =date(ADDTIME(now(), '05:30:00'))) as t1
    left join (select Hexcode,BodyType,REPLACE(RegNo, '-', '') as RegNo  from FlightMaster) as t2 on t1.{DFS_craft} = t2.{FM_table}
    """,con=conn) 
    df4_parent = df4_parent.drop(['Aircraft'],axis=1) if OpName==4 else df4_parent.drop(['AircraftRegistration_Arrival'],axis=1)
    #AirlineName = 'SpiceJet' #event['AirlineName']
    df4_dumm = df4_parent.copy() if Airline =='' else df4_parent[df4_parent['Airline']==Airline]
    #df4_dumm['BodyType']=df4_dumm['FlightNumber_Arrival'].str.len()
    #df4_dumm['BodyType'] = df4_dumm['BodyType'].apply(lambda x: 'Narrow' if x==6 else 'TurboProp')
    df4_dumm.BodyType = df4_dumm.BodyType.replace({'Turboprop':'Bombardier'})
    df5_dumm = df4_dumm.groupby('BodyType')['FTE'].agg('mean').round(2).reset_index()
    data_mean = pd.DataFrame({'BodyType':['All'], 'FTE':round(df4_dumm.FTE.mean(),2)} )
    dd1=pd.DataFrame.append(df5_dumm,data_mean).reset_index(drop=True)
    dd1.FTE = dd1.FTE*100
    dd1_json = dd1.to_json(orient='split')
    df_narrow_json = df4_parent[df4_parent['BodyType'] == 'Narrow'].reset_index(drop=True).to_json(orient='split')
    df_bombardier_json = df4_parent[df4_parent['BodyType'] == 'Bombardier'].reset_index(drop=True).to_json(orient='split')
    df_wide_json = df4_parent[df4_parent['BodyType'] == 'Wide'].reset_index(drop=True).to_json(orient='split')
    
    #final responses
    return {
       'statusCode': 200,
        'PercentFlights_Faster_Than_Expected_Airport': json.loads(dd1_json),
        'Detailed_narrow' : json.loads(df_narrow_json),
        'Detailed_bombardier' : json.loads(df_bombardier_json),
        'Detailed_wide' : json.loads(df_wide_json)
    }