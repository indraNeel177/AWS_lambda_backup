import json
import boto3
import pandas as pd
import numpy as np
import s3fs
from datetime import datetime, timedelta

def lambda_handler(event, context):
    airlines=None
    operation_unit=None
    level=None
    flights=[]
    flights_str=None
    today=str(datetime.now().date())
    max_date=(datetime.now()-timedelta(1)).date()
    min_date=(datetime.now()-timedelta(31)).date()
    today=str(datetime.now().date())
    if event!=None and event!="":
        if 'airline' in event.keys() and event.get('airline')!=None and event.get("airline")!="":
            airlines=event.get("airline")
        if 'OperationUnit' in event.keys() and event.get('OperationUnit')!=None and event.get("OperationUnit")!="":
            operation_unit=event.get("OperationUnit")
        if 'level' in event.keys() and event.get('level')!=None and event.get("level")!="":
            level=event.get("level")
        if 'flights' in event.keys() and event.get('flights')!=None and event.get("flights")!="":
            flights_str = event.get('flights')
            flights=[flight for flight in flights_str.split('|')]
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")+" 00:00:00"
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d %H:%M:%S')
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")+" 23:59:59"
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S')
   
    data_path='s3://athena-dumps/lambda-staging/OTP/{}/otp_staging_raw_data.csv'.format(today)
    data=pd.read_csv(data_path)
    
    data=data[~data.TerminalArrival.isin(['X','Z'])]
    data['initial']=data.flightnumber_arrival.apply(lambda x: x.split(' ')[0])
    if len(flights)>0:
        data=data[data.initial.isin(flights)]
    data.drop('initial', axis=1, inplace=True)
    data = data[data.OperationUnit==float(operation_unit)]
    
    
    if (operation_unit == '4' or operation_unit == '13' ):
        data=data[(pd.to_datetime(data.Sensor_ATA)>=min_date) & (pd.to_datetime(data.Sensor_ATD)<=max_date)]
        
    else :
        data=data[(pd.to_datetime(data.ATA)>=min_date) & (pd.to_datetime(data.ATD)<=max_date)]
        
    #data=data[(pd.to_datetime(data.ATA)>=min_date) & (pd.to_datetime(data.ATD)<=max_date)]
    
    print(data.shape)
    
    
    data.TerminalArrival=data.TerminalArrival.fillna('DISABLE')
    raw_data_json=None
    if level=="airlines":
        if not airlines==None and len(airlines)>0:
                data=data[data.Airline.isin(airlines)]
        print(data.shape)
        grouped_data=data.groupby(['Airline','TerminalArrival']).agg({"OTP_A0":"mean","OTP_A15":"mean","OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
        grouped_data.rename(columns={'ata_month':'month1','ata_day':'day1','OTP_A0':'OTP_A00','OTP_D0':'OTP_D00'}, inplace=True)
        
        grouped_data.OTP_A00=grouped_data.OTP_A00*100
        grouped_data.OTP_A15=grouped_data.OTP_A15*100
        grouped_data.OTP_D00=grouped_data.OTP_D00*100
        grouped_data.OTP_D15=grouped_data.OTP_D15*100
        grouped_data.Airline=np.where(grouped_data.Airline=='Spicejet','SpiceJet',grouped_data.Airline)
        raw_data_json=grouped_data.to_json(orient='split')
        
    elif level=="flights":
        if not airlines==None and len(airlines)>0:
                data=data[data.Airline.isin(airlines)]
        grouped_data=data.groupby(['Airline', 'flightnumber_arrival', 'flightnumber_departure', 'TerminalArrival', 'from_city','to_city']).agg({"OTP_A0":"mean", "OTP_A15":"mean", "OTP_D0":"mean", "OTP_D15":"mean"}).reset_index()
        grouped_data.rename(columns={'ata_month':'month1','ata_day':'day1','OTP_A0':'OTP_A00','OTP_D0':'OTP_D00'}, inplace=True)
        
        grouped_data.OTP_A00=grouped_data.OTP_A00*100
        grouped_data.OTP_A15=grouped_data.OTP_A15*100
        grouped_data.OTP_D00=grouped_data.OTP_D00*100
        grouped_data.OTP_D15=grouped_data.OTP_D15*100
        grouped_data.Airline=np.where(grouped_data.Airline=='Spicejet','SpiceJet',grouped_data.Airline)
        raw_data_json=grouped_data.to_json(orient='split')
        
        
    return {
        'statusCode': 200,
        'raw_data':json.loads(raw_data_json)
    }
