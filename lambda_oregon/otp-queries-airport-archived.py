import pandas as pd
from datetime import datetime, timedelta
import time
import io
import s3fs
import json


def lambda_handler(event, context):
    otp_d_a=None
    today=str(datetime.now().date())
    max_date=(datetime.now()-timedelta(1)).date()
    min_date=(datetime.now()-timedelta(31)).date()
    attribute=None
    origin_param=None
    destination_param=None
    body_type_param=None
    grouped_data=None
    airlines=None
    operation_unit=None
    flights_str=None
    flights=[]
    data_path='s3://athena-dumps/lambda-staging/OTP/{}/otp_staging_raw_data.csv'.format(today)
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")+" 00:00:00"
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d %H:%M:%S')
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")+" 23:59:59"
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S')
        if 'attribute' in event.keys() and event.get('attribute')!=None and event.get("attribute")!="":
            attribute=event.get("attribute")
        if 'OperationUnit' in event.keys() and event.get('OperationUnit')!=None and event.get("OperationUnit")!="":
            operation_unit=event.get("OperationUnit")
        if 'airline' in event.keys() and event.get('airline')!=None and event.get("airline")!="":
            airlines=event.get("airline")
        if 'flights' in event.keys() and event.get('flights')!=None and event.get("flights")!="":
            flights_str = event.get('flights')
            flights=[flight for flight in flights_str.split('|')]
        
        
        data=pd.read_csv(data_path)
        data=data[~data.TerminalArrival.isin(['X','Z'])]
        data.TerminalArrival=data.TerminalArrival.fillna('DISABLE')
        # data.OperationUnit=data.OperationUnit.astype(str)
        data=data[data.OperationUnit==float(operation_unit)]
        
        data['initial']=data.flightnumber_arrival.apply(lambda x: x.split(' ')[0])
        if len(flights)>0:
            data=data[data.initial.isin(flights)]
        data.drop('initial', axis=1, inplace=True)
        
        # print(data.bodytype.value_counts())
        print(data.shape)
        if operation_unit == '4' or operation_unit == '13':
            data=data[(pd.to_datetime(data.Sensor_ATA)>min_date) & (pd.to_datetime(data.Sensor_ATA)<max_date)]
        else :
            data=data[(pd.to_datetime(data.ATA)>min_date) & (pd.to_datetime(data.ATA)<max_date)]
        print(data.shape)
        # print(data.bodytype.value_counts())
        if not airlines==None:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
            data=data[data.Airline.isin(airlines)]
        if attribute=='origin':
            data=data[['Airline','OTP_A0', 'OTP_D0', 'OTP_A15', 'OTP_D15','origin', 'destination','flightnumber_arrival','flightnumber_departure','TerminalArrival','from_city','to_city']]
            grouped_data=data.groupby(['Airline', 'origin', 'destination','TerminalArrival']).agg({"OTP_A0":"mean","OTP_A15":"mean","OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
            print("testing",grouped_data)
        if attribute=='Narrow':
            data=data[['Airline','OTP_A0', 'OTP_D0', 'OTP_A15', 'OTP_D15','bodytype','flightnumber_arrival','flightnumber_departure','TerminalArrival','from_city','to_city']]
            data=data[data.bodytype==attribute]
            grouped_data=data.groupby(['Airline','bodytype','TerminalArrival']).agg({"OTP_A0":"mean","OTP_A15":"mean","OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
        if attribute=='TurboProp':
            data=data[['Airline','OTP_A0', 'OTP_D0', 'OTP_A15', 'OTP_D15','bodytype','flightnumber_arrival','flightnumber_departure','TerminalArrival','from_city','to_city']]
            data=data[data.bodytype==attribute]
            grouped_data=data.groupby(['Airline','bodytype','TerminalArrival']).agg({"OTP_A0":"mean","OTP_A15":"mean","OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
            

            
        grouped_data_json=grouped_data.to_json(orient='split')
        data=data.groupby(['Airline','flightnumber_arrival','flightnumber_departure','TerminalArrival','from_city','to_city']).agg({"OTP_A0":"mean","OTP_A15":"mean","OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
        data_json=data.to_json(orient='split')
    
    # TODO implement
    return {
        'statusCode': 200,
        'OTP_Level2': json.loads(grouped_data_json),
        'OTP_Level3':json.loads(data_json)
    }
