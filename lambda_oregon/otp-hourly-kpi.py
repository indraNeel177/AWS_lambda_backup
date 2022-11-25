import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def lambda_handler(event, context):
    today=str(datetime.now().date())
    max_date=(datetime.now()-timedelta(1)).date()
    min_date=(datetime.now()-timedelta(31)).date()
    airlines=None
    origin_param=None
    destination_param=None
    body_type_param=None
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
        if 'airlines' in event.keys() and event.get('airlines')!=None and len(event.get("airlines"))>0:
            airlines=event.get("airlines")
        if ('OperationUnit' in event.keys()) and (event.get("OperationUnit")!=None) and (event.get("OperationUnit")!=""):
            operation_unit=event.get("OperationUnit")
        if 'flights' in event.keys() and event.get('flights')!=None and event.get("flights")!="":
            flights_str = event.get('flights')
            flights=[flight for flight in flights_str.split('|')]
            
        print("Min date is {}".format(str(min_date)))
        print("Max date is {}".format(str(max_date)))
        print("Airlines to filter are {}".format(str(airlines)))
        
        
        data=pd.read_csv(data_path)
        data=data[~data.TerminalArrival.isin(['X','Z'])]
        data=data[data.OperationUnit==float(operation_unit)]
        
        data['initial']=data.flightnumber_arrival.apply(lambda x: x.split(' ')[0])
        if len(flights)>0:
            data=data[data.initial.isin(flights)]
        data.drop('initial', axis=1, inplace=True)
        
        data.TerminalArrival=data.TerminalArrival.fillna("DISABLE")
            
        if airlines!=None:
            if  (operation_unit == '4' or operation_unit == '13' ):
                arrival_data=data[(pd.to_datetime(data.Sensor_ATA)>min_date) & (pd.to_datetime(data.Sensor_ATA)<max_date) & (data.Airline.isin(airlines))]
                arrival_data=arrival_data.groupby(["ata_hour","Airline","TerminalArrival"]).agg({"OTP_A0":"mean","OTP_A15":"mean"}).reset_index()
            
                departure_data=data[(pd.to_datetime(data.Sensor_ATD)>min_date) & (pd.to_datetime(data.Sensor_ATD)<max_date) & (data.Airline.isin(airlines))]
                departure_data=departure_data.groupby(["atd_hour", "Airline","TerminalArrival"]).agg({"OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
            else :
                arrival_data=data[(pd.to_datetime(data.ATA)>min_date) & (pd.to_datetime(data.ATA)<max_date) & (data.Airline.isin(airlines))]
                arrival_data=arrival_data.groupby(["ata_hour","Airline","TerminalArrival"]).agg({"OTP_A0":"mean","OTP_A15":"mean"}).reset_index()
            
                departure_data=data[(pd.to_datetime(data.ATD)>min_date) & (pd.to_datetime(data.ATD)<max_date) & (data.Airline.isin(airlines))]
                departure_data=departure_data.groupby(["atd_hour", "Airline","TerminalArrival"]).agg({"OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
                
        else:
            if  (operation_unit == '4' or operation_unit == '13' ):
                arrival_data=data[(pd.to_datetime(data.Sensor_ATA)>min_date) & (pd.to_datetime(data.Sensor_ATA)<max_date)]
                arrival_data=arrival_data.groupby(["ata_hour","Airline","TerminalArrival"]).agg({"OTP_A0":"mean","OTP_A15":"mean"}).reset_index()
            
                departure_data=data[(pd.to_datetime(data.Sensor_ATD)>min_date) & (pd.to_datetime(data.Sensor_ATD)<max_date)]
                departure_data=departure_data.groupby(["atd_hour", "Airline","TerminalArrival"]).agg({"OTP_D0":"mean","OTP_D15":"mean"}).reset_index()
            else :
                arrival_data=data[(pd.to_datetime(data.ATA)>min_date) & (pd.to_datetime(data.ATA)<max_date)]
                arrival_data=arrival_data.groupby(["ata_hour","Airline","TerminalArrival"]).agg({"OTP_A0":"mean","OTP_A15":"mean"}).reset_index()
            
                departure_data=data[(pd.to_datetime(data.ATD)>min_date) & (pd.to_datetime(data.ATD)<max_date)]
                departure_data=departure_data.groupby(["atd_hour", "Airline","TerminalArrival"]).agg({"OTP_D0":"mean","OTP_D15":"mean"}).reset_index()

                
                
                       
        arrival_data_json=arrival_data.to_json(orient="split")
        departure_data_json=departure_data.to_json(orient="split")
        
        
        
    return {
        'statusCode': 200,
        'OTP_Archived_Arrival':json.loads(arrival_data_json),
        'OTP_Archived_Departure':json.loads(departure_data_json),
        
    }
