import pandas as pd
import pymysql
import json
import numpy as np
import configparser
import boto3
import io
from datetime import timedelta

def lambda_handler(event, context):

    def Arrival_Depature(Depature):
    
        if Depature is None :
            return 'A'
        else :
            return Depature
    
    def ActivityStartEnd(start,end):
    
        if str(end) != 'NaT' :
            return end
        else :
            return start

    
    OpName=int(event['OperationUnit'])
    Airline = event['Airline']
    Min_date=event['Mindate']
    Max_date=event["Maxdate"]
    airline_filter_condition= ""

    
    #airline_filter_condition= "" if OpName!= 13 else "and Airline in ('Air India','Etihad','Singapore Airlines','Alliance Air','Thai Airways','Air Arabia','Cathay Pacific','FlyDubai','Go Air','Jazeera Airways','Silk Air','Vistara')"
        
    connection = pymysql.connect(host='test-vpc-instance.cscnpwfy9ubd.ap-south-1.rds.amazonaws.com',
                                         database='AviLeap',
                                         user='avileap_admin',
                                         password='admin_1q2w#E$R')
    
    #dt=pd.read_sql(f"""select * from TurnaroundEquipment t2 where AirportCode={OpName} {airline_filter_condition} and date(OnBlockTime) = '{Max_date}' and date(OffBlockTime) = '{Max_date}' """,con=connection)
    dt=pd.read_sql(f"""select * from TurnaroundEquipment t2 where AirportCode={OpName} {airline_filter_condition} and date(OnBlockTime) = '{Max_date}' """,con=connection)

    if not dt.empty:


        dt['ActivityStartEnd']=dt.apply(lambda x : ActivityStartEnd(x["ActivityStartTime"],x["ActivityEndTime"]),axis=1)
        dt['ExpectedStartEnd']=dt.apply(lambda x : ActivityStartEnd(x["ExpectedStartTime"],x["ExpectedEndTime"]),axis=1)
        dt['Arrival_Depature']=dt.apply(lambda x : Arrival_Depature(x["EndType"]),axis=1)
        Type = set(dt['Arrival_Depature'])
        Activity=dt[["Airline","ArrivalFlight",'BayType', 'FlightType','BodyType',"OperationName","ActivityStartTime","ActivityEndTime","ExpectedStartTime","ExpectedEndTime"]]
        dt.dropna(subset=['StartType', 'EndType'], how='all',inplace=True)
        dt.dropna(subset=['DelayCode'], how='all',inplace=True)
        
        
        dt['Max_Date']=dt.groupby(['LogId','Arrival_Depature'])['ExpectedStartEnd'].transform(max)
        dt['diff_Max_Log'] = (dt['Max_Date']-dt['ActivityStartEnd']).dt.total_seconds().div(60)
        dt['Flight_status'] = np.where(dt['diff_Max_Log']>=0, 'Ontime','Delay')
        
        total_flights=pd.DataFrame({'Total Flights':[dt['LogId'].count()]},columns=['Total Flights'])


        Count_data=dt.groupby(['Arrival_Depature'])['Flight_status'].agg({"count"}).reset_index()
        total_data=dt.groupby(["Arrival_Depature"])['Flight_status'].agg({"value_counts"}).reset_index()
        Delay_card=pd.merge(total_data, Count_data, how='left',on='Arrival_Depature')
        #Night_halt_Card["percentage"]=(Night_halt_Card["value_counts"]*100/Night_halt_Card['count']).round(2)
        Delay_card=Delay_card[["Arrival_Depature","Flight_status","value_counts"]]
        
        Delay_chart=dt[["DelayCode","ArrivalFlight","Flight_status","Airline","Arrival_Depature","BodyType","BayType","FlightType","AircraftType"]]
        Delay_chart_output=Delay_chart.groupby(["DelayCode","ArrivalFlight","Flight_status","Airline","Arrival_Depature","BodyType","BayType","FlightType","AircraftType"])['DelayCode'].agg({"count"}).reset_index()
        
        Total_flight_data = total_flights.to_json(orient='split')
        Delay_card = Delay_card.to_json(orient='split')
        Delay_data = Delay_chart_output.to_json(orient='split')
        
        ##################Activity ####################
        
        ################ Arrival agg#############
        
        Activity_total_flights=pd.DataFrame({"Total Flights":[Activity['ArrivalFlight'].count()]},columns=['Total Flights'])


    
        Activity['diff_Flog'] = (Activity['ExpectedStartTime']-Activity['ActivityStartTime']).dt.total_seconds().div(60)

        Activity['status_Flog'] = np.where(Activity['diff_Flog']<=0, 'Arrival_Ontime','Arrival_Delay')
        Arrival_count = pd.DataFrame(Activity['status_Flog'].value_counts(normalize=True).round(2) * 100).reset_index()


        ################ departure agg#############



        Activity['diff_Tlog'] = (Activity['ExpectedEndTime']-Activity['ActivityEndTime']).dt.total_seconds().div(60)
        Activity['status_Tlog'] = np.where(Activity['diff_Tlog']<=0, 'Depature_Ontime','Depature_Delay')
        Depature_count  = pd.DataFrame(Activity['status_Tlog'].value_counts(normalize=True).round(2) * 100).reset_index()
        
        
        Activity_card=pd.concat([Arrival_count, Depature_count]) 
        
        Arrival=Activity[['BayType', 'FlightType','BodyType','OperationName','status_Flog']]
        total_data=Arrival.groupby(['BayType', 'FlightType','BodyType','OperationName'])['status_Flog'].agg({"value_counts"}).reset_index()
        Count_data=Arrival.groupby(['BayType', 'FlightType', 'BodyType', 'OperationName'])['status_Flog'].agg({"count"}).reset_index()
        Arrival_out= pd.merge(total_data,Count_data,on=['BayType', 'FlightType', 'BodyType','OperationName'])
        Arrival_out["percentage"]= (Arrival_out['value_counts']*100/Arrival_out['count']).round(2)

        Depature=Activity[['BayType', 'FlightType','BodyType','OperationName','status_Tlog']]
        total_data_D=Depature.groupby(['BayType', 'FlightType','BodyType','OperationName'])['status_Tlog'].agg({"value_counts"}).reset_index()
        Count_data_D=Depature.groupby(['BayType', 'FlightType', 'BodyType', 'OperationName'])['status_Tlog'].agg({"count"}).reset_index()
        Depature_out= pd.merge(total_data_D,Count_data_D,on=['BayType', 'FlightType', 'BodyType','OperationName'])
        Depature_out["percentage"]= (Depature_out['value_counts']*100/Depature_out['count']).round(2)
        
        Activity['Actual_average_duration'] = (Activity['ActivityEndTime']-Activity['ActivityStartTime']).dt.total_seconds().div(60)
        Activity['Expected_average_duration'] = (Activity['ExpectedEndTime']-Activity['ExpectedStartTime']).dt.total_seconds().div(60)

        Activity_charts=Activity[["Airline","ArrivalFlight","Actual_average_duration","Expected_average_duration",'BayType', 'FlightType','BodyType','OperationName']]
        Activity_charts_out=Activity_charts.groupby(["Airline","ArrivalFlight",'BayType', 'FlightType','BodyType','OperationName'])["Actual_average_duration","Expected_average_duration",].agg({"sum"}).reset_index()
        
        Activity_total_flights = Activity_total_flights.to_json(orient='split')
        Activity_card_out = Activity_charts_out.to_json(orient='split')
        Activity_charts_output = Activity_charts_out.to_json(orient='split')

    
        
        return {
            "statusCode": 200,
            'Total_Flight_count_delay' : json.loads(Total_flight_data),
            'Delay_card': json.loads(Delay_card),
            'Delay_data' : json.loads(Delay_data),
            'Activity_total_count': json.loads(Activity_total_flights),
            'Activity_card': json.loads(Activity_card_out),
            'Activity_charts_output' : json.loads(Activity_charts_output)
            
        }


