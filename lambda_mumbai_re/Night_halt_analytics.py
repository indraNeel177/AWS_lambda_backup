import pandas as pd
import pymysql
import json
import numpy as np
import configparser
import boto3
import io
from datetime import timedelta
import datetime


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources', 'db_creds_ini/Host_credentials.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = 'prdouctionavileapvpc.cscnpwfy9ubd.ap-south-1.rds.amazonaws.com'
    jdbcPort = 3306
    username = 'avileap_curd'
    password = 'avileap^7t*ALP'
    dbname = 'AviLeap'

    try:
        connection = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname,
                                     connect_timeout=5)
    except Exception as e:
        print("Sql connection error")

    def Arrival_Depature(Depature):

        if Depature is None:
            return 'A'
        else:
            return Depature

    def ActivityStartEnd(start, end):

        if str(end) != 'NaT':
            return end
        else:
            return start

    def Single_Activity_Handle(ADType, start, end):
        if str(start) == 'NaT' or str(end) == 'NaT':
            if str(start) == 'NaT':
                if ADType == 'A':
                    return end + datetime.timedelta(minutes=1)
                else:
                    return end - datetime.timedelta(minutes=1)
            else:
                return start
        else:
            return start

    OpName = int(event['OperationUnit'])
    Airline = event['Airline']
    Min_date = event['Mindate']
    Max_date = event["Maxdate"]

    rangedate = pd.date_range(start=Min_date, end=Max_date)
    count = 0

    Night = pd.DataFrame()
    Night_Turn_Around_time = pd.DataFrame()
    Night_total_flight = pd.DataFrame()
    Night_total_card = pd.DataFrame()
    Night_Start_end_activity_airline = pd.DataFrame()
    Night_Start_end_activity_Flight = pd.DataFrame()
    Night_Aircraft_data_list = pd.DataFrame()

    Airlines_Names = pd.read_sql("""select ai.Name  from AirlineInfo ai left join EntityAirlineMapping eam on eam.AirlineId = ai.Id where eam.Status =1
                             and eam.isDeleted =0 and ai.isDeleted =0 and eam.AirportId ={} and eam.EntityId ={}""".format(
        OpName, 9), con=connection)
    airline_filter_condition = """ and Airline in {}""".format(tuple(list(Airlines_Names['Name'])))

    # airline_filter_condition= "" if OpName!= 13 else "and Airline in ('Emirates','Cathay Dragon','Etihad','Kuwait Airways','Korean Air','Malaysian Airlines','Silk Air','Vistara','Thai Airways','Saudia','Alliance Air','TruJet','Singapore Airlines','Air India','Air Mauritius','Tiger Airways','Air India Express')"

    Night_data_out = pd.read_sql(
        f"""select * from TurnaroundEquipment t2 where AirportCode={OpName} {airline_filter_condition} and (date(STA) between date("{Min_date}") and "{Max_date}") and (date(STD) between date("{Min_date}") and "{Max_date}") and MGT < timestampdiff(MINUTE,STA,STD) and date(STA) < date(STD) """,
        con=connection)
    print(
        f"""select * from TurnaroundEquipment t2 where AirportCode={OpName} {airline_filter_condition} and (date(STA) between date("{Min_date}") and "{Max_date}") and (date(STD) between date("{Min_date}") and "{Max_date}") and MGT < timestampdiff(MINUTE,STA,STD) and date(STA) < date(STD) """)
    Night_data_out['AircraftType'] = Night_data_out['AircraftType'].apply(lambda x: "None" if x == None else x)

    print(Night_data_out)

    if not Night_data_out.empty:
        Night_data_out['STA_date'] = pd.to_datetime(Night_data_out['STA'].dt.date)
        Night_data_out['STD_date'] = pd.to_datetime(Night_data_out['STD'].dt.date)

        while count < len(rangedate) - 1:

            Night_data = Night_data_out[
                (Night_data_out['STA_date'] == rangedate[count]) & (Night_data_out['STD_date'] == rangedate[count + 1])]

            count += 1
            if not Night_data.empty:

                if Night_data_out.empty == True:
                    Night = Night_data.copy()

                else:
                    Night = pd.concat([Night, Night_data], ignore_index=True)

        if not Night.empty:

            Night['ActivityStartEnd'] = Night.apply(
                lambda x: ActivityStartEnd(x["ActivityStartTime"], x["ActivityEndTime"]), axis=1)
            Night['ExpectedStartEnd'] = Night.apply(
                lambda x: ActivityStartEnd(x["ExpectedStartTime"], x["ExpectedEndTime"]), axis=1)
            Night['Arrival_Depature'] = Night.apply(lambda x: Arrival_Depature(x["EndType"]), axis=1)

            Night['ActivityStartTime'] = Night.apply(
                lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ActivityStartTime"], x["ActivityEndTime"]),
                axis=1)
            Night['ActivityEndTime'] = Night.apply(
                lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ActivityEndTime"], x["ActivityStartTime"]),
                axis=1)

            Night['ExpectedStartTime'] = Night.apply(
                lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ExpectedStartTime"], x["ExpectedEndTime"]),
                axis=1)
            Night['ExpectedEndTime'] = Night.apply(
                lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ExpectedEndTime"], x["ExpectedStartTime"]),
                axis=1)

            Type = set(Night['Arrival_Depature'])

            # Logic Implementation
            # Card Data#

            Night['Max_Date'] = Night.groupby(['LogId', 'Arrival_Depature'])['ExpectedStartEnd'].transform(max)
            Night['diff_Max_Log'] = (Night['Max_Date'] - Night['ActivityStartEnd']).dt.total_seconds().div(60)
            Night['Max_status'] = np.where(Night['diff_Max_Log'] >= 0, 'Ontime', 'Delay')

            Night_Aircraft_data_list = pd.DataFrame(list(set(Night['AircraftType'])), columns={'AircraftType'})

            Flight_out = Night.groupby(['LogId', 'Arrival_Depature'])['Max_status'].apply(
                lambda tags: "Delay" if ("Delay,Delay" in (','.join(tags))) else (
                    "Ontime" if ("Ontime,Ontime" in (','.join(tags))) else "Delay")).reset_index()
            Night_Aircraft_data_list = Night_Aircraft_data_list[
                Night_Aircraft_data_list['AircraftType'] != "None"].reset_index(drop=True)

            # Night Halt card data

            Night_total_flight = pd.DataFrame({"Total Flights": [Night['LogId'].nunique()]}, columns=['Total Flights'])

            Count_data = Flight_out.groupby(['Arrival_Depature'])['Max_status'].agg({"count"}).reset_index()
            total_data = Flight_out.groupby(["Arrival_Depature"])['Max_status'].agg({"value_counts"}).reset_index()
            Night_total_card = pd.merge(total_data, Count_data, how='left', on='Arrival_Depature')
            Night_total_card["percentage"] = (Night_total_card["value_counts"] * 100 / Night_total_card['count']).round(
                2)
            Night_total_card = Night_total_card[["Arrival_Depature", "Max_status", "percentage"]]

            for i in Type:

                datat = Night[Night["Arrival_Depature"] == i]

                if i == 'A':
                    datat['Flight'] = datat["ArrivalFlight"]
                else:
                    datat['Flight'] = datat["DepartureFlight"]

                datat.reset_index(drop=True, inplace=True)

                # Airline_out=datat.groupby(['Airline','BayType', 'AircraftType','FlightType','BodyType','Arrival_Depature'])['Max_status'].apply(lambda tags:"Delay" if ("Delay,Delay" in (','.join(tags))) else ("Ontime" if ("Ontime,Ontime" in (','.join(tags))) else "Delay")).reset_index()
                flight_out = datat.groupby(
                    ['Airline', 'Flight', 'BayType', 'AircraftType', 'FlightType', 'BodyType', 'Arrival_Depature'])[
                    'Max_status'].apply(lambda tags: "Delay" if ("Delay,Delay" in (','.join(tags))) else (
                    "Ontime" if ("Ontime,Ontime" in (','.join(tags))) else "Delay")).reset_index()

                Airline_Arrival_status = flight_out.groupby(
                    ['Airline', 'FlightType', 'BayType', 'BodyType', 'AircraftType', "Arrival_Depature"])[
                    'Max_status'].agg({"value_counts"}).reset_index()
                Count_data_Airline = flight_out.groupby(
                    ['Airline', 'FlightType', 'BayType', 'BodyType', 'AircraftType', "Arrival_Depature"])[
                    'Max_status'].agg({"count"}).reset_index()

                Airline_Arrival_output_status = pd.merge(Airline_Arrival_status, Count_data_Airline, how='left',
                                                         on=['Airline', 'FlightType', 'BayType', 'BodyType',
                                                             'AircraftType', "Arrival_Depature"])

                Airline_Arrival_output_status["percentage"] = (
                            Airline_Arrival_output_status['value_counts'] * 100 / Airline_Arrival_output_status[
                        'count']).round(2)
                Airline_Arrival_output_status = Airline_Arrival_output_status[
                    ['Airline', 'FlightType', 'BayType', 'BodyType', 'AircraftType', "Arrival_Depature", "Max_status",
                     'count', 'value_counts', "percentage"]]

                Flight_Arrival_status = flight_out.groupby(
                    ["Airline", 'Flight', 'FlightType', 'BayType', 'BodyType', 'AircraftType', "Arrival_Depature"])[
                    'Max_status'].agg({"value_counts"}).reset_index()

                Count_data_Flight = flight_out.groupby(
                    ["Airline", 'Flight', 'FlightType', 'BayType', 'BodyType', 'AircraftType', "Arrival_Depature"])[
                    'Max_status'].agg({"count"}).reset_index()

                Flight_Arrival_output_status = pd.merge(Flight_Arrival_status, Count_data_Flight, how='left',
                                                        on=["Airline", 'Flight', 'FlightType', 'BayType', 'BodyType',
                                                            'AircraftType', "Arrival_Depature"])
                Flight_Arrival_output_status["percentage"] = (
                            Flight_Arrival_output_status["value_counts"] * 100 / Flight_Arrival_output_status[
                        'count']).round(2)
                Flight_Arrival_output_status = Flight_Arrival_output_status[
                    ["Airline", 'Flight', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature", "Max_status",
                     'AircraftType', 'count', 'value_counts', "percentage"]]
                Night_Turn_Around_Flight_time = datat[
                    ['Airline', 'Flight', 'FlightType', 'BayType', 'BodyType', 'AircraftType', 'OperationName',
                     'Max_status', 'Arrival_Depature', 'ActivityStartTime', 'ActivityEndTime', 'ExpectedStartTime',
                     'ExpectedEndTime']]

                if Night_Start_end_activity_airline.empty == True:
                    Night_Start_end_activity_airline = Airline_Arrival_output_status.copy()

                else:
                    Night_Start_end_activity_airline = pd.concat(
                        [Night_Start_end_activity_airline, Airline_Arrival_output_status], ignore_index=True)

                if Night_Start_end_activity_Flight.empty == True:
                    Night_Start_end_activity_Flight = Flight_Arrival_output_status.copy()

                else:
                    Night_Start_end_activity_Flight = pd.concat(
                        [Night_Start_end_activity_Flight, Flight_Arrival_output_status], ignore_index=True)

                if Night_Turn_Around_time.empty == True:
                    Night_Turn_Around_time = Night_Turn_Around_Flight_time.copy()

                else:
                    Night_Turn_Around_time = pd.concat([Night_Turn_Around_time, Night_Turn_Around_Flight_time],
                                                       ignore_index=True)

    Night_Turn_Around_time = Night_Turn_Around_time.to_json(orient='split')
    Night_total_flight = Night_total_flight.to_json(orient='split')
    Night_total_card = Night_total_card.to_json(orient='split')
    Night_Start_end_activity_airline = Night_Start_end_activity_airline.to_json(orient='split')
    Night_Start_end_activity_Flight = Night_Start_end_activity_Flight.to_json(orient='split')
    Night_Aircraft_data_list = Night_Aircraft_data_list.to_json(orient='split')

    return {
        "statusCode": 200,
        'Night_total_flight': json.loads(Night_total_flight),
        'Night_total_card': json.loads(Night_total_card),
        'Night_Start_end_activity_airline': json.loads(Night_Start_end_activity_airline),
        'Night_Start_end_activity_Flight': json.loads(Night_Start_end_activity_Flight),
        'Night_Turn_Around_time': json.loads(Night_Turn_Around_time),
        'Night_Aircraft_data_list': json.loads(Night_Aircraft_data_list)
    }
