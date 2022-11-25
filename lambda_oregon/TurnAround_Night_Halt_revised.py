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

        if Depature is None:
            return 'A'
        else:
            return Depature

    def ActivityStartEnd(start, end):

        if str(end) != 'NaT':
            return end
        else:
            return start

    OpName = int(event['OperationUnit'])
    Airline = event['Airline']
    Min_date = event['Mindate']
    Max_date = event["Maxdate"]

    airline_filter_condition = "" if Airline == "" else "and Airline in ({Airline})"

    connection = pymysql.connect(host='test-vpc-instance.cscnpwfy9ubd.ap-south-1.rds.amazonaws.com',
                                 database='AviLeap',
                                 user='avileap_admin',
                                 password='admin_1q2w#E$R')
    dt = pd.read_sql(
        f"""select * from TurnaroundEquipment t2 where AirportCode={OpName} {airline_filter_condition} and date(OnBlockTime) = '{Min_date}' and date(OffBlockTime) = '{Max_date}' and MGT < timestampdiff(MINUTE,OnBlockTime,OffBlockTime)""",
        con=connection)
    if not dt.empty:

        dt['ActivityStartEnd'] = dt.apply(lambda x: ActivityStartEnd(x["ActivityStartTime"], x["ActivityEndTime"]),
                                          axis=1)
        dt['ExpectedStartEnd'] = dt.apply(lambda x: ActivityStartEnd(x["ExpectedStartTime"], x["ExpectedEndTime"]),
                                          axis=1)
        dt['Arrival_Depature'] = dt.apply(lambda x: Arrival_Depature(x["EndType"]), axis=1)
        Type = set(dt['Arrival_Depature'])

        # Logic Implementation
        # Card Data#

        dt['Max_Date'] = dt.groupby(['LogId', 'Arrival_Depature'])['ExpectedStartEnd'].transform(max)
        dt['diff_Max_Log'] = (dt['Max_Date'] - dt['ActivityStartEnd']).dt.total_seconds().div(60)
        dt['Max_status'] = np.where(dt['diff_Max_Log'] >= 0, 'Ontime', 'Delay')

        # Night Halt card data

        total_flights = pd.DataFrame({'index': ["Total Flights"], 'status': [dt['LogId'].count()]},
                                     columns=['index', 'status'])

        Count_data = dt.groupby(['Arrival_Depature'])['Max_status'].agg({"count"}).reset_index()
        total_data = dt.groupby(["Arrival_Depature"])['Max_status'].agg({"value_counts"}).reset_index()
        Night_halt_Card = pd.merge(total_data, Count_data, how='left', on='Arrival_Depature')
        Night_halt_Card["percentage"] = (Night_halt_Card["value_counts"] * 100 / Night_halt_Card['count']).round(2)
        Night_halt_Card = Night_halt_Card[["Arrival_Depature", "Max_status", "percentage"]]

        Start_end_activity_airline = pd.DataFrame()
        Start_end_activity_Flight = pd.DataFrame()
        for i in Type:

            datat = dt[dt["Arrival_Depature"] == i]

            datat.reset_index(drop=True, inplace=True)

            Airline_out = \
            datat.groupby(['Airline', 'ArrivalFlight', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature"])[
                'Max_status'].apply(lambda tags: "Delay" if ("Delay,Delay" in (','.join(tags))) else (
                "Ontime" if ("Ontime,Ontime" in (','.join(tags))) else "Delay")).reset_index()
            Airline_Arrival_status = \
            Airline_out.groupby(['Airline', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature"])['Max_status'].agg(
                {"value_counts"}).reset_index()
            Count_data_Airline = \
            Airline_out.groupby(['Airline', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature"])['Max_status'].agg(
                {"count"}).reset_index()
            Airline_Arrival_output_status = pd.merge(Airline_Arrival_status, Count_data_Airline, how='left',
                                                     on=['Airline', 'FlightType', 'BayType', 'BodyType',
                                                         "Arrival_Depature"])

            Airline_Arrival_output_status["percentage"] = (
                        Airline_Arrival_output_status['value_counts'] * 100 / Airline_Arrival_output_status[
                    'count']).round(2)
            Airline_Arrival_output_status = Airline_Arrival_output_status[
                ['Airline', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature", "Max_status", "percentage"]]

            Flight_Arrival_status = \
            datat.groupby(["Airline", 'ArrivalFlight', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature"])[
                'Max_status'].agg({"value_counts"}).reset_index()
            Count_data_Flight = \
            datat.groupby(["Airline", 'ArrivalFlight', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature"])[
                'Max_status'].agg({"count"}).reset_index()
            Flight_Arrival_output_status = pd.merge(Flight_Arrival_status, Count_data_Flight, how='left',
                                                    on=["Airline", 'ArrivalFlight', 'FlightType', 'BayType', 'BodyType',
                                                        "Arrival_Depature"])
            Flight_Arrival_output_status["percentage"] = (
                        Flight_Arrival_output_status["value_counts"] * 100 / Flight_Arrival_output_status[
                    'count']).round(2)
            Flight_Arrival_output_status = Flight_Arrival_output_status[
                ["Airline", 'ArrivalFlight', 'FlightType', 'BayType', 'BodyType', "Arrival_Depature", "Max_status",
                 "percentage"]]

            if Start_end_activity_airline.empty == True:
                Start_end_activity_airline = Airline_Arrival_output_status.copy()
                Airline_Arrival_output_status.drop(Airline_Arrival_output_status.index, inplace=True)

            else:
                Start_end_activity_airline = pd.concat([Start_end_activity_airline, Airline_Arrival_output_status],
                                                       ignore_index=False)
                Airline_Arrival_output_status.drop(Airline_Arrival_output_status.index, inplace=True)

            if Start_end_activity_Flight.empty == True:
                Start_end_activity_Flight = Flight_Arrival_output_status.copy()
                Flight_Arrival_output_status.drop(Flight_Arrival_output_status.index, inplace=True)

            else:
                Start_end_activity_Flight = pd.concat([Start_end_activity_Flight, Flight_Arrival_output_status],
                                                      ignore_index=False)
                Flight_Arrival_output_status.drop(Flight_Arrival_output_status.index, inplace=True)

        Turn_Around_time = dt[
            ['Airline', 'ArrivalFlight', 'FlightType', 'BayType', 'BodyType', 'OperationName', 'ActivityStartTime',
             'ActivityEndTime', 'ExpectedStartTime',
             'ExpectedEndTime']]

        Total_flight_count_card = total_flights.to_json(orient='split')
        Night_Halt_card_data = Night_halt_Card.to_json(orient='split')

        Airline_level = Start_end_activity_airline.to_json(orient='split')
        Flight_level = Start_end_activity_Flight.to_json(orient='split')
        Actual_expected = Turn_Around_time.to_json(orient='split')

        return {
            "statusCode": 200,
            'Total Flight count': json.loads(Total_flight_count_card),
            'Card_response': json.loads(Night_Halt_card_data),
            'Level_Airline': json.loads(Airline_level),
            'Level2_Flight': json.loads(Flight_level),
            'Level3_Actual_Expected': json.loads(Actual_expected)
        }