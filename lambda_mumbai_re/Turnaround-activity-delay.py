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

    def Equipment_opname(Equip, Operationame):

        if Equip == '':
            return Operationame
        else:
            return Equip

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
    # airline_filter_condition= ""

    Airlines_Names = pd.read_sql("""select ai.Name  from AirlineInfo ai left join EntityAirlineMapping eam on eam.AirlineId = ai.Id where eam.Status =1
                             and eam.isDeleted =0 and ai.isDeleted =0 and eam.AirportId ={} and eam.EntityId ={}""".format(
        OpName, 9), con=connection)
    airline_filter_condition = """ and Airline in {}""".format(tuple(list(Airlines_Names['Name'])))

    # airline_filter_condition= "" if OpName!= 13 else "and Airline in ('Emirates','Cathay Dragon','Etihad','Kuwait Airways','Korean Air','Malaysian Airlines','Silk Air','Vistara','Thai Airways','Saudia','Alliance Air','TruJet','Singapore Airlines','Air India','Air Mauritius','Tiger Airways','FedEx Express','Air India Express')"

    dt = pd.read_sql(
        f""" select AircraftType,Activity_name,STA,COALESCE(OnBlockTime,SensorATA) as OnBlockTime,STD,COALESCE(OffBlockTime,SensorATD) as OffBlockTime,LogId,Airline,ArrivalFlight,DepartureFlight,BayType,FlightType,BodyType,OperationName,Equipment,ActivityStartTime,ActivityEndTime,ExpectedStartTime,ExpectedEndTime,EndType,DelayCode from TurnaroundEquipment t2 where AirportCode={OpName} and  (date(COALESCE(ActivityEndTime,ActivityStartTime)) between "{Min_date}" and "{Max_date}"){airline_filter_condition} """,
        con=connection)

    delay = pd.read_sql(
        f""" select AircraftType, COALESCE(ETA,STA) as STA,COALESCE(ETD,STD) as STD,COALESCE(OnBlockTime,SensorATA) as OnBlockTime,COALESCE(OffBlockTime,SensorATD) as OffBlockTime,LogId,Airline,ArrivalFlight,DepartureFlight,BayType,FlightType,BodyType,OperationName,Equipment,ActivityStartTime,ActivityEndTime,ExpectedStartTime,ExpectedEndTime,EndType,DelayCode from TurnaroundEquipment t2 where AirportCode={OpName} and (date(COALESCE(OffBlockTime,SensorATD)) between "{Min_date}" and "{Max_date}"){airline_filter_condition} """,
        con=connection)
    Delay_total_flights_arrival_data = pd.read_sql(
        f""" select AircraftType, COALESCE(ETA,STA) as STA,COALESCE(ETD,STD) as STD,COALESCE(OnBlockTime,SensorATA) as OnBlockTime,COALESCE(OffBlockTime,SensorATD) as OffBlockTime,LogId,Airline,ArrivalFlight,DepartureFlight,BayType,FlightType,BodyType,OperationName,Equipment,ActivityStartTime,ActivityEndTime,ExpectedStartTime,ExpectedEndTime,EndType,DelayCode from TurnaroundEquipment t2 where AirportCode={OpName} and  (date(COALESCE(OnBlockTime,SensorATA)) between "{Min_date}" and "{Max_date}") {airline_filter_condition} """,
        con=connection)
    # dt['Equip_opname']=dt.apply(lambda x : Equipment_opname(x["Equipment"],x["OperationName"]),axis=1)

    dt['AircraftType'] = dt['AircraftType'].apply(lambda x: "None" if x == None else x)
    delay['AircraftType'] = delay['AircraftType'].apply(lambda x: "None" if x == None else x)

    # delay['Equip_opname']=delay.apply(lambda x : Equipment_opname(x["Equipment"],x["OperationName"]),axis=1)

    Operation_name_output = pd.DataFrame()
    Airline_output = pd.DataFrame()
    Flight_output = pd.DataFrame()
    Aircraft_data_list = pd.DataFrame()
    Activity_total_flight = pd.DataFrame()
    Activity_card = pd.DataFrame()
    Flight_gantview = pd.DataFrame()

    if not dt.empty:

        dt['ActivityStartEnd'] = dt.apply(lambda x: ActivityStartEnd(x["ActivityStartTime"], x["ActivityEndTime"]),
                                          axis=1)
        dt['ExpectedStartEnd'] = dt.apply(lambda x: ActivityStartEnd(x["ExpectedStartTime"], x["ExpectedEndTime"]),
                                          axis=1)
        dt['Arrival_Depature'] = dt.apply(lambda x: Arrival_Depature(x["EndType"]), axis=1)

        dt['ActivityStartTime'] = dt.apply(
            lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ActivityStartTime"], x["ActivityEndTime"]),
            axis=1)
        dt['ActivityEndTime'] = dt.apply(
            lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ActivityEndTime"], x["ActivityStartTime"]),
            axis=1)

        dt['ExpectedStartTime'] = dt.apply(
            lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ExpectedStartTime"], x["ExpectedEndTime"]),
            axis=1)
        dt['ExpectedEndTime'] = dt.apply(
            lambda x: Single_Activity_Handle(x["Arrival_Depature"], x["ExpectedEndTime"], x["ExpectedStartTime"]),
            axis=1)

        Type = set(dt['Arrival_Depature'])

        dt['ActivityEndTime'] = dt.groupby(['LogId', 'OperationName', 'Arrival_Depature'])['ActivityEndTime'].transform(
            max)

        dt["ActivityStartTime"] = dt.groupby(['LogId', 'OperationName', 'Arrival_Depature'])[
            'ActivityStartTime'].transform(max)

        dt = dt.drop_duplicates(subset=['LogId', 'OperationName', 'Arrival_Depature', 'ActivityEndTime'], keep='first')
        dt = dt.drop_duplicates(subset=['LogId', 'OperationName', 'Arrival_Depature', 'ActivityStartTime'],
                                keep='first')

        dt['Max_Date'] = dt.groupby(['LogId', 'Arrival_Depature'])['ExpectedStartEnd'].transform(max)
        dt['diff_Max_Log'] = (dt['Max_Date'] - dt['ActivityStartEnd']).dt.total_seconds().div(60)
        dt['Max_status'] = np.where(dt['diff_Max_Log'] >= 0, 'Ontime', 'Delay')

        Activity = dt[["LogId", "Airline", "ArrivalFlight", 'Activity_name', 'DepartureFlight', 'AircraftType',
                       'Arrival_Depature', 'BayType', 'FlightType', 'BodyType', "OperationName", 'Equipment',
                       "ActivityStartTime", "ActivityEndTime", "ExpectedStartTime", "ExpectedEndTime",
                       "ExpectedStartEnd", "ActivityStartEnd", 'Max_status', 'OnBlockTime', 'OffBlockTime', 'STA',
                       "STD"]]

        Aircraft_data_list = pd.DataFrame(list(set(Activity['AircraftType'])), columns={'AircraftType'})

        Aircraft_data_list = Aircraft_data_list[Aircraft_data_list['AircraftType'] != "None"].reset_index(drop=True)

        ##################Activity ####################

        # Night Halt card data

        # Total_count_data=Activity.drop_duplicates(subset=['LogId','Activity_name'],keep='first')

        Activity_total_flight = pd.DataFrame({"Total Flights": [Activity['LogId'].count()]}, columns=['Total Flights'])

        Activity_card = pd.DataFrame(Activity['Max_status'].value_counts(normalize=True).round(2) * 100).reset_index()

        for i in Type:

            datat = Activity[Activity["Arrival_Depature"] == i]

            if i == 'A':
                datat['Flight'] = datat["ArrivalFlight"]
            else:
                datat['Flight'] = datat["DepartureFlight"]

            datat.reset_index(drop=True, inplace=True)

            ###################Operation level #############

            Arrival = datat[['Airline', 'Flight', 'Activity_name', 'BayType', 'AircraftType', 'FlightType', 'BodyType',
                             'Arrival_Depature', 'Max_status', "ActivityStartTime", "ActivityEndTime",
                             "ExpectedStartTime", "ExpectedEndTime"]]

            Arrival_flight = Arrival.groupby(
                ['Airline', 'Flight', 'Activity_name', 'BayType', 'AircraftType', 'FlightType', 'BodyType',
                 'Arrival_Depature'])['Max_status'].apply(
                lambda tags: "Delay" if ("Delay,Delay" in (','.join(tags))) else (
                    "Ontime" if ("Ontime,Ontime" in (','.join(tags))) else "Delay")).reset_index()
            Arrival_airline = Arrival.groupby(
                ['Airline', 'Activity_name', 'BayType', 'AircraftType', 'FlightType', 'BodyType', 'Arrival_Depature'])[
                'Max_status'].apply(lambda tags: "Delay" if ("Delay,Delay" in (','.join(tags))) else (
                "Ontime" if ("Ontime,Ontime" in (','.join(tags))) else "Delay")).reset_index()

            OpN_total_data = \
            Arrival.groupby(['Activity_name', 'BayType', 'FlightType', 'BodyType', 'AircraftType', 'Arrival_Depature'])[
                'Max_status'].agg({"value_counts"}).reset_index()
            OpN_Count_data = Arrival.groupby(['Activity_name', 'BayType', 'FlightType', 'AircraftType', 'BodyType'])[
                'Max_status'].agg({"count"}).reset_index()
            Operation_name_out = pd.merge(OpN_total_data, OpN_Count_data,
                                          on=['Activity_name', 'BayType', 'FlightType', 'AircraftType', 'BodyType'])
            Operation_name_out["percentage"] = (
                        Operation_name_out['value_counts'] * 100 / Operation_name_out['count']).round(2)

            ################## Airline Level #############

            Airline_total_data = Arrival.groupby(
                ['Airline', 'Activity_name', 'BayType', 'FlightType', 'BodyType', 'AircraftType', 'Arrival_Depature'])[
                'Max_status'].agg({"value_counts"}).reset_index()
            Airline_Count_data = \
            Arrival.groupby(['Airline', 'Activity_name', 'BayType', 'FlightType', 'BodyType', 'AircraftType'])[
                'Max_status'].agg({"count"}).reset_index()
            Airline_out = pd.merge(Airline_total_data, Airline_Count_data,
                                   on=['Airline', 'Activity_name', 'BayType', 'FlightType', 'BodyType', 'AircraftType'])
            Airline_out["percentage"] = (Airline_out['value_counts'] * 100 / Airline_out['count']).round(2)

            ################# Flight Level ################

            Flight_total_data = Arrival.groupby(
                ['Airline', 'Flight', 'Activity_name', 'BayType', 'FlightType', 'BodyType', 'AircraftType',
                 'Arrival_Depature'])['Max_status'].agg({"value_counts"}).reset_index()
            Flight_Count_data = Arrival.groupby(
                ['Airline', 'Flight', 'Activity_name', 'BayType', 'FlightType', 'BodyType', 'AircraftType'])[
                'Max_status'].agg({"count"}).reset_index()
            Flight_out = pd.merge(Flight_total_data, Flight_Count_data,
                                  on=['Airline', 'Flight', 'Activity_name', 'BayType', 'FlightType', 'BodyType',
                                      'AircraftType'])
            Flight_out["percentage"] = (Flight_out['value_counts'] * 100 / Flight_out['count']).round(2)

            ################## gantchart flight level #################
            Flight_gantchart = Arrival[
                ['Airline', 'Flight', 'Activity_name', 'BayType', 'AircraftType', 'FlightType', 'BodyType',
                 'Arrival_Depature', "ActivityStartTime", "ActivityEndTime", "ExpectedStartTime", "ExpectedEndTime"]]

            if Operation_name_output.empty == True:
                Operation_name_output = Operation_name_out.copy()

            else:
                Operation_name_output = pd.concat([Operation_name_output, Operation_name_out], ignore_index=True)

            if Airline_output.empty == True:
                Airline_output = Airline_out.copy()

            else:
                Airline_output = pd.concat([Airline_output, Airline_out], ignore_index=True)

            if Flight_output.empty == True:
                Flight_output = Flight_out.copy()

            else:
                Flight_output = pd.concat([Flight_output, Flight_out], ignore_index=True)

            if Flight_gantview.empty == True:
                Flight_gantview = Flight_gantchart.copy()

            else:
                Flight_gantview = pd.concat([Flight_gantchart, Flight_gantview], ignore_index=True)

    delay_code_level1 = pd.DataFrame()
    delay_code_level2 = pd.DataFrame()
    delay_code_aircraft_data_list = pd.DataFrame()
    Delay_total_flights_arrival = pd.DataFrame()
    Delay_total_depature = pd.DataFrame()
    delay_card = pd.DataFrame()

    if not (delay.empty and Delay_total_flights_arrival_data.empty):
        try:

            delay['Arrival_Depature'] = delay.apply(lambda x: Arrival_Depature(x["EndType"]), axis=1)

            delay['Depature_diff'] = (delay['STD'] - delay['OffBlockTime']).dt.total_seconds().div(60)
            # delay['Arrival_status'] = np.where(delay['Arrival_diff']>=0, 'Arrival_Ontime','Arrival_Delay')
            delay['Depature_status'] = np.where(delay['Depature_diff'] <= 0, 'Depature_Ontime', 'Depature_Delay')

            Delay_total_flights_arrival_data['Arrival_Depature'] = Delay_total_flights_arrival_data.apply(
                lambda x: Arrival_Depature(x["EndType"]), axis=1)

            Delay_total_flights_arrival_data['Arrival_diff'] = (
                        Delay_total_flights_arrival_data['STA'] - Delay_total_flights_arrival_data[
                    'OnBlockTime']).dt.total_seconds().div(60)

            Delay_total_flights_arrival_data['Arrival_status'] = np.where(
                Delay_total_flights_arrival_data['Arrival_diff'] >= 0, 'Arrival_Ontime', 'Arrival_Delay')

            Delay_total_flights_arrival = pd.DataFrame(
                {"Total Flights": [Delay_total_flights_arrival_data['LogId'].nunique()]}, columns=['Total Flights'])

            Delay_total_depature = pd.DataFrame({"Total Flights": [delay['LogId'].nunique()]},
                                                columns=['Total Flights'])

            Card_data_arrival = Delay_total_flights_arrival_data[["LogId", "Arrival_status"]]
            Card_data_depature = delay[["LogId", "Depature_status"]]

            total_data_arrival = Card_data_arrival.groupby(["LogId"])['Arrival_status'].agg(
                {"value_counts"}).reset_index()
            total_data_depature = Card_data_depature.groupby(["LogId"])['Depature_status'].agg(
                {"value_counts"}).reset_index()

            # Delay_total_depature=pd.DataFrame({"Total Flights":[delay['DepartureFlight'].nunique()]},columns=['Total Flights'])
            Delay_arrival = pd.DataFrame(total_data_arrival['Arrival_status'].value_counts().round(2)).reset_index()
            Delay_depature = pd.DataFrame(total_data_depature['Depature_status'].value_counts().round(2)).reset_index()

            Delay_arrival.rename(columns={"Arrival_status": "status", 'index': 'Type'}, inplace=True)
            Delay_depature.rename(columns={"Depature_status": "status", 'index': 'Type'}, inplace=True)

            delay_card = pd.concat([Delay_arrival, Delay_depature], ignore_index=True)

            delay.dropna(subset=['DelayCode'], how='all', inplace=True)

            if not delay.empty:
                delay[['DelayCode_code', 'DelayCode_value']] = delay['DelayCode'].str.split('-', expand=True)

                delay_code_aircraft_data_list = pd.DataFrame(list(set(delay['AircraftType'])), columns={'AircraftType'})

                delay_code_aircraft_data_list = delay_code_aircraft_data_list[
                    delay_code_aircraft_data_list['AircraftType'] != "None"].reset_index(drop=True)

                delay_code_aircraft_data_list = delay_code_aircraft_data_list.dropna(subset=['AircraftType'],
                                                                                     how='all').reset_index(drop=True)

                delay_code_level1 = \
                delay.groupby(["DelayCode_value", "Airline", "BodyType", "BayType", "FlightType", "AircraftType"])[
                    'ArrivalFlight'].agg({"nunique"}).reset_index()
                delay_code_level2 = delay.groupby(
                    ["DelayCode_value", "ArrivalFlight", "Airline", "BodyType", "BayType", "FlightType",
                     "AircraftType"])['ArrivalFlight'].agg({"nunique"}).reset_index()

                delay_code_level1.rename(columns={"nunique": "count"}, inplace=True)
                delay_code_level2.rename(columns={"nunique": "count"}, inplace=True)
        except Exception as e:
            print("Error in data process", e)
    Activity_total_flight = Activity_total_flight.to_json(orient='split')
    Activity_card = Activity_card.to_json(orient='split')
    Aircraft_data_list = Aircraft_data_list.to_json(orient='split')

    Activity_name_output = Operation_name_output.to_json(orient='split')
    Activity_Airline_output = Airline_output.to_json(orient='split')
    ActivityFlight_output = Flight_output.to_json(orient='split')
    ActivityFlight_gantview = Flight_gantview.to_json(orient='split')

    Delay_total_flights_arrival = Delay_total_flights_arrival.to_json(orient='split')
    Delay_total_flights_depature = Delay_total_depature.to_json(orient='split')
    delay_card = delay_card.to_json(orient='split')
    delay_code_level1 = delay_code_level1.to_json(orient='split')
    delay_code_level2 = delay_code_level2.to_json(orient='split')
    delay_code_aircraft_data_list = delay_code_aircraft_data_list.to_json(orient='split')

    return {
        "statusCode": 200,
        "Activity_total_flight": json.loads(Activity_total_flight),
        "Activity_card": json.loads(Activity_card),
        "Aircraft_data_list": json.loads(Aircraft_data_list),
        "Activity_name_output": json.loads(Activity_name_output),
        "Activity_Airline_output": json.loads(Activity_Airline_output),
        "ActivityFlight_output": json.loads(ActivityFlight_output),
        "ActivityFlight_gantview": json.loads(ActivityFlight_gantview),

        "Delay_total_flights_arrival": json.loads(Delay_total_flights_arrival),
        "Delay_total_flights_depature": json.loads(Delay_total_flights_depature),

        "delay_card": json.loads(delay_card),
        "delay_code_level1": json.loads(delay_code_level1),
        "delay_code_level2": json.loads(delay_code_level2),
        "delay_code_aircraft_data_list": json.loads(delay_code_aircraft_data_list)

    }

