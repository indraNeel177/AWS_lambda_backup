import pandas as pd
import pymysql
import json
import numpy as np
import configparser
import boto3
import io
from datetime import timedelta


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

    OpName = int(event['OperationUnit'])
    Airline = event['Airline']
    Min_date = event['Mindate']
    Max_date = event["Maxdate"]
    # airline_filter_condition= ""

    Device_id = " " if OpName != 13 else "and DevId like 'ASAT%'"

    airline_filter_condition = "" if OpName != 13 else "and Airline in ('Emirates','Cathay Dragon','Etihad','Kuwait Airways','Korean Air','Malaysian Airlines','Silk Air','Vistara','Thai Airways','Saudia','Alliance Air','TruJet','Singapore Airlines','Air India','Air Mauritius','Tiger Airways','FedEx Express','Air India Express')"

    data = pd.read_sql(
        f""" select LogId,Airline,ArrivalFlight,DevId,ActivityStartTime,ActivityEndTime,OwnershipCategory,EquipmentCategory,OperationName,BodyType,BayType,Equipment from TurnaroundEquipment t2 where (date(ActivityStartTime) between '{Min_date}' and '{Max_date}') {airline_filter_condition}{Device_id} and AirportCode={OpName} """,
        con=connection)
    data['Equipment'] = np.where(data['Equipment'] == '', data['OperationName'], data['Equipment'])

    card_data_out = pd.DataFrame()
    Airline_data_list = pd.DataFrame()
    Level1_data = pd.DataFrame()
    Level2_data = pd.DataFrame()
    Level3_data = pd.DataFrame()

    if not data.empty:
        ########## Card data ###################

        card_data_grouping = \
        data.groupby(['Equipment', 'Airline', 'BodyType', 'BayType', 'OwnershipCategory', 'EquipmentCategory'])[
            'LogId'].agg({"nunique"}).reset_index()

        card_data = card_data_grouping.groupby(['Equipment'])['nunique'].agg({"sum"}).reset_index()

        card_data.rename(columns={"sum": "count"}, inplace=True)

        card_data_out = card_data.sort_values(by='count', ascending=False).head(5).reset_index(drop=True)

        ########## charts data ###############

        Airline_data_list = pd.DataFrame(list(set(data['Airline'])), columns={'Airline'})

        Level1_data = \
        data.groupby(['Equipment', 'Airline', 'BodyType', 'BayType', 'OwnershipCategory', 'EquipmentCategory'])[
            'LogId'].agg({"nunique"}).reset_index()
        Level1_data.rename(columns={"nunique": "count"}, inplace=True)
        Level2_data = data.groupby(
            ['DevId', 'Equipment', 'Airline', 'BodyType', 'BayType', 'OwnershipCategory', 'EquipmentCategory'])[
            'LogId'].agg({"nunique"}).reset_index()
        Level2_data.rename(columns={"nunique": "count"}, inplace=True)

        Level3_data = data[['DevId', 'ArrivalFlight', 'Airline', "Equipment", 'ActivityStartTime', 'ActivityEndTime',
                            'OwnershipCategory', 'EquipmentCategory', 'BodyType', 'BayType']]

    card_data_out = card_data_out.to_json(orient='split')
    Airline_data_list = Airline_data_list.to_json(orient='split')

    Level1_data = Level1_data.to_json(orient='split')

    Level2_data = Level2_data.to_json(orient='split')
    Level3_data = Level3_data.to_json(orient='split')

    return {
        "statusCode": 200,
        'Equip_flight_Card_data': json.loads(card_data_out),
        'Equip_Airline_list': json.loads(Airline_data_list),
        'Equip_flight_level1': json.loads(Level1_data),
        'Equip_flight_level2': json.loads(Level2_data),
        'Equip_flight_level3': json.loads(Level3_data)
    }
