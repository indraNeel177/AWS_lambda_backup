import json
import boto3
import pymysql
import pandas as pd
import datetime
import configparser
import io
import random


# client = boto3.client('rds')
def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources', 'db_creds_ini/db_creds_prod_read_replica.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname,
                           connect_timeout=5)

    OpName = event['OperationUnit']
    Airline = event['Airline']
    yesterday = event['yesterday']
    # OpName=4
    # Airline=""
    # yesterday = "True"
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday == "True" else ""

    # devid regex condition

    equipments = event['equipments']
    # equipments=""
    flights = event['flights']
    equipment_filter_condition_1 = "" if equipments == "" else f" and devid REGEXP '{equipments}'"

    equipment_filter_condition = "" if equipments == "" else f" and t1.devid REGEXP '{equipments}'"

    airline_filter_condition = "" if flights == "" else f" and FlightNumber_Arrival REGEXP '{flights}'"

    condition1 = '' if Airline == "" else f"and DevId like '%SG%' "

    # print(f"""select COUNT(distinct DevId) as count from NonCompliance where Airport={OpName} and date(LogDate)=date(ADDTIME(now(), '05:30:00'))""")
    df1 = pd.read_sql(f"""
    select COUNT(distinct DevId) as count from NonCompliance where Airport={OpName} and 
    date(LogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}  {equipment_filter_condition_1}  and `type`='OverSpeed' {condition1}
    """, con=conn)
    df1_json = df1.to_json(orient='split')

    ####################################################################################################################
    #                                                                                                                  #
    #                                             Total Distance travelled                                             #
    #                                                                                                                  #
    ####################################################################################################################

    condition2 = '' if Airline == "" else f"and t1.DevId like '%SG%' "
    df2_1 = pd.read_sql(f"""
    select t1.devid,t2.operationunit,t2.Entity,sum((t1.LegSize)/1000) as total_distance,t2.ShortDevName from (
        select devid,LegSize from EquipActivityLogs where 
        date(FLogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
        and type='Movement' and LegSize<10000
        )as t1 left join (select devid,operationunit,entity,ShortDevName from EquipmentMaster) as t2
        on t1.DevId=t2.DevId where operationunit={OpName} {equipment_filter_condition} {condition2} group by devid,operationunit,Entity 

    """, con=conn)

    print("pppppppppppppppppppppp", f"""
    select t1.devid,t2.operationunit,t2.Entity,sum((t1.LegSize)/1000) as total_distance,t2.ShortDevName from (
        select devid,LegSize from EquipActivityLogs where 
        date(FLogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
        and type='Movement' and LegSize<10000
        )as t1 left join (select devid,operationunit,entity,ShortDevName from EquipmentMaster) as t2
        on t1.DevId=t2.DevId where operationunit={OpName} {equipment_filter_condition} {condition2} group by devid,operationunit,Entity 

    """)
    # df2_1=df2_1.fillna(0.0, inplace=True)

    df2_1_avg = round(df2_1.total_distance.mean()) if len(df2_1.total_distance) != 0 else 100
    df2_1_avg_json = {'Total_Distance_Travelled_avg': df2_1_avg}
    df2_1_avg_json_op = pd.DataFrame(df2_1_avg_json, index=['0']).to_json(orient='split')

    if df2_1.empty == False:
        dd1 = df2_1.devid.str.split("_", expand=True).drop([0, 1, 3], axis=1).reset_index().drop('index',
                                                                                                 axis=1).rename(
            columns={2: 'devname1'})
        df2_1['DevName'] = dd1.devname1.str[1:]
    else:
        # list_activities= ['PCBA', 'PCB', 'PCDA', 'PCD', 'LAA', 'FLE', 'CAT' , 'WFG' , 'TCG' , 'CHO' , 'CHF' , 'PushBack' , 'BBF' , 'BBL' , 'LTD' , 'FTD']
        # lo = 0
        # hi = 25
        # size = 16
        # random_numb = [random.randint(lo, hi) for _ in range(size)]
        # dict_dummy = dict(zip(list_activities,random_numb))
        # Dummy_df = pd.DataFrame(dict_dummy.items(),columns=['Activities','Count'])
        dd = df2_1.append([0]).fillna(0).drop(0, axis=1)
        dd = dd[['devid', 'operationunit', 'Entity', 'total_distance', 'ShortDevName']]
        dd.rename(columns={'devid': 'DevName'}, inplace=True)
        df2_1 = dd.copy()

    print(df2_1.columns)
    df2_2 = df2_1[['ShortDevName', 'operationunit', 'Entity', 'total_distance', 'DevName']]
    print(df2_2.columns)
    df2_json = df2_2.to_json(orient='split')

    ####################################################################################################################
    #                                                                                                                  #
    #                                             ALL_Flights_By_ALL_EQP                                               #
    #                                                                                                                  #
    ####################################################################################################################

    df1 = pd.read_sql(f"""
    select t3.*,t4.Entity,t4.ShortDevName from (
    select * from (
    select DevId,Flight_PK as FlightServed from EquipActivityLogs 
    where date(ADDTIME(FLogDate, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {equipment_filter_condition_1} and Assigned=1
    and duration >=30 and operationname not in ('PCG')
    ) as t1 left join 
    (
    select LogId, OperationUnit, Airline from DailyFlightSchedule_Merged WHERE
    OperationUnit= {OpName} and date(ADDTIME(On_Block_time, '05:30:00')) =date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition}
    )  as t2 on t1.FlightServed =t2.LogId  where t2.OperationUnit={OpName}
    ) as t3 left join (select DevId,Entity,ShortDevName from EquipmentMaster) as t4
    on t3.DevId=t4.DevId where ShortDevName!='BLOCK'
    {condition2}
    """, con=conn)

    df1 = df1.drop(columns={'LogId', 'OperationUnit', 'Airline', 'Entity'}, axis=1)

    df1['FlightServed1'] = df1.groupby(['DevId', 'ShortDevName'])['FlightServed'].transform('nunique')

    df2 = df1.drop_duplicates(['DevId']).reset_index(drop=True)
    if df2.empty == False:

        df2.drop('FlightServed', axis=1, inplace=True)
        df2.rename(columns={'FlightServed1': 'FlightServed'}, inplace=True)
        dd2 = df2.DevId.str.split("_", expand=True).drop([0, 1, 3], axis=1).reset_index().drop('index', axis=1).rename(
            columns={2: 'devname1'})
        df2['DevType'] = dd2.devname1.str[1:]

        df2 = df2[['DevId', 'DevType', 'FlightServed', 'ShortDevName']]

        avgflight_served_by_eqip = round(df2.FlightServed.sum() / df2.DevId.count(), 1)
    else:
        avgflight_served_by_eqip = 0

    ALL_Flights_By_ALL_EQP_JSON = {
        "columns": [
            "avgflight_served_by_eqip"
        ],
        "index": [
            0
        ],
        "data": [
            [
                avgflight_served_by_eqip
            ]
        ]
    }

    ####################################################################################################################
    #                                                                                                                  #
    #                                           Flights_By_Each_Type_EQP                                               #
    #                                                                                                                  #
    ####################################################################################################################

    # data["DevName"]=data.DevId.str.slice(8,11)
    # dd1 = data.DevId.str.split("_", expand=True).drop([0,1,3], axis=1).reset_index().drop('index', axis=1).rename(columns={2:'devname1'})
    # data['DevName'] =dd1.devname1.str[1:]
    # Devices_to_drop =['BAY','SCHE']

    # data3 = data.copy() if Airline =="" else data[data['Airline']==Airline]

    # grouped_data=data3.groupby('DevName').agg({'Flight_PK':pd.Series.nunique}).reset_index()
    # grouped_data = grouped_data[~grouped_data.isin(Devices_to_drop)].dropna()
    # Flights_By_Each_Type_EQP_JSON=grouped_data.to_json(orient='split')

    return {
        'statusCode': 200,
        'Noncompliance_Agg': json.loads(df1_json),
        'Total_Distance_Travelled_avg': json.loads(df2_1_avg_json_op),
        'Total_Distance_Travelled': json.loads(df2_json),
        'ALL_Airline_By_ALL_EQP': ALL_Flights_By_ALL_EQP_JSON
        # 'Airline_By_Each_Type_EQP': json.loads(Flights_By_Each_Type_EQP_JSON),
        # 'message':'hello'
    }