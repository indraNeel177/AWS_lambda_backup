import json
import boto3
import pymysql
import pandas as pd
import io
import configparser


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    obj = s3.Object('tbb-temp-research-bucket', 'db_creds_prod.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    # hostname = configParser.get('db-creds-prod', 'hostname')
    hostname = 'aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = 'Reporting'
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname,
                           connect_timeout=5)

    min_date = event['min_date']
    max_date = event['max_date']
    OpName = event['OperationUnit']
    flights = event['flights']
    airline_filter_condition = "" if flights == "" else f" and FlightNumber_Arrival REGEXP '{flights}'"

    # equipments = event['equipments']
    # equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    # equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"

    Turnaround_first_level_1 = pd.read_sql(f"""
    select * from Turnaround_first_level_1 where date(archived_date)>date('{min_date}') and date(archived_date)<date('{max_date}') and Operationunit='{OpName}'
    """, con=conn)

    Turnaround_first_level_1_json = Turnaround_first_level_1.to_json(orient='split')

    Turnaround_first_level_2 = pd.read_sql(f"""
    select * from Turnaround_first_level_2 where date(archived_date)>date('{min_date}') and date(archived_date)<date('{max_date}') and Operationunit='{OpName}'
    """, con=conn)

    Turnaround_first_level_2_json = Turnaround_first_level_2.to_json(orient='split')

    # Turnaround_first_level_2_1= pd.read_sql(f"""
    # select * from Turnaround_first_level_2_1 where date(archived_date)>date({min_date}) and date(archived_date)<date({max_date}) and Operationunit={OpName}
    # """, con=conn)
    # Turnaround_first_level_2_1_json = Turnaround_first_level_2_1.to_json(orient='split')

    Turnaround_first_level_3 = pd.read_sql(f"""
    select * from Turnaround_first_level_3 where date(archived_date)>date('{min_date}') and date(archived_date)<date('{max_date}') and Operationunit='{OpName}'
     """, con=conn)

    Turnaround_first_level_3_json = Turnaround_first_level_3.to_json(orient='split')

    return {
        'statusCode': 200,
        'Turnaround_Level_1_2_3': json.loads(Turnaround_first_level_1_json),
        'Agg_Level_1': json.loads(Turnaround_first_level_2_json),
        # 'Turnaround_first_level_2_1': json.loads(Turnaround_first_level_2_1_json),
        'Agg_Level_2': json.loads(Turnaround_first_level_3_json)

    }

