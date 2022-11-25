import json
import boto3
import pymysql
import pandas as pd
import io
import configparser


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    #hostname = configParser.get('db-creds-prod', 'hostname')
    hostname='avileap-test.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = 'AviLeap'
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user="avileap_admin", passwd="admin_1q2w#E$R", db=dbname, connect_timeout=5)

    min_date= event['min_date']
    max_date= event['max_date']
    OpName = event['OperationUnit']
    flights = event['flights']
    kpi_id=event['KPI']
    Airline=event['Airline']
    operationshortname=event['OperationShortName']
    equipments=event['equipments']

    #min_date= '2019-08-05'
    #max_date= '2019-08-15'
    #OpName = '4'
    #flights = ""
    #kpi_id='airline_level_chart'
    #Airline=""
    #operationshortname=""
    #equipments="IOSL|DIAL_DIAL"

    if equipments == 'CLB|DIAL_DIAL':
        Operations = ["LAA", "LAP", "PCA", "PCDA", "PCD",
                      "BFA", "BFP", "BFH", "BFU", "BBF",
                      "BBL", "GPU", "ACU", "BFL", "FTD", 
                      "LTD", "WFG", "TCG", "PCG", "PCB",
                      "PCBA", "PBT", "PushBack", "CHF", "CHO"]
        ############
        #IOSL
    elif equipments == 'IOSL|DIAL_DIAL':
        Operations = ["FLE", "CHO", "CHF"]
        ##############
        #BSSPL
    elif  equipments == 'BSSPL|DIAL_DIAL':
        Operations = ["FLE", "CHO", "CHF"]
        #
        #OBEROI
    elif  equipments == 'OFS|DIAL_DIAL':
        Operations = ["CAA", "CAT", "CHO", "CHF"]
    else:
        Operations =['CHO', 'PFC', 'LAA', 'LAP', 'PCA', 'PCD', 'PCDA', 
                     'BFA', 'BFP', 'BFH', 'BFU', 'BBF', 'BBL', 'GPU', 'FLE', 
                     'CAA', 'CAT', 'CCO', 'PIL', 'CLE', 'WFG', 'TCG', 'PCB', 
                     'PCBA', 'LSP', 'LSS', 'BFL', 'FTD', 'LTD', 'ARS', 'PBT', 
                     'PushBack', 'CHF', 'ACU', 'PCG', 'ACL']

    print(Operations)


    airline_filter_condition= "" if flights=="" or flights==None else f" and FlightNumber_Departure REGEXP '{flights}'"
    airline_condition= "" if Airline=="" or Airline==None else f" and Airline= '{Airline}'"
    kpi_result={}

    #equipments = event['equipments']
    equipment_filter_condition_1= "" if equipments=="" else f" and devid REGEXP '{equipments}'"
    equipment_filter_condition_2= "" if equipments=="" else f" and devid REGEXP and '{equipments}'"
    equipment_filter_condition= "" if equipments=="" else f" and t2.devid REGEXP '{equipments}'"

    #=============NOT REQUIRED==============

    # Turnaround_Agg_Level_1= pd.read_sql(f"""
    # select * from Turnaround_Agg_Level_1 where date(archived_date)>date('{min_date}') and date(archived_date)<date('{max_date}') and Operationunit='{OpName}'
    # """, con=conn)
    # Turnaround_Agg_Level_1 = Turnaround_Agg_Level_1[['r_id','schedulestatus']]
    # Turnaround_Agg_Level_1.drop('r_id',axis=1,inplace=True)
    # Turnaround_Agg_Level_1['index']='WithinPTS'
    # Turnaround_Agg_Level_1=Turnaround_Agg_Level_1[['index','schedulestatus']]

    # Turnaround_Agg_Level_1_json=Turnaround_Agg_Level_1.to_json(orient='split')
    if kpi_id=="agg_pie_chart":
        Turnaround_Agg_Pie_Level_2= pd.read_sql(f"""
        select baytype,flighttype,bodytype,
        destination_type,shortname,schedulestatus,count_schedulestatus_x,
        count_schedulestatus_y,percentage_of_total,operationname 
        from Turnaround_Agg_Pie_Level_2 where date(archived_date)>date('{min_date}') and date(archived_date)<date('{max_date}') and Operationunit='{OpName}' 
        """, con=conn)
        #print("ffffffffff",Turnaround_Agg_Pie_Level_2)
        # Turnaround_Agg_Pie_Level_2.drop('r_id',axis=1,inplace=True)
        #Turnaround_Agg_Pie_Level_2=Turnaround_Agg_Pie_Level_2[['baytype','FlightType','bodytype','destination_type',
        #'shortname','schedulestatus','count_schedulestatus_x','count_schedulestatus_y','percentage_of_total','operationname','bay','archived_date','operationunit']]
        #Turnaround_Agg_Pie_Level_2.to_csv("testing.csv")
        Turnaround_Agg_Pie_Level_2_grouped=Turnaround_Agg_Pie_Level_2.groupby(['baytype', 'flighttype', 'bodytype', 'destination_type', 'shortname',
       'schedulestatus','operationname'])['count_schedulestatus_x', 'count_schedulestatus_y',
       'percentage_of_total'].agg('mean').reset_index()
        Turnaround_Agg_Pie_Level_2_grouped=Turnaround_Agg_Pie_Level_2_grouped.round(1)
        Turnaround_Agg_Pie_Level_2_grouped=Turnaround_Agg_Pie_Level_2_grouped[['baytype', 'flighttype', 'bodytype', 'destination_type', 'shortname',
       'schedulestatus', 'count_schedulestatus_x', 'count_schedulestatus_y',
       'percentage_of_total', 'operationname']]
    
        Operations_condition_df_agg = Turnaround_Agg_Pie_Level_2_grouped.copy() if equipments=="" else Turnaround_Agg_Pie_Level_2_grouped[Turnaround_Agg_Pie_Level_2_grouped.shortname.isin(Operations)]
        Turnaround_Agg_Pie_Level_2_json = Operations_condition_df_agg.to_json(orient='split')

        kpi_result['Agg_Pie_Chart']=json.loads(Turnaround_Agg_Pie_Level_2_json)

    elif kpi_id=="airline_level_chart":
        Turnaround_Agg_Airline_Level_3= pd.read_sql(f"""
        select 
airline, flightnumber_departure, flighttype,
       destination_type, baytype, bodytype, operationname,
       schedulestatus,bay, toairport_iatacode, shortname,
       opfullname, archived_date, operationunit,
       
avg(Acutal_Duration) AS Acutal_Duration, 
avg(expected_duration_int) AS expected_duration_int,
       avg(Acutal_Arrival) AS Acutal_Arrival, 
       avg(Actual_Departure) AS Actual_Departure,
       avg(Expected_Arrival) AS Expected_Arrival,
       avg(Expected_Departure) AS Expected_Departure
from Turnaround_Agg_Aircraft_Level_4 where date(archived_date)>=date('{min_date}') and date(archived_date)<=date('{max_date}')
and Operationunit='{OpName}' and operationname='{operationshortname}' {airline_condition} {airline_filter_condition} group by Airline, FlightType,
destination_type, BayType, 'BodyType', 'OperationName','ScheduleStatus','Bay'
        """, con=conn)
        print(Turnaround_Agg_Airline_Level_3.shape)
        Operations_condition_df_Level_3 = Turnaround_Agg_Airline_Level_3.copy() if equipments=="" else Turnaround_Agg_Airline_Level_3[Turnaround_Agg_Airline_Level_3.operationname.isin(Operations)]

        Turnaround_Agg_Airline_Level_3 = Operations_condition_df_Level_3[['airline', 'flighttype',
           'destination_type', 'baytype', 'bodytype', 'operationname',
           'schedulestatus', 'Acutal_Duration', 'expected_duration_int',
           'Acutal_Arrival', 'Actual_Departure', 'Expected_Arrival',
           'Expected_Departure']]



        #print(Turnaround_Agg_Airline_Level_3.columns)
        Turnaround_Agg_Airline_Level_3_json = Turnaround_Agg_Airline_Level_3.to_json(orient='split')

        kpi_result['Agg_Airline_Level_3_Eqip']=json.loads(Turnaround_Agg_Airline_Level_3_json)

    elif kpi_id=="flight_level_chart":
        Turnaround_Agg_Aircraft_Level_4= pd.read_sql(f"""
        select * from Turnaround_Agg_Aircraft_Level_4
        where date(archived_date)>date('{min_date}') and 
        date(archived_date)<date('{max_date}') and Operationunit='{OpName}'  and operationname='{operationshortname}'  {airline_condition}  {airline_filter_condition}
        """, con=conn)

        Operations_condition_df_Level_4 = Turnaround_Agg_Aircraft_Level_4.copy() if equipments=="" else Turnaround_Agg_Aircraft_Level_4[Turnaround_Agg_Aircraft_Level_4.operationname.isin(Operations)]

        Turnaround_Agg_Aircraft_Level_4=Turnaround_Agg_Aircraft_Level_4[['airline', 
        'flightnumber_departure', 'flighttype',
           'destination_type', 'baytype', 'bodytype', 
           'schedulestatus', 'Acutal_Duration', 'expected_duration_int',
           'Acutal_Arrival', 'Actual_Departure', 'Expected_Arrival',
           'Expected_Departure', 'bay', 'toairport_iatacode', 
           'opfullname']]



        #print(Turnaround_Agg_Aircraft_Level_4.columns)
        #,'FlightNumber_Departure', 'ToAirport_IATACode'
        Turnaround_Agg_Aircraft_Level_4_json = Turnaround_Agg_Aircraft_Level_4.to_json(orient='split')

        kpi_result['Agg_Flight_Level_4_Eqip']=json.loads(Turnaround_Agg_Aircraft_Level_4_json)


    return {
        'statusCode': 200,
        'data':kpi_result

        }
