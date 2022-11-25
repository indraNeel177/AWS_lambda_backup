import json
import pandas as pd
import numpy as np
import boto3
import time
import io
from datetime import datetime


def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_athena_dump = "s3://aws-athena-result-zestiot/athena_dumps/"
    today = str(datetime.now().date())
    max_date = None
    min_date = None
    kpi_id = None

    OpName = event['OperationUnit']
    Airline = event['Airline']

    if event != None and event != "":
        if ('min_date' in event.keys()) and (event.get("min_date") != None) and (event.get("min_date") != ""):
            min_date_str = event.get("min_date") + ' 00:00:01'
            min_date = datetime.strptime(min_date_str, '%Y-%m-%d %H:%M:%S').date()
        if 'max_date' in event.keys() and event.get("max_date") != None and event.get("max_date") != "":
            max_date_str = event.get("max_date") + ' 23:59:59'
            max_date = datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S').date()
        if 'KPIId' in event.keys() and event.get("KPIId") != None and event.get("KPIId") != "":
            kpi_id = event.get("KPIId")

    equipments = event['equipments']
    selected_equipment = list(event['selected_equipment'])
    # selected_equipment=""
    # equipments=""
    equipment_filter_condition_1 = "" if equipments == "" else f" and regexp_like(devid,'{equipments}')"
    equipment_filter_condition = "" if equipments == "" else f" and regexp_like(t1.devid,'{equipments}')"

    condition1 = '' if Airline == "" or Airline == None else f"where Airline=\'{Airline}\' "

    # entity condition added and hard coded for spicejet
    condition2 = '' if Airline == "" or Airline == None else f"and Airline='{Airline}' "

    flights = event['flights']
    airline_filter_condition = "" if flights == "" else f" and regexp_like(FlightNumber_Arrival,'{flights}')"

    Devices_to_drop = ['BAY', 'SCHE', 'ADSB']
    if kpi_id == 'Agg1' or kpi_id == '' or kpi_id == None:
        query_for_all_flights_by_all_equip = f"""SELECT t1.DevId as DevId,
                     t2.OperationUnit as OperationUnit,
                     t2.Entity as Entity,
                     count(DISTINCT(t1.Flight_PK)) AS equip_flights
            FROM 
                (SELECT DevId,
                     Flight_PK
                FROM equipactivitylogs_parquet
                WHERE date_parse(slogdate, '%Y-%m-%d %H:%i:%s')>date('{min_date_str}')
                        AND date_parse(slogdate, '%Y-%m-%d %H:%i:%s')<=date('{max_date_str}')
                        AND Assigned='1' and slogdate!='') AS t1
            LEFT JOIN 
                (SELECT *
                FROM EquipmentMaster_parquet) AS t2
                ON t1.DevId = t2.DevId {equipment_filter_condition} {condition1}
            GROUP BY  t1.DevId,OperationUnit,Entity"""

        print(query_for_all_flights_by_all_equip)
        response = athena.start_query_execution(QueryString=query_for_all_flights_by_all_equip,
                                                QueryExecutionContext={'Database': 'avileap_prod'},
                                                ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

        while status == 'RUNNING' or status == "QUEUED":
            print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/' + query_execution_id + '.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
        all_flights_by_all_equip_raw_data = pd.concat([pd.read_csv(
            io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1",
            error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index=True)

        entity = 2 if Airline == "SpiceJet" else all_flights_by_all_equip_raw_data.Entity
        df_equip_flights2 = all_flights_by_all_equip_raw_data.copy() if Airline == "" else \
        all_flights_by_all_equip_raw_data[all_flights_by_all_equip_raw_data.Entity == entity]
        final_agg = round(df_equip_flights2.equip_flights.sum() / len(df_equip_flights2.DevId))
        final_agg_json = {'avgflight_served_by_eqip': final_agg}
        final_agg_json_op = pd.DataFrame(final_agg_json, index=['0']).to_json(orient='split')
        result_json = {'ALL_Airline_By_ALL_EQP': json.loads(final_agg_json_op)}

    elif kpi_id == 'Total_flights_served_by_each_equipment':
        query_for_flights_served_by_each_equipment = f"""SELECT DevType,
                 t1.DevId as DevId,
                FlightsServed,
                 ShortDevName
        FROM 
            (SELECT substr(split_part(DevId,
                 '_',3), 2) AS DevType,DevId , count(distinct FlightServed) AS FlightsServed
            FROM 
                (SELECT t3.*,
                 t4.Entity
                FROM 
                    (SELECT *
                    FROM 
                        (SELECT DevId,
                 Flight_PK AS FlightServed
                        FROM equipactivitylogs_parquet
                        WHERE date_parse(SLogDate, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                                AND date_parse(SLogDate, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                                AND Slogdate!=''
                                AND SLogDate!='nan'
                                AND Assigned='1'  {equipment_filter_condition_1}) AS t1
                        LEFT JOIN 
                            (SELECT LogId,
                 OperationUnit,
                 Airline, Flightnumber_arrival
                            FROM DailyFlightSchedule_Merged_parquet
                            WHERE OperationUnit= '{OpName}'
                                    AND date_parse(SLogDate, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                                    AND date_parse(SLogDate, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                                    AND Slogdate!=''
                                    AND SLogDate!='nan') AS t2
                                ON t1.FlightServed =t2.LogId {condition2}
                            WHERE t2.OperationUnit='{OpName}' {airline_filter_condition}) AS t3
                            LEFT JOIN 
                                (SELECT DevId,
                 Entity
                                FROM EquipmentMaster_parquet) AS t4
                                    ON t3.DevId=t4.DevId )t5
                                GROUP BY  substr(split_part(DevId,'_',3), 2), DevId)t1
                            LEFT JOIN 
                            (SELECT DISTINCT ShortDevName,
                 DevId
                            FROM EquipmentMaster_parquet
                            WHERE DevId NOT IN ('BLOCK','Block','BAY 45') ) t2
                            ON t1.DevId=t2.DevId 
        """

        print(query_for_flights_served_by_each_equipment)
        response = athena.start_query_execution(QueryString=query_for_flights_served_by_each_equipment,
                                                QueryExecutionContext={'Database': 'avileap_prod'},
                                                ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

        while status == 'RUNNING' or status == "QUEUED":
            print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/' + query_execution_id + '.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
        flights_served_by_each_equipment_raw_data = pd.concat([pd.read_csv(
            io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1",
            error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index=True)
        flights_served_by_each_equipment_raw_data_json = flights_served_by_each_equipment_raw_data.to_json(
            orient='split')
        result_json = {'Flights_By_Each_EQP_Airport': json.loads(flights_served_by_each_equipment_raw_data_json)}

    elif kpi_id == 'Total_flights_served_by_each_type_of_equipment':
        query_for_flights_served_by_each_type_of_equipment = f"""SELECT substr(split_part(DevId,
         '_',3), 2) AS DevName,  count(distinct Flight_PK)/date_diff('day',date('{event.get("min_date")}'),date('{event.get("max_date")}')) AS Flight_PK
FROM 
    (SELECT t5.*,
         t6.Entity
    FROM 
        (SELECT t3.*,
         t4.Airline
        FROM 
            (SELECT t1.*,
         t2.OperationUnit
            FROM 
                (SELECT DISTINCT DevId,
         Flight_PK
                FROM equipactivitylogs_parquet
                WHERE date_parse(slogdate, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                        AND date_parse(slogdate, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                        AND slogdate!='' 
                        AND Assigned='1' ) AS t1
                LEFT JOIN 
                    (SELECT DISTINCT DevId,
         OperationUnit, Entity
                    FROM EquipmentMaster_parquet
                    WHERE operationunit='{OpName}'
                    GROUP BY  devid, operationunit, entity)AS t2
                        ON t1.DevId = t2.DevId 
                         WHERE operationunit='{OpName}'  {equipment_filter_condition})as t3
                    LEFT JOIN 
                        (SELECT DISTINCT LogId,
         Airline
                        FROM DailyFlightSchedule_Merged_parquet
                        WHERE operationunit='{OpName}'
                                AND date_parse(slogdate, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                                AND date_parse(slogdate, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                                AND slogdate!='' and slogdate !='None')AS t4
                            ON t3.Flight_PK=t4.LogId )as t5
                        LEFT JOIN 
                            (SELECT DISTINCT DevId,
         Entity
                            FROM EquipmentMaster_parquet
                            WHERE operationunit='{OpName}') AS t6
                                ON t5.DevId= t6.DevId)t7 {condition1}
                        GROUP BY  substr(split_part(DevId, '_',3), 2)
        """

        print(query_for_flights_served_by_each_type_of_equipment)
        response = athena.start_query_execution(QueryString=query_for_flights_served_by_each_type_of_equipment,
                                                QueryExecutionContext={'Database': 'avileap_prod'},
                                                ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

        while status == 'RUNNING' or status == "QUEUED":
            # print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/' + query_execution_id + '.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
        Flights_By_Each_Type_EQP = pd.concat([pd.read_csv(
            io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1",
            error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index=True)
        # print("Flights_By_Each_Type_EQP",Flights_By_Each_Type_EQP)
        Flights_By_Each_Type_EQP = Flights_By_Each_Type_EQP[~Flights_By_Each_Type_EQP.isin(Devices_to_drop)].dropna()
        Flights_By_Each_Type_EQP_JSON = Flights_By_Each_Type_EQP.to_json(orient='split')
        result_json = {'Airline_By_Each_Type_EQP': json.loads(Flights_By_Each_Type_EQP_JSON)}

    elif kpi_id == 'Average_equipment_usage':
        print(str(selected_equipment))
        query_for_AVG_total_number_of_flights = f"""SELECT hour_of_day,
                     round(avg(arrival_flights_count)) arrival_flights_count,
                     round(avg(depart_flights_count)) AS depart_flights_count
            FROM 
                (SELECT arrival_hour.hour AS hour_of_day,
                     arrival_hour.arrival_flights_count ,
                     depart_flights_count
                FROM 
                    (SELECT hour(date_parse(On_Block_Time,
                     '%Y-%m-%d %H:%i:%s')) AS hour, count(FlightNumber_Arrival) AS arrival_flights_count
                    FROM dailyflightschedule_merged_parquet
                    WHERE OperationUnit='{OpName}' {condition2} 
                            AND date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                            AND date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                            AND On_Block_Time!='' and  On_Block_Time!='None' {airline_filter_condition}
                    GROUP BY  hour(date_parse(On_Block_Time, '%Y-%m-%d %H:%i:%s')), FlightNumber_Arrival) AS arrival_hour
                    LEFT JOIN 
                        (SELECT hour(date_parse(Off_Block_Time,
                     '%Y-%m-%d %H:%i:%s')) AS hour, count(FlightNumber_Departure) AS depart_flights_count
                        FROM DailyFlightSchedule_Merged_parquet
                        WHERE OperationUnit='{OpName}' {condition2} 
                                AND date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                                AND date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                                AND Off_Block_Time!='' and  Off_Block_Time!='None'
                        GROUP BY  hour(date_parse(Off_Block_Time, '%Y-%m-%d %H:%i:%s')), FlightNumber_Departure) AS depart_hour
                            ON arrival_hour.hour= depart_hour.hour)temp
                    GROUP BY  hour_of_day
            """
        print(query_for_AVG_total_number_of_flights)
        response = athena.start_query_execution(QueryString=query_for_AVG_total_number_of_flights,
                                                QueryExecutionContext={'Database': 'avileap_prod'},
                                                ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

        while status == 'RUNNING' or status == "QUEUED":
            print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/' + query_execution_id + '.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
        total_number_of_flights_raw_data = pd.concat([pd.read_csv(
            io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1",
            error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index=True)
        total_number_of_flights_raw_data_json = total_number_of_flights_raw_data.to_json(orient='split')

        query_for_avg_equipment_usage = f"""SELECT *
        FROM 
            (SELECT substr(split_part(DevId,
                 '_',3), 2) AS DeviceName , DevId, t1.Airline as Airline, t1.OperationUnit as OperationUnit, hour(date_parse(FlogDate,'%Y-%m-%d %H:%i:%s')) AS hour_of_day, t2.Flight_PK AS FlightServed
            FROM dailyflightschedule_merged_parquet AS t1
            LEFT JOIN equipactivitylogs_parquet AS t2
                ON t1.logid= t2.Flight_PK
            WHERE t2.FLogDate is NOT null
                    AND t2.Assigned='1'
                    AND date_parse(t2.FlogDate, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                    AND date_parse(t2.FlogDate, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                    AND t2.FlogDate!='' AND t2.FlogDate!='None'
                    AND t1.OperationUnit='{OpName}' {equipment_filter_condition_1} {condition2})t3
        WHERE DeviceName NOT IN ('BAY','SCHE') AND substr(split_part(DevId,'_',3), 2) in ({str(selected_equipment)[1:-1]})
        """
        print(query_for_avg_equipment_usage)
        response = athena.start_query_execution(QueryString=query_for_avg_equipment_usage,
                                                QueryExecutionContext={'Database': 'avileap_prod'},
                                                ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

        while status == 'RUNNING' or status == "QUEUED":
            # print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/' + query_execution_id + '.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
        avg_equipment_usage_raw_data = pd.concat([pd.read_csv(
            io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1",
            error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index=True)
        print("hhhhhhhhhhhhhh", avg_equipment_usage_raw_data.head())
        dd3 = avg_equipment_usage_raw_data.groupby(['DeviceName', 'hour_of_day'])['FlightServed'].agg(
            {'FlightServed': lambda x: x.nunique()}).reset_index() if Airline == "" else \
            avg_equipment_usage_raw_data.groupby(['DeviceName', 'hour_of_day', 'Airline'])['FlightServed'].agg(
                'count').reset_index()

        flightserved_cumsum = avg_equipment_usage_raw_data.groupby(['DevId', 'hour_of_day'])['FlightServed'].agg(
            {'FlightServed': lambda x: x.nunique()}).reset_index() if Airline == "" else \
            avg_equipment_usage_raw_data.groupby(['DevId', 'hour_of_day', 'Airline'])['FlightServed'].agg(
                'count').reset_index()

        flightserved_cumsum['cumsum'] = flightserved_cumsum.groupby(['DevId'])['FlightServed'].transform(
            lambda x: x.cumsum())
        flightserved_cumsum = flightserved_cumsum.sort_values(by=['hour_of_day', 'DevId'])
        flightserved_cumsum = flightserved_cumsum[['hour_of_day', 'DevId', 'FlightServed', 'cumsum']]
        flightserved_cumsum_dict_json = flightserved_cumsum.to_json(orient='split')

        # dd3_final = dd3[['DeviceName','hour_of_day','FlightServed']]
        dd3 = dd3[~dd3.DeviceName.isin(Devices_to_drop)]
        dd3_json = dd3.to_json(orient='split')

        dd1_sub = avg_equipment_usage_raw_data.drop('FlightServed', axis=1)
        dd4 = dd1_sub.groupby(['hour_of_day', 'DeviceName'])['DevId'].nunique().reset_index() if Airline == "" else \
            dd1_sub.groupby(['hour_of_day', 'DeviceName', 'Airline'])['DevId'].nunique().reset_index()

        dd4 = dd4[~dd4.DeviceName.isin(Devices_to_drop)]
        dd4_json = dd4.to_json(orient='split')

        dd5 = avg_equipment_usage_raw_data.groupby('DeviceName')['DevId'].nunique().reset_index() if Airline == "" else \
            avg_equipment_usage_raw_data.groupby(['DeviceName', 'Airline'])['DevId'].nunique().reset_index()
        dd5.rename(columns={'DevId': 'Eqip_Count'}, inplace=True)

        dd5 = dd5[~dd5.DeviceName.isin(Devices_to_drop)]
        dd5_json = dd5.to_json(orient='split')
        result_json = {'Total_Number_Of_Flights': json.loads(total_number_of_flights_raw_data_json),
                       'Total_Flights_Served_By_Equp_By_Hour': json.loads(dd3_json),
                       'Total_Eqip_In_Use': json.loads(dd4_json),
                       'Equip_Being_Tracked': json.loads(dd5_json),
                       'flightserved_cumsum': json.loads(flightserved_cumsum_dict_json)
                       }

    elif kpi_id == 'cumulative_equipment_utilization':
        query_for_fligths_served_cumulative_equipment_utilization = f"""SELECT *
        FROM 
            (SELECT substr(split_part(DevId,
                 '_',3), 2) AS DeviceName , DevId, t1.Airline as Airline, t1.OperationUnit as OperationUnit, hour(date_parse(FlogDate,'%Y-%m-%d %H:%i:%s')) AS hour_of_day, t2.Flight_PK AS FlightServed
            FROM dailyflightschedule_merged_parquet AS t1
            LEFT JOIN equipactivitylogs_parquet AS t2
                ON t1.logid= t2.Flight_PK
            WHERE t2.FLogDate is NOT null
                    AND t2.Assigned='1'
                    AND date_parse(t2.FlogDate, '%Y-%m-%d %H:%i:%s')>date_parse('{min_date_str}', '%Y-%m-%d %H:%i:%s')
                    AND date_parse(t2.FlogDate, '%Y-%m-%d %H:%i:%s')<=date_parse('{max_date_str}', '%Y-%m-%d %H:%i:%s')
                    AND t2.FlogDate!=''
                    AND t1.OperationUnit='{OpName}' {equipment_filter_condition_1} {condition2})t3
        WHERE DeviceName NOT IN ('BAY','SCHE') AND substr(split_part(DevId,'_',3), 2) in ({str(selected_equipment)[1:-1]})"""

        response = athena.start_query_execution(QueryString=query_for_fligths_served_cumulative_equipment_utilization,
                                                QueryExecutionContext={'Database': 'avileap_prod'},
                                                ResultConfiguration={'OutputLocation': s3_athena_dump})
        query_execution_id = response['QueryExecutionId']
        print("Query execution id is {}".format(query_execution_id))

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']

        while status == 'RUNNING' or status == "QUEUED":
            # print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
            time.sleep(1)
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

        s3 = boto3.client('s3')
        bucket = 'aws-athena-result-zestiot'
        key = 'athena_dumps/' + query_execution_id + '.csv'
        keys = [i['Key'] for i in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]
        avg_equipment_usage_raw_data = pd.concat([pd.read_csv(
            io.BytesIO(s3.get_object(Bucket=bucket, Key=i)['Body'].read()), encoding="ISO-8859-1",
            error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index=True)

        flightserved_cumsum = avg_equipment_usage_raw_data.groupby(['DevId', 'hour_of_day'])['FlightServed'].agg(
            {'FlightServed': lambda x: x.nunique()}).reset_index() if Airline == "" else \
            avg_equipment_usage_raw_data.groupby(['DevId', 'hour_of_day', 'Airline'])['FlightServed'].agg(
                'count').reset_index()
        flightserved_cumsum['cumsum'] = flightserved_cumsum.groupby(['DevId'])['FlightServed'].transform(
            lambda x: x.cumsum())
        flightserved_cumsum = flightserved_cumsum.sort_values(by=['hour_of_day', 'DevId'])
        flightserved_cumsum = flightserved_cumsum[['hour_of_day', 'DevId', 'FlightServed', 'cumsum']]
        flightserved_cumsum_dict_json = flightserved_cumsum.to_json(orient='split')

        result_json = {
            'flightserved_cumsum': json.loads(flightserved_cumsum_dict_json)
        }

    return {
        'statusCode': 200,
        'data': result_json

    }
