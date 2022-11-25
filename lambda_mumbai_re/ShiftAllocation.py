import pandas as pd
import json
import boto3
import time
from datetime import date


def lambda_handler(event, context=None):
    current_year = date.today().year
    athena = boto3.client('athena', region_name='ap-south-1')
    s3_athena_dump = "s3://prod-and-staging-athena-query-results/prod-athena-dumps/"
    database = "avileap_prod"
    s3_dumps = 'prod-athena-dumps'
    bucket = 'prod-and-staging-athena-query-results'
    year = event["queryStringParameters"].get('year', current_year)  # event["year"]
    month = event["queryStringParameters"].get("month", '')
    month = str(month)[1] if month and len(month) > 1 and int(month) < 10 else month
    day = event["queryStringParameters"].get("day", '')  # event["day"]
    day = str(day)[1] if day and len(day) > 1 and int(day) < 10 else day
    if year and not month and not day:
        where_caluse = """ where "ss"."year" ='{}' """.format(year)
    elif year and month and not day:
        where_caluse = """ where "ss"."year" ='{}' and  "ss"."month" ='{}'""".format(str(year), str(month))
    elif year and month and day:
        where_caluse = """ where "ss"."airport" ='13' and  "ss"."year" ='{}' and "ss"."month" ='{}' and "ss"."day" ='{}'""".format(
            year, month,
            day)
    # query_for_flight_handled= """ select ss.location as Location,ss.airportname as AirportName, ss.sas_date as date,ss.shift_tittle as shift_type, ss.shift_start_time as Shift_Start_,ss.shift_end_time as Shift_End_Time,ss.actual_start_time as Actual_Shift_Start_Time,ss.shift_end_time as Actual_Shift_End_Time, "airline code", dfsm.airline,dfsm.aircrafticao_arrival as aircraft_type,
    # dfsm.flightnumber_arrival,dfsm.logid,dfsm.flightnumber_departure,dfsm.bodytype,"Bay" as Arrival_Bay,
    # Bay_Departure,"arrival bay type", "departure bay type","FromAirport_IATACode" as Origin, "ToAirport_IATACode" as Destination,
    # "STA", "STD", "ETA", "ETD", "On_Block_Time", "Off_Block_Time", sas2.split_type as flight_split_type, sas2.splitted_for as flight_splitted_for,AircraftRegistration_Arrival as flight_registration_code, employee_id,member_name,position,equipment_sta,
    # equipment_std,sae.master_devid as devid,sae.equipment_category,sae.ownership_category,sae.equipment as equipment_type,
    # sae.shortdevname as Equipment_Number from shiftAllocation_shiftteammembers as ss left join shiftAllocation_shiftteamflights sas2 on sas2.shift_team_id =ss.shift_team_id
    # left join DailyFlightSchedule_Merged dfsm on sas2.flight_arrival_id=dfsm.FlightArrivalId left join shiftAllocation_equipmentallocations sae on ss.id=sae.team_member_id and sae.team_flight_id =sas2.id {} """.format(where_caluse)

    # Removed lines:- sas2.split_type as flight_split_type, sas2.splitted_for as flight_splitted_for,
    query_for_flight_handled = """ select S.Location,S.AirportName,S.date as Date,S.shift_type as Shift_Type, S.Shift_Start_Time as Shift_Start_Time,S.Shift_End_Time as Shift_End_Time,S.Actual_Shift_Start_Time as Actual_Shift_Start_Time,S.Actual_Shift_End_Time as Actual_Shift_End_Time,
    S.airline as Airline,S."airline code" as Airline_Code, S.flightnumber_arrival as Flight_Number_Arrival,S.flightnumber_departure as Flight_Number_Departure,S.aircraft_type as Aircraft_Type,S.flight_registration_code as Flight_Registration_Code,S.bodytype as Bodytype,
    S."Arrival_Bay", S.Bay_Departure,S."arrival bay type" as Arrival_Bay_Type, S."departure bay type" as Departure_Bay_Type,S.Origin, S.Destination,
    S."STA", S."STD", S."ETA", S."ETD", S."On_Block_Time", S."Off_Block_Time",S.employee_id as Employee_Id, S.member_name as Employee_Name,S.position as Designation,S.devid as Equipment_Id,
    S.equipment_category as Equipment_Category,S.ownership_category as Ownership_Category,S.equipment_type as Equipment_Type,S.Equipment_Number,T.activity_name as Operation_Name, coalesce(T.schedule_start_time, S.equipment_sta) as Operation_Scheduled_Start_Time,
    coalesce(T.schedule_end_time,S.equipment_std) as Operation_Scheduled_End_Time,T.actual_start_time as Operation_Actual_Start_Time,
    T.actual_end_time as Operation_Actual_End_Time,S.year,S.month,S.day from 

    (select dfsm.logid as mlogid,ss.location as Location,ss.airportname as AirportName, ss.sas_date as date,ss.shift_tittle as shift_type,
    ss.shift_start_time as Shift_Start_Time,ss.shift_end_time as Shift_End_Time,ss.actual_start_time as Actual_Shift_Start_Time,
    ss.shift_end_time as Actual_Shift_End_Time, "airline code", dfsm.airline,dfsm.aircrafticao_arrival as aircraft_type,
    dfsm.flightnumber_arrival,dfsm.flightnumber_departure,dfsm.bodytype,"Bay" as Arrival_Bay,
    Bay_Departure,"arrival bay type", "departure bay type","FromAirport_IATACode" as Origin, "ToAirport_IATACode" as Destination,
    "STA", "STD", "ETA", "ETD", "On_Block_Time", "Off_Block_Time",
    AircraftRegistration_Arrival as flight_registration_code, employee_id,member_name,position,equipment_sta,
    equipment_std,sae.master_devid as devid,sae.equipment_category,sae.ownership_category,sae.equipment as equipment_type,
    sae.shortdevname as Equipment_Number,ss.year,ss.month,ss.day from shiftAllocation_shiftteammembers as ss left join shiftAllocation_shiftteamflights sas2 on sas2.shift_team_id =ss.shift_team_id 
    left join DailyFlightSchedule_Merged dfsm on sas2.flight_arrival_id=dfsm.FlightArrivalId left join shiftAllocation_equipmentallocations sae on ss.id=sae.team_member_id and (sae.team_flight_id =sas2.id or sae.allocation_mode ='common')  {} ) S left join turnaroundoperations T on S.mlogid=T.logid and T.devid=S.devid """.format(
        where_caluse)

    print(query_for_flight_handled)

    def run_query(query, database, s3_output, s3_dumps, bucket, max_execution=15):
        try:
            response = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': database
                },
                ResultConfiguration={
                    'OutputLocation': s3_output
                })
            execution_id = response['QueryExecutionId']
            state = 'QUEUED'
        except Exception as e:
            print(e)
        while (state in ['RUNNING', 'QUEUED']):
            try:
                max_execution = max_execution - 1
                response = athena.get_query_execution(QueryExecutionId=execution_id)
                if 'QueryExecution' in response and \
                        'Status' in response['QueryExecution'] and \
                        'State' in response['QueryExecution']['Status']:
                    state = response['QueryExecution']['Status']['State']
                    if state == 'SUCCEEDED':
                        s3_key = '{}/'.format(s3_dumps) + execution_id + '.csv'
                        local_filename = '/tmp/' + execution_id + '.csv'
                        try:
                            s3 = boto3.resource('s3', region_name='ap-south-1')
                            s3.Bucket(bucket).download_file(s3_key, local_filename)
                            df = pd.read_csv(local_filename, na_filter=False)
                            if not df.empty:
                                df['Equipment_Category'].loc[(df['Equipment_Category'] == '1')] = "Motorized"
                                df['Equipment_Category'].loc[(df['Equipment_Category'] == '2')] = "Non-Motorized"
                                df['Ownership_Category'].loc[(df['Ownership_Category'] == '1')] = "Own"
                                df['Ownership_Category'].loc[(df['Ownership_Category'] == '2')] = "Leased"
                            return {"status": 200, "details": "success", "data": df}
                        except Exception as e:
                            print('exception: ' + str(e))
                            if e.response['Error']['Code'] == "404":
                                return {"status": 404, "details": "The object does not exist", "data": pd.DataFrame()}
                            else:
                                return {"status": 404, "details": str(e), "data": pd.DataFrame()}
                    elif state == 'FAILED' or state == 'CANCELLED':
                        return {"status": 404, "details": "QUERY CANCELLED or FAILED", "data": pd.DataFrame()}
                # time.sleep(3)
            except Exception as e:
                print('exception: ' + str(e))
        return {"status": 404, "details": "Max execution reached", "data": pd.DataFrame()}

    result = run_query(query_for_flight_handled, database, s3_athena_dump, s3_dumps, bucket, 50)
    final_flights_df = result.get("data")
    status = result.get("status")
    final_flights_df = final_flights_df.to_dict('records')
    return {"statusCode": status, 'body': json.dumps(final_flights_df), "headers": {'Content-Type': 'application/json'}}
