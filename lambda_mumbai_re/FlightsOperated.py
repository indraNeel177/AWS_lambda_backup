import json
import botocore
import pandas as pd
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
    = ('AI', 'MK', '9I', 'EK', 'EY', 'KE', 'KU', 'MI', 'SQ', 'TG', 'TR', '2T', 'UK', 'IX', 'KA')


OperationUnit = event["queryStringParameters"].get("OperationUnit") if event["queryStringParameters"].get(
    "OperationUnit") else "13"
where_caluse = """ where "year"='{}' """.format(current_year)
columns_clause = """ LogId as UFRN,"location" as Location, "airport" as Airport, "Airline", "airline code" as "Airline_Code", "FlightNumber_Arrival" as Flight_Number_Arrival, "FlightNumber_Departure" as Flight_Number_Departure,"flighttype" as Flight_Type, "bodytype" as Body_Type, "AircraftIcao_Arrival" as Aircraft_Type,AircraftRegistration_Arrival as Registartion_Number,
"Bay" as Arrival_Bay, "Bay_Departure", "arrival bay type" as Arrival_Bay_Type, "departure bay type" as Departure_Bay_Type,
                          "PassengerCount_Arrival" as "Arrivalairlines_codes_Pax", "PassengerCount_Departure" as "Departure_Pax", "STA", "STD", "ETA", "ETD", "actual_on_block_time" as On_Block_Time, "Off_Block_Time", "ETD" as Target_Off_Block_Time,
                          "FromAirport_IATACode" as Origin, "ToAirport_IATACode" as Destination, "delaycode" as Delaycode, "delay" as Delay,"delay due to" as Delay_Due_To,"delay cause" as Delay_Cause, "delay in mins" as Delay_in_mins,"change aircraft" as Change_Aircraft,"cancel flight" as Cancel_flight,"unscheduled flight" as Unscheduled_Flight,"rh form status" as RH_Form_Status, "Belt_FirstBag" as First_Bag, "Belt_LastBag" as Last_Bag,category, ServiceType_Arrival as service_type, "year", "month", "day" """
# condition_caluse = """ "OperationUnit" = '{}' and "airline code" in {} """.format(OperationUnit, airlines_codes)
condition_caluse = """ "OperationUnit" = '{}' """.format(OperationUnit)
if year and not month and not day:
    where_caluse = """ where "year" ='{}' and {} """.format(year, condition_caluse)
elif year and month and not day:
    where_caluse = """ where "year" ='{}' and  "month" ='{}' """.format(str(year), str(month))
elif year and month and day:
    where_caluse = """ where "year" ='{}' and "month" ='{}' and "day" ='{}' and {} """.format(year, month, day,
                                                                                              condition_caluse)

# and ai.FlightCode !='I5'
query_for_flight_handled = """ SELECT {} FROM dailyflightschedule_merged as to2 inner join ( Select ai.FlightCode,eam.EntityId, ai.name from AirlineInfo ai left join EntityAirlineMapping eam  on ai.Id =eam.AirlineId where eam.EntityId ='9' and eam.AirportId = '22' and eam.status='1' and ai.isDeleted='0'  ) TAI on ( ("to2"."airline code" = TAI.FlightCode) or ("to2"."category"=TAI.FlightCode) ) {} """.format(
    columns_clause, where_caluse)
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
                            df['RH_Form_Status'].loc[(df['RH_Form_Status'] == "SUB")] = "Submitted"
                            df['RH_Form_Status'].loc[(df['RH_Form_Status'] == "SIGN")] = "Signed"

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
