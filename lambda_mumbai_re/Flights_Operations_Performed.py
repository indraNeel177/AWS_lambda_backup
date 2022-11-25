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
        where_caluse = """ where "to2"."year" ='{}' """.format(year)
    elif year and month and not day:
        where_caluse = """ where "to2"."year" ='{}' and  "to2"."month" ='{}'""".format(str(year), str(month))
        # where_caluse = """ where "to2"."year" ='2021' and "to2"."month" ='7' and "to2"."day" in ('22','23','24','25','26','27','28','29','30','31') """
    elif year and month and day:
        where_caluse = """ where "to2"."year" ='{}' and "to2"."month" ='{}' and "to2"."day" ='{}'""".format(year, month,
                                                                                                            day)
    else:
        return {"statusCode": 404, 'body': json.dumps(df.DataFrame()),
                'details': "pass at least one parameter (year or month or day)",
                "headers": {'Content-Type': 'application/json'}}

    query_for_flight_handled = """ select logid as UFRN, "Location", "AirportName", "Airline_Name", "Airline_Code",
                                                "Arrival_Flight_Number",
                                                "Departure_Flight_Number",
                                                "Body_Type",
                                                "Aircraft_Type",
                                                "arrival_flight_registration_code" as Flight_Registartion_Code,
                                                "Arrival_Bay",
                                                "Departure_Bay",
                                                "Arrival_Bay_Type",
                                                "Departure_Bay_Type",
                                                "STA",
                                                "STD","ETA","ETD",
                                                "onblocktime" as On_Block_Time,
                                                "offblocktime" as Off_Block_Time,
                                                "Operation_Code",
                                                "Operation_Name",
                                                "Performed_On_Bay",
                                                "Schedule_Start_Time","Actual_Start_Time",
                                                "Schedule_End_Time",
                                                "Actual_End_Time",
                                                "Scheduled_Complition" as Scheduled_Complition_in_mins ,
                                                "Actual_Complition" as Actual_Complition_in_mins,
                                                "Operation_Status",
                                                "Delay_Duration" as Delay_Duration_in_mins,
                                                "devid" as Equipment_Id,
                                                "Equipment_Number",
                                                "Equipment_Type",
                                                "Equipment_Category",
                                                "Equipment_OwnerType",
                                                "Operator_ID",
                                                "Operator_Name",to2.year,to2.month,to2.day from turnaroundoperations as to2 inner join ( Select ai.FlightCode,eam.EntityId from AirlineInfo ai left join EntityAirlineMapping eam  on ai.Id =eam.AirlineId where eam.EntityId ='9' and eam.AirportId = '13' ) TAI on to2.Airline_Code =TAI.FlightCode
                                                                    {}  """.format(where_caluse)
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
    details = result.get("details")
    final_flights_df = final_flights_df.to_dict('records')
    return {"statusCode": status, 'body': json.dumps(final_flights_df), "headers": {'Content-Type': 'application/json'}}
