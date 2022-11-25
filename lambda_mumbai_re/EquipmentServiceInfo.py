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
    day =  event["queryStringParameters"].get("day", '')  # event["day"]
    day = str(day)[1] if day and len(day) > 1 and int(day) < 10 else day
    if year and not month and not day:
        where_caluse = """ where "ea"."year" ='{}' and "ea"."category"='service' """.format(year)
    elif year and month and not day:
        where_caluse = """ where "ea"."year" ='{}' and  "ea"."month" ='{}' and "ea"."category"='service' """.format(str(year), str(month))
    elif year and month and day:
        where_caluse = """ where "ea"."year" ='{}' and "ea"."month" ='{}' and "ea"."day" ='{}' """.format(year, month,
                                                                                                         day)
    query_for_flight_handled= """ select ea.Location as Location, ea.AirportName, ea.DevId as Equipment_Number , em.ShortDevName as Equipment_Short_Name, em.Iata_Code as Equipment_Code, em.Equipment as Equipment_Type, em.Equipment_Category ,em.Ownership_Category,em.Asset_Tag_No,ea.specification as Specification, ea.Created as Date_Of_Action, ea.Status  as Action, ea.Unserviceable_For as Reason,
                                  ea.Comments as Comments,em.Last_Maintainence_Done as Last_Maintenance,em.Next_Maintainence_Due as Next_Maintenance,em.Date_Of_Completion as Expected_Completion,ea.User_Name as Action_Performed_By_ID,"ea"."employee name" as Action_Performed_By_Name, ea.RoleName as Action_Performed_Role,ea.year,ea.month,ea.day from EquipmentAudit ea left join EquipmentMaster em on ea.DevId =em.DevId {} """.format(where_caluse)
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
            print (e)
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
                            print(list(df.columns))
                            if not df.empty:
                                df['Equipment_Category'].loc[(df['Equipment_Category'] == 1)] = "Motorized"
                                df['Equipment_Category'].loc[(df['Equipment_Category'] == 2)] = "Non-Motorized"
                                df['Ownership_Category'].loc[(df['Ownership_Category'] == 1)] = "Own"
                                df['Ownership_Category'].loc[(df['Ownership_Category'] == 2)] = "Leased"
                                df['Action'].loc[(df['Action'] == 0)] = "Unserviceable"
                                df['Action'].loc[(df['Action'] == 1)] = "Serviceable"
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
