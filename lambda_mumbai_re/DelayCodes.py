import pandas as pd
import json
import boto3
import time


def lambda_handler(event, context=None):
    athena = boto3.client('athena', region_name='ap-south-1')
    s3_athena_dump = "s3://prod-and-staging-athena-query-results/prod-athena-dumps/"
    database = "avileap_prod"
    s3_dumps = 'prod-athena-dumps'
    bucket = 'prod-and-staging-athena-query-results'

    query= """ Select * from DelayCodes """
    print(query)
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

    result = run_query(query, database, s3_athena_dump, s3_dumps, bucket, 50)
    final_flights_df = result.get("data")
    status = result.get("status")
    final_flights_df = final_flights_df.to_dict('records')
    return {"statusCode": status, 'body': json.dumps(final_flights_df), "headers": {'Content-Type': 'application/json'}}
