import json
import pandas as pd
import s3fs

def lambda_handler(event, context):
    input_file_path=f's3://tbb-temp-research-bucket/opt_results/flights/b85e15f9-0b78-4c5b-83d7-f74483a11462.csv'
    df_flight=pd.read_csv(input_file_path)
    df_flight_1 = df_flight.loc[(df_flight['Airline'] ==event['Airline']) &
                            ((df_flight['month1']>=event['from_month']) &
                             (df_flight['day1'] >=event['from_day']))
                            & ((df_flight['month1'] <=event['to_month']) &
                               (df_flight['day1'] <=event['to_day']))]
    df_flight_1_json=df_flight_1.to_json(orient='split')
    return {
        'statusCode': 200,
        'Historic_Flights_post':json.loads(df_flight_1_json)
    }
