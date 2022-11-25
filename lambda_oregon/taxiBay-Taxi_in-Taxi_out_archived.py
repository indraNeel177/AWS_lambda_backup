import json
import pandas as pd
import numpy as np
import boto3
import time
import io
from datetime import datetime

def lambda_handler(event, context):
    athena = boto3.client('athena',region_name='us-west-2')
    s3_athena_dump="s3://aws-athena-result-zestiot/athena_dumps/"
    today=str(datetime.now().date())
    max_date=None
    min_date= None
    kpi_id=None
    
    OpName=event['OperationUnit']
    Airline=event['Airline']
    
    
    if event!=None and event!="":
        if ('min_date' in event.keys()) and (event.get("min_date")!=None) and (event.get("min_date")!=""):
            min_date_str=event.get("min_date")
            min_date=datetime.strptime(min_date_str, '%Y-%m-%d').date()
        if 'max_date' in event.keys() and event.get("max_date")!=None and event.get("max_date")!="":
            max_date_str=event.get("max_date")
            max_date=datetime.strptime(max_date_str, '%Y-%m-%d').date()
        if 'KPIId' in event.keys() and event.get("KPIId")!=None and event.get("KPIId")!="":
            kpi_id=event.get("KPIId")
            
    equipments = event['equipments']
    #equipments=""
    equipment_filter_condition_1= "" if equipments=="" else f" and regexp_like(devid,'{equipments}')"
    equipment_filter_condition= "" if equipments=="" else f" and regexp_like(t1.devid,'{equipments}')"
    
    
    condition1 = '' if Airline =="" or Airline==None else f"and Airline=\'{Airline}\' "
    
    #entity condition added and hard coded for spicejet
    condition2 = '' if Airline =="" or Airline==None else f"and entity=2 "
    
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and flightnumber_arrival REGEXP '{flights}'"
    
            
    Devices_to_drop =['BAY','SCHE']
    if OpName == '4' or OpName == '13':
        Taxi_details=f"""
        SELECT bay,
     operationunit,
    airline,flightnumber_arrival,flightnumber_departure,terminalarrival,runway_arrival,runway_departure,
    date_diff('minute',date_parse(sensor_ata,'%Y-%m-%d %H:%i:%s'),date_parse(On_Block_Time,'%Y-%m-%d %H:%i:%s')) as taxi_in ,
    date_diff('minute',date_parse(Off_Block_Time,'%Y-%m-%d %H:%i:%s'),date_parse(Sensor_ATD,'%Y-%m-%d %H:%i:%s')) as taxi_out
    FROM dailyflightschedule_merged_parquet
    WHERE date(
    try(date_parse(atd,
    '%Y-%m-%d %H:%i:%s'))
    )>date('{min_date}') and date(
    try(date_parse(atd,
    '%Y-%m-%d %H:%i:%s'))
    )<date('{max_date}')
    AND OperationUnit='{OpName}'
    AND on_block_time != ''
    AND off_block_time != ''
    AND Sensor_ATA !=''
    AND Sensor_ATD !=''
    {airline_filter_condition}
    
        """
    else :
        Taxi_details=f"""
        SELECT bay,
     operationunit,
    airline,flightnumber_arrival,flightnumber_departure,terminalarrival,runway_arrival,runway_departure,
    date_diff('minute',date_parse(ata,'%Y-%m-%d %H:%i:%s'),date_parse(On_Block_Time,'%Y-%m-%d %H:%i:%s')) as taxi_in ,
    date_diff('minute',date_parse(Off_Block_Time,'%Y-%m-%d %H:%i:%s'),date_parse(ATD,'%Y-%m-%d %H:%i:%s')) as taxi_out
    FROM dailyflightschedule_merged_parquet
    WHERE date(
    try(date_parse(atd,
    '%Y-%m-%d %H:%i:%s'))
    )>date('{min_date}') and date(
    try(date_parse(atd,
    '%Y-%m-%d %H:%i:%s'))
    )<date('{max_date}')
    AND OperationUnit='{OpName}'
    AND on_block_time != ''
    AND off_block_time != ''
    AND ATA !=''
    AND ATD !=''
    {airline_filter_condition}
    
        """
        
    print(Taxi_details)   
    response = athena.start_query_execution(QueryString=Taxi_details, QueryExecutionContext={'Database': 'avileap_prod'}, ResultConfiguration={'OutputLocation': s3_athena_dump})
    query_execution_id = response['QueryExecutionId']
    print("Query execution id is {}".format(query_execution_id))
    
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']
    
    while status=='RUNNING' or status=="QUEUED":
        print("Query status is {}. Going to sleep. Zzzzzzz....".format(status))
        time.sleep(1)
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
    
    s3 = boto3.client('s3',region_name='us-west-2')
    bucket = 'aws-athena-result-zestiot'
    key = 'athena_dumps/'+query_execution_id+'.csv'
    keys = [i['Key'] for i in s3.list_objects(Bucket = bucket,Prefix = key)['Contents']]
    taxi_raw_data = pd.concat([pd.read_csv(io.BytesIO(s3.get_object(Bucket = bucket,Key=i)['Body'].read()),encoding = "ISO-8859-1",error_bad_lines=False) for i in keys if i.endswith('csv')], ignore_index = True)
    
    print("taxi_raw_data")
    print(taxi_raw_data)
    df1= taxi_raw_data.copy()
    
    df1.taxi_in = df1.taxi_in.fillna(round(df1.taxi_in.mean(),1))
    df1.taxi_out = df1.taxi_out.fillna(round(df1.taxi_out.mean(),1))
    
    df1.terminalarrival=np.where(df1.terminalarrival.isin(['1C','1D']), 'T1',df1.terminalarrival)
    df1=df1[~(df1.terminalarrival.isin(['f','x','z','F','X','X']))]
    df1.terminalarrival= df1.terminalarrival.fillna('DISABLED')
    df2 = df1[abs(df1.taxi_out-df1.taxi_out.mean()) <= abs(3*df1.taxi_out.std())]
    df2['terminalarrival']='DISABLED' if OpName=="4" or OpName== "13" else df2['terminalarrival']

    df2 = df2.reset_index(drop=True)
    df_2_group = df2.groupby(['bay',
     'operationunit',
    'airline','flightnumber_arrival','flightnumber_departure','terminalarrival'],as_index=True)['taxi_in', 'taxi_out'].mean().reset_index().round(1)
    
    #df_2_group['airline'] =  df_2_group['airline']
    #df_2_group['flightnumber_arrival'] = df_2_group['flightnumber_arrival'] 
    #df_2_group['flightnumber_departure'] = df_2_group['flightnumber_departure']
    #df_2_group['runway_arrival'] = df_2_group['runway_arrival'] 
    #df_2_group['runway_departure'] = df_2_group['runway_departure']
    df_2_group= df_2_group[['bay', 'operationunit', 
           'airline', 'flightnumber_arrival','terminalarrival','taxi_in', 'taxi_out','flightnumber_departure']]
    
    df2_json = df_2_group.to_json(orient='split')
    
    df2['bay']=df2['bay'].astype('category')
    
    #taxi in distribution
    
    #print(OpName==4)
    if OpName=="4" or OpName=="13" :
        step_taxi_in = int((df2.taxi_in.max() -df2.taxi_in.min()))
        step_div = step_taxi_in/2
        
        print("step_div")
        print(step_div)
        list_of_ints_taxi_in=list(range(int(df2.taxi_in.min()),int(df2.taxi_in.max()),int((step_taxi_in/step_div))))
    
        #print(list_of_ints_taxi_in)
    
        classes_taxi_in = pd.cut(df2.taxi_in,list_of_ints_taxi_in,precision=0,right=True,include_lowest=True)
    
        df2_1 = pd.DataFrame(pd.value_counts(classes_taxi_in)).reset_index()
        df3 = df2_1.rename(columns={'index':'class','taxi_in':'number of bays'})
    
        labels_taxi_in=[]
        for each in df3['class']:
            labels_taxi_in.append(f'from {each.left} to {each.right}')
    
        classes_taxi_in = pd.cut(df2.taxi_in,list_of_ints_taxi_in,precision=0,labels=labels_taxi_in,right=True,include_lowest=True)
        df2_1 = pd.DataFrame(pd.value_counts(classes_taxi_in)).reset_index()
        df3 = df2_1.rename(columns={'index':'class','taxi_in':'number of bays'})
    
    
        #classes_taxi_in = pd.cut(df2.taxi_in,10)
        #df2_1 = pd.DataFrame(pd.value_counts(classes_taxi_in)).reset_index()    
        #df3 = df2_1.rename(columns={'index':'class','taxi_in':'number of bays'})
        #df3['class']= df3['class'].astype('str')
    
        df3['terminalarrival']='DISABLED'
        df3_json = df3.to_json(orient='split')
        df_taxi_all_json =  df3.to_json(orient='split')
    
        #classes_taxi_out = pd.cut(df2.taxi_out,10)
        #df2_2 = pd.DataFrame(pd.value_counts(classes_taxi_out)).reset_index()
        #df3_out = df2_2.rename(columns={'index':'class','taxi_out':'number of bays'})
        #df3_out['class']= df3_out['class'].astype('str')
    
    
        step_taxi_out = int((df2.taxi_out.max() -df2.taxi_out.min()))
        step_div = step_taxi_out/2
        list_of_ints_taxi_out=list(range(int(df2.taxi_out.min()),int(df2.taxi_out.max()),int((step_taxi_out/step_div))))
    
        classes_taxi_out_1 = pd.cut(df2.taxi_out,list_of_ints_taxi_out,precision=0,right=True,include_lowest=True)
    
    
        df2_2_1 = pd.DataFrame(pd.value_counts(classes_taxi_out_1)).reset_index()
        df3_out = df2_2_1.rename(columns={'index':'class','taxi_out':'number of bays'})
    
        labels_taxi_out=[]
        for each in df3_out['class']:
            labels_taxi_out.append(f'from {each.left} to {each.right}')
    
        classes_taxi_out = pd.cut(df2.taxi_out,list_of_ints_taxi_out,precision=0,labels=labels_taxi_out,right=True,include_lowest=True)
        df2_2 = pd.DataFrame(pd.value_counts(classes_taxi_out)).reset_index()
        df3_out = df2_2.rename(columns={'index':'class','taxi_out':'number of bays'})
        df3_out['terminalarrival']='DISABLED'
    
        df4_json = df3_out.to_json(orient='split')
        df_taxi_out_all_json =  df3_out.to_json(orient='split')
    
    else:
    
        def taxi_in(df,terminal):  
            global df_class
            step_taxi_in = int((df.taxi_in.max() -df.taxi_in.min()))
            step_div = step_taxi_in/2
            list_of_ints_taxi_in=list(range(int(df.taxi_in.min()),int(df.taxi_in.max()),int((step_taxi_in/step_div))))
    
            classes_taxi_in_1 = pd.cut(df.taxi_in,list_of_ints_taxi_in,precision=0,right=True,include_lowest=True)
    
            df2_1_classes = pd.DataFrame(pd.value_counts(classes_taxi_in_1)).reset_index()
            df_class = df2_1_classes.rename(columns={'index':'class','taxi_in':'number of bays'})
    
            labels_taxi_in=[]
            for each in df_class['class']:
                labels_taxi_in.append(f'from {each.left} to {each.right}')
    
            df_class['class'] = labels_taxi_in
    
            df_class['terminalarrival']= terminal
            return df_class
    
    
        def taxi_out(df,terminal):  
            global df_class
            step_taxi_out = int((df.taxi_out.max() -df.taxi_out.min()))
            step_div = step_taxi_out/2
            list_of_ints_taxi_out=list(range(int(df.taxi_out.min()),int(df.taxi_out.max()),int((step_taxi_out/step_div))))
            classes_taxi_out_1 = pd.cut(df.taxi_out,list_of_ints_taxi_out,precision=0,right=True,include_lowest=True)
    
            df2_1_classes = pd.DataFrame(pd.value_counts(classes_taxi_out_1)).reset_index()
            df_class = df2_1_classes.rename(columns={'index':'class','taxi_out':'number of bays'})
    
            labels_taxi_out=[]
            for each in df_class['class']:
                labels_taxi_out.append(f'from {each.left} to {each.right}')
    
            df_class['class'] = labels_taxi_out
    
            df_class['terminalarrival']= terminal
            return df_class
    
    #terminal value
        df2_t1=df2[df2.terminalarrival=='T1'].reset_index(drop=True)
        df2_t2=df2[df2.terminalarrival=='T2'].reset_index(drop=True)
        df2_t3=df2[df2.terminalarrival=='T3'].reset_index(drop=True)
    
        #taxi in 
        df_Taxi_in_T1 = None if df2_t1.empty else  taxi_in(df2_t1,'T1')
        df_Taxi_in_T2 = None if df2_t2.empty else  taxi_in(df2_t2,'T2')
        df_Taxi_in_T3 = None if df2_t3.empty else  taxi_in(df2_t3,'T3')
        Df_Taxi_All = None if df2.empty else  taxi_in(df2,'ALL')
        df_taxi_all_json= Df_Taxi_All.to_json(orient='split')
    
        #Taxi out
        df_Taxi_out_T1 = None if df2_t1.empty else taxi_out(df2_t1,'T1') 
        df_Taxi_out_T2 = None if df2_t2.empty else taxi_out(df2_t2,'T2') 
        df_Taxi_out_T3 = None if df2_t3.empty else taxi_out(df2_t3,'T3') 
        df_taxi_out_all =None if df2.empty else taxi_out(df2,'ALL')
    
        df_taxi_out_all_json = df_taxi_out_all.to_json(orient='split')
    
        df_taxi_in = pd.concat([df_Taxi_in_T1,df_Taxi_in_T2,df_Taxi_in_T3]).reset_index(drop=True)
        print(df_taxi_in)
        df3_json = df_taxi_in.to_json(orient='split')
    
        df_taxi_out = pd.concat([df_Taxi_out_T1,df_Taxi_out_T2,df_Taxi_out_T3]).reset_index(drop=True)
        print(df_taxi_out)
        df4_json = df_taxi_out.to_json(orient='split')
    

    return {
       'statuscode': 200,
        'Taxi_In_Taxi_Out': json.loads(df2_json),
        'Taxi_in_distribution':json.loads(df3_json),
        'Taxi_out_distribution':json.loads(df4_json),
        'Taxi_in_ALL_distribution':json.loads(df_taxi_all_json),
        'Taxi_out_ALL_distribution':json.loads(df_taxi_out_all_json)
        
    }