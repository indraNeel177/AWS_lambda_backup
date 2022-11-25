import json
import pandas as pd
import pymysql
import configparser
import boto3
import io
import numpy as np
def lambda_handler(event,context):
    #events from post call
    TypeofUser=event['TypeofUser']
    OpName  = int(event['OperationUnit'])# OperationUnit = 4
    Airline = event['Airline']
    
    metro_names_hyd = ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') 
    connected_bay_names_hyd = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
    
    
    #OpName = 22
    #Airline =""
    yesterday= event['yesterday']
    #yesterday = "True"
    
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    s3 = boto3.resource('s3')
    obj = s3.Object('zestiot-resources','db_creds_ini/db_creds_prod_read_replica.ini')
    config_file_buffer = io.StringIO(obj.get()['Body'].read().decode('utf-8'))
    configParser = configparser.ConfigParser()
    configParser.readfp(config_file_buffer)
    hostname = configParser.get('db-creds-prod', 'hostname')
    jdbcPort = configParser.get('db-creds-prod', 'jdbcPort')
    username = configParser.get('db-creds-prod', 'username')
    password = configParser.get('db-creds-prod', 'password')
    dbname = configParser.get('db-creds-prod', 'dbname')
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)
    
    
    condition1= '' if Airline=="" else f",airline"
    condition2 = '' if Airline=="" else f"and airline='{Airline}'"
    
    if OpName == 13 or OpName == 4 :
        df1= pd.read_sql(f"""
    select Bay, OperationUnit,Airline,FlightNumber_Arrival,FlightNumber_Departure,TerminalArrival,Runway_Arrival,Runway_Departure,
    timestampdiff(MINUTE,Sensor_ATA,On_Block_Time) as Taxi_in,
    timestampdiff(MINUTE,Off_Block_Time,Sensor_ATD) as Taxi_out 
    from DailyFlightSchedule_Merged
    where 
    Sensor_ATA is not null and On_Block_Time is not null and 
    Off_Block_Time is not null and Sensor_ATD is not null and 
    Sensor_ATA !='' and On_Block_Time !='' and 
    Off_Block_Time !='' and Sensor_ATD !='' and(
    date(Sensor_ATA)=date(ADDTIME(now(), '05:30:00'))
    or date(Sensor_ATA)=date(ADDTIME(now(),'00:05:30')) or date(STA)=date(ADDTIME(now(),'00:05:30')) or date(ETA)=date(ADDTIME(now(),'00:05:30')) or date(STD)=date(ADDTIME(now(),'00:05:30')){yesterday_condition} )and OperationUnit={OpName}
    {condition2}{airline_filter_condition}
    group by TerminalArrival,Bay,FlightNumber_Arrival
    """,con=conn)
    else :
        df1= pd.read_sql(f"""
    select Bay, OperationUnit,Airline,FlightNumber_Arrival,FlightNumber_Departure,TerminalArrival,Runway_Arrival,Runway_Departure,
    timestampdiff(MINUTE,ATA,On_Block_Time) as Taxi_in,
    timestampdiff(MINUTE,Off_Block_Time,ATD) as Taxi_out 
    from DailyFlightSchedule_Merged
    where 
    ATA is not null and On_Block_Time is not null and 
    Off_Block_Time is not null and ATD is not null and 
    ATA !='' and On_Block_Time !='' and 
    Off_Block_Time !='' and ATD !='' and
    date(ATA)=date(ADDTIME(now(), '05:30:00'))
    or date(ATA)=date(ADDTIME(now(),'00:05:30')) or date(STA)=date(ADDTIME(now(),'00:05:30')) or date(ETA)=date(ADDTIME(now(),'00:05:30')) or date(STD)=date(ADDTIME(now(),'00:05:30')){yesterday_condition} and OperationUnit={OpName}
    {condition2}{airline_filter_condition}
    group by TerminalArrival,Bay,FlightNumber_Arrival
    """,con=conn)
        
                         
    
    print(f"""select Bay, OperationUnit,Airline,FlightNumber_Arrival,FlightNumber_Departure,TerminalArrival,Runway_Arrival,Runway_Departure,
    timestampdiff(MINUTE,ATA,On_Block_Time) as Taxi_in,
    timestampdiff(MINUTE,Off_Block_Time,ATD) as Taxi_out 
    from DailyFlightSchedule_Merged
    where 
    ATA is not null and On_Block_Time is not null and 
    Off_Block_Time is not null and ATD is not null and 
    ATA !='' and On_Block_Time !='' and 
    Off_Block_Time !='' and ATD !='' and
    date(ATA)=date(ADDTIME(now(), '05:30:00'))
    or date(ATA)=date(ADDTIME(now(),'00:05:30')) or date(STA)=date(ADDTIME(now(),'00:05:30')) or date(ETA)=date(ADDTIME(now(),'00:05:30')) or date(STD)=date(ADDTIME(now(),'00:05:30')){yesterday_condition} and OperationUnit={OpName}
    {condition2}{airline_filter_condition}
    group by OperationUnit,TerminalArrival,Bay,FlightNumber_Arrival""")
    
    df1.Taxi_in = df1.Taxi_in.fillna(round(df1.Taxi_in.mean(),1))
    df1.Taxi_out = df1.Taxi_out.fillna(round(df1.Taxi_out.mean(),1))
    
    df1.TerminalArrival=np.where(df1.TerminalArrival.isin(['1C','1D']), 'T1',df1.TerminalArrival)
    df1=df1[~(df1.TerminalArrival.isin(['F','X','Z']))]
    df1.TerminalArrival= df1.TerminalArrival.fillna('ALL')
    df2 = df1[abs(df1.Taxi_out-df1.Taxi_out.mean()) <= abs(3*df1.Taxi_out.std())]
    df2 = df2.reset_index(drop=True)
#     df2 = df2[df2['OperationUnit'] == 13]
    df2=df2[(df2['Taxi_in']>1) & (df2['Taxi_out']>1)]
    df2 = df2[df2['OperationUnit']==OpName]
    
    df_2_group = df2.groupby(['Bay', 'OperationUnit',
                              'Airline','FlightNumber_Arrival','TerminalArrival',
                              'FlightNumber_Departure']).agg('mean').reset_index().round(1)
   # print("hhhhhhhhh",df_2_group.head())
    #if OpName==13 or OpName == 4:
    #    df_2_group['TerminalArrival']='DISABLED'
    df_2_group['TerminalArrival']='DISABLED' if OpName==13 or OpName == 4 else df_2_group['TerminalArrival']
   # print("ggggggggggggg",df_2_group['TerminalArrival'].head())
    df_2_group['Airline'] =  df_2_group['Airline']
    df_2_group['FlightNumber_Arrival'] = df_2_group['FlightNumber_Arrival'] 
    df_2_group['FlightNumber_Departure'] = df_2_group['FlightNumber_Departure']
#     df_2_group['Runway_Arrival'] = df_2_group['Runway_Arrival'] 
#     df_2_group['Runway_Departure'] = df_2_group['Runway_Departure'] 
#     df_2_group= df_2_group[['Bay', 'OperationUnit', 
#            'Airline', 'FlightNumber_Arrival','TerminalArrival','Taxi_in', 'Taxi_out','FlightNumber_Departure','Runway_Arrival','Runway_Departure']]
    df_2_group= df_2_group[['Bay', 'OperationUnit', 
           'Airline', 'FlightNumber_Arrival','TerminalArrival','Taxi_in', 'Taxi_out','FlightNumber_Departure']]  
    df2_json = df_2_group.to_json(orient='split')
   
    
    df2['Bay']=df2['Bay'].astype('category')
    
    #taxi in distribution
    
    if OpName==13:
        step_Taxi_in = int((df2.Taxi_in.max() -df2.Taxi_in.min()))
        step_div = step_Taxi_in/2
        list_of_ints_taxi_in=list(range(int(df2.Taxi_in.min()),int(df2.Taxi_in.max()),int((step_Taxi_in/step_div))))
    
        classes_taxi_in = pd.cut(df2.Taxi_in,int(list_of_ints_taxi_in[0]),precision=0,right=True,include_lowest=True)
    
        df2_1 = pd.DataFrame(pd.value_counts(classes_taxi_in)).reset_index()
        df3 = df2_1.rename(columns={'index':'class','Taxi_in':'Number of Bays'})
    
        labels_taxi_in=[]
        for each in df3['class']:
            labels_taxi_in.append(f'from {each.left} to {each.right}')
    
        classes_taxi_in = pd.cut(df2.Taxi_in,int(list_of_ints_taxi_in[0]),precision=0,labels=labels_taxi_in,right=True,include_lowest=True) #int(list_of_ints_taxi_in[0])
        df2_1 = pd.DataFrame(pd.value_counts(classes_taxi_in)).reset_index()
        df3 = df2_1.rename(columns={'index':'class','Taxi_in':'Number of Bays'})
    
    
        #classes_taxi_in = pd.cut(df2.Taxi_in,10)
        #df2_1 = pd.DataFrame(pd.value_counts(classes_taxi_in)).reset_index()    
        #df3 = df2_1.rename(columns={'index':'class','Taxi_in':'Number of Bays'})
        #df3['class']= df3['class'].astype('str')
    
        df3['TerminalArrival']='DISABLED'
        df3_json = df3.to_json(orient='split')
        Df_Taxi_All_json =  df3.to_json(orient='split')
    
        #classes_taxi_out = pd.cut(df2.Taxi_out,10)
        #df2_2 = pd.DataFrame(pd.value_counts(classes_taxi_out)).reset_index()
        #df3_out = df2_2.rename(columns={'index':'class','Taxi_out':'Number of Bays'})
        #df3_out['class']= df3_out['class'].astype('str')
    
     
        step_Taxi_out = int((df2.Taxi_out.max() -df2.Taxi_out.min()))
        step_div = step_Taxi_out/2
        list_of_ints_taxi_out=list(range(int(df2.Taxi_out.min()),int(df2.Taxi_out.max()),int((step_Taxi_out/step_div))))
    
        classes_taxi_out_1 = pd.cut(df2.Taxi_out,int(list_of_ints_taxi_out[0]),precision=0,right=True,include_lowest=True)#from here list_of_ints_taxi_out to -->> int(list_of_ints_taxi_out[0])
    
    
        df2_2_1 = pd.DataFrame(pd.value_counts(classes_taxi_out_1)).reset_index()
        df3_out = df2_2_1.rename(columns={'index':'class','Taxi_out':'Number of Bays'})
    
        labels_taxi_out=[]
        for each in df3_out['class']:
            labels_taxi_out.append(f'from {each.left} to {each.right}')
    
        classes_taxi_out = pd.cut(df2.Taxi_out,int(list_of_ints_taxi_out[0]),precision=0,labels=labels_taxi_out,right=True,include_lowest=True)
        df2_2 = pd.DataFrame(pd.value_counts(classes_taxi_out)).reset_index()
        df3_out = df2_2.rename(columns={'index':'class','Taxi_out':'Number of Bays'})
        #df3_out['TerminalArrival']='DISABLED'
    
        df4_json = df3_out.to_json(orient='split')
        Df_Taxi_out_All_json =  df3_out.to_json(orient='split')
    else:
    
        def Taxi_in(df,terminal):  
            global df_class
            step_Taxi_in = int((df.Taxi_in.max() -df.Taxi_in.min()))
            step_div = step_Taxi_in/2
            list_of_ints_taxi_in=list(range(int(df.Taxi_in.min()),int(df.Taxi_in.max()),int((step_Taxi_in/step_div))))
            classes_taxi_in_1 = pd.cut(df.Taxi_in,list_of_ints_taxi_in,precision=0,right=True,include_lowest=True)
    
            df2_1_classes = pd.DataFrame(pd.value_counts(classes_taxi_in_1)).reset_index()
            df_class = df2_1_classes.rename(columns={'index':'class','Taxi_in':'Number of Bays'})
    
            labels_taxi_in=[]
            for each in df_class['class']:
                labels_taxi_in.append(f'from {each.left} to {each.right}')
    
            df_class['class'] = labels_taxi_in
    
            df_class['TerminalArrival']= terminal
            return df_class
    
    
        def Taxi_out(df,terminal):  
            global df_class
            step_Taxi_out = int((df.Taxi_out.max() -df.Taxi_out.min()))
            step_div = step_Taxi_out/2
            list_of_ints_taxi_out=list(range(int(df.Taxi_out.min()),int(df.Taxi_out.max()),int((step_Taxi_out/step_div))))
            classes_taxi_out_1 = pd.cut(df.Taxi_out,list_of_ints_taxi_out,precision=0,right=True,include_lowest=True)
    
            df2_1_classes = pd.DataFrame(pd.value_counts(classes_taxi_out_1)).reset_index()
            df_class = df2_1_classes.rename(columns={'index':'class','Taxi_out':'Number of Bays'})
    
            labels_taxi_out=[]
            for each in df_class['class']:
                labels_taxi_out.append(f'from {each.left} to {each.right}')
    
            df_class['class'] = labels_taxi_out
    
            df_class['TerminalArrival']= terminal
            return df_class
    
        #terminal value
        df2_t1=df2[df2.TerminalArrival=='T1'].reset_index(drop=True)
        df2_t2=df2[df2.TerminalArrival=='T2'].reset_index(drop=True)
        df2_t3=df2[df2.TerminalArrival=='T3'].reset_index(drop=True)
    
        #taxi in 
        df_Taxi_in_T1 = None if df2_t1.empty else  Taxi_in(df2_t1,'T1')
        df_Taxi_in_T2 = None if df2_t2.empty else  Taxi_in(df2_t2,'T2')
        df_Taxi_in_T3 = None if df2_t3.empty else  Taxi_in(df2_t3,'T3')
        Df_Taxi_All = None if df2.empty else  Taxi_in(df2,'ALL')
        Df_Taxi_All_json= Df_Taxi_All.to_json(orient='split')
    
        #Taxi out
        df_Taxi_out_T1 = None if df2_t1.empty else Taxi_out(df2_t1,'T1') 
        df_Taxi_out_T2 = None if df2_t2.empty else Taxi_out(df2_t2,'T2') 
        df_Taxi_out_T3 = None if df2_t3.empty else Taxi_out(df2_t3,'T3') 
        Df_Taxi_out_All =None if df2.empty else Taxi_out(df2,'ALL')
        
        Df_Taxi_out_All_json = Df_Taxi_out_All.to_json(orient='split')
    
        df_Taxi_in = pd.concat([df_Taxi_in_T1,df_Taxi_in_T2,df_Taxi_in_T3]).reset_index(drop=True)
    
        df3_json = df_Taxi_in.to_json(orient='split')
    
        df_Taxi_out = pd.concat([df_Taxi_out_T1,df_Taxi_out_T2,df_Taxi_out_T3]).reset_index(drop=True)
    
        df4_json = df_Taxi_out.to_json(orient='split')

    return {
       'statusCode': 200,
        'Taxi_In_Taxi_Out': json.loads(df2_json),
        'Taxi_in_distribution':json.loads(df3_json),
        'Taxi_out_distribution':json.loads(df4_json),
        'Taxi_in_ALL_distribution':json.loads(Df_Taxi_All_json),
        'Taxi_out_ALL_distribution':json.loads(Df_Taxi_out_All_json)
    }