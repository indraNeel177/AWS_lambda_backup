import pandas as pd
import pymysql
import json
import numpy as np
import configparser
import boto3
import io
from datetime import timedelta

def lambda_handler(event, context):
    def ExpectedFlog(x):
        if df5['start_refs'][x]=='A':
            return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['On_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        elif df5['start_refs'][x]=='D':
            if df5['ETD'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['ETD'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['Off_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))       
        else:
            return None
    
    def ExpectedTlog(x):
        if df5['end_refs'][x]=='A':
            return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['On_Block_Time'][x]))) + pd.Timedelta(minutes=df5['end'][x]))
        elif df5['end_refs'][x]=='D':
            if df5['ETD'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['ETD'][x]))) + pd.Timedelta(minutes=df5['end'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['Off_Block_Time'][x]))) + pd.Timedelta(minutes=df5['end'][x]))
        else:
            return None
    
    
    def arrivalConditions(x):
        if df5['start_refs'][x]=='A' and df5['end_refs'][x]=='A':
            return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['On_Block_Time'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['On_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        elif df5['start_refs'][x]=='A' and df5['end_refs'][x] =='D':
            if df5['ETD'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['ETD'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['On_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['Off_Block_Time'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['On_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        elif df5['start_refs'][x]=='D' and df5['end_refs'][x] =='D':
            if df5['ETD'][x] is not pd.NaT:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['ETD'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['ETD'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
            else:
                return (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['Off_Block_Time'][x]))) + pd.Timedelta(minutes=df5['end'][x])) - (pd.Timestamp(pd.Timestamp(pd.Timestamp(df5['Off_Block_Time'][x]))) + pd.Timedelta(minutes=df5['start'][x]))
        else:
            return None
    
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
    #activity_list = ['PCBA', 'PCB', 'PCDA', 'PCD', 'LAA', 'FLE', 'CAT' , 'WFG' , 'TCG' , 'CHO' , 'CHF' , 'PushBack' , 'BBF' , 'BBL' , 'LTD' , 'FTD']
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname, connect_timeout=5)
    
    OpName= int(event['OperationUnit'])
    Airline = event['Airline']
    yesterday= event['yesterday']
    #OpName=4
    #Airline=""
    #yesterday = ''
    flights = event['flights']
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Departure REGEXP '{flights}'"
    
    equipments = event['equipments']
    equipments_filter_condition = "" if equipments=="" else f" and DevId REGEXP '{equipments}'"
    
    
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
        
    #print(Operations)
    
    if OpName == 4:
        connected_bay = ('54L','54R','55L','55R','56L','56R','57L','57R','58L','58R','51','52','53','54','55','56','57','58')
    else :
        connected_bay = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22')
    Condition1 = '' if Airline =="" else f"where Airline=\'{Airline}\'"
    
    yesterday_condition = "- INTERVAL 2 DAY" if yesterday=="True" else "- INTERVAL 1 DAY"
    today_condition = "- INTERVAL 1 DAY" if yesterday=="True" else " "
    if OpName == 4 or OpName == 13:
        DFS_craft = 'Aircraft'
        FM_table = 'Hexcode'
    else :
        DFS_craft = 'AircraftRegistration_Arrival'
        FM_table = 'RegNo'
    
    df1= pd.read_sql(f"""
select Airline,FlightNumber_Departure,ToAirport_IATACode,FlightType,Bay,destination_type,On_Block_Time,Off_Block_Time,BayType,BodyType,OperationName,Duration,timestampdiff(MINUTE,On_Block_Time,flogdate) as time_arrival,timestampdiff(MINUTE,flogdate,Off_Block_Time) as time_departure,ETD,FLogDate,TLogDate,

if(Airline='Spicejet',2,if(Airline in ('Air India','Etihad','SriLankan Airlines','Alliance Air','Thai Airways','Air Arabia','Cathay Pacific','FlyDubai','Go Air','Jazeera Airways','Silk Air','Vistara'),9,4)) as Entity,aircraft_group from

(select LogId,On_Block_Time,ETD,Off_Block_Time,Airline,FlightNumber_Departure,ToAirport_IATACode,FlightType,Bay,if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as destination_type,

if(Bay in {connected_bay},'Connected','Remote') as BayType,BodyType,aircraft_group

from (select * from (select * from DailyFlightSchedule_Merged where OperationUnit={OpName} and (date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')) {yesterday_condition} and date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){today_condition} ) {airline_filter_condition}  ) as t1

left join

(select Hexcode,BodyType,aircraftType,SUBSTRING_INDEX(aircraftType, "-", 1)as aircraft_group from FlightMaster) as t2

on t1.Aircraft = t2.Hexcode) as t3

where  Off_Block_Time is not null) as t4

inner join

(select FlightNo,OperationName,Duration,flight_pk,flogdate,tlogdate from EquipActivityLogs where  Assigned=1 and operationname not in 

('ETT','PCA','CAR','PCG') and OperationName not like 'ac:%' and (duration >30 or OperationName in ('CHO','CHF'))

order by FlightNo) as t5 on t4.Logid=t5.flight_pk {Condition1} """, con=conn) if OpName ==4 or OpName == 13 else pd.read_sql(f"""
select Airline,FlightNumber_Departure,ToAirport_IATACode,AircraftRegistration_Arrival,FlightType,Bay,destination_type,On_Block_Time,Off_Block_Time,BayType,BodyType,OperationName,Duration,timestampdiff(MINUTE,On_Block_Time,flogdate) as time_arrival,timestampdiff(MINUTE,flogdate,Off_Block_Time) as time_departure,ETD,FLogDate,TLogDate,

if(Airline='Spicejet',2,if(Airline in ('Air India','Etihad','SriLankan Airlines','Alliance Air','Thai Airways','Air Arabia','Cathay Pacific','FlyDubai','Go Air','Jazeera Airways','Silk Air','Vistara'),9,4)) as Entity,aircraft_group from

(select LogId,On_Block_Time,ETD,Off_Block_Time,Airline,FlightNumber_Departure,ToAirport_IATACode,AircraftRegistration_Arrival,
FlightType,Bay,if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as destination_type,

 if(TerminalArrival in ('T3'),'Connected','Remote') as BayType,BodyType,aircraft_group

from (select * from (select * from DailyFlightSchedule_Merged where OperationUnit=22 and date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} {airline_filter_condition} and Off_Block_Time is not null) as t1

left join

(select Hexcode,REPLACE(RegNo, '-', '') as RegNo,BodyType,aircraftType,SUBSTRING_INDEX(aircraftType, "-", 1)as aircraft_group from FlightMaster) as t2

on t1.AircraftRegistration_Arrival = t2.RegNo) as t3

where date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')){yesterday_condition}) as t4

inner join

(select FlightNo,OperationName,Duration,flight_pk,flogdate,tlogdate from EquipActivityLogs where date(FLogDate)=date(ADDTIME(now(), '05:30:00')){yesterday_condition} and Assigned=1 and operationname not in 

('ETT','PCA','CAR','PCG') and OperationName not like 'ac:%' and (duration >30 or OperationName in ('CHO','CHF')) {equipments_filter_condition}

order by FlightNo) as t5 on t4.Logid=t5.flight_pk {Condition1}
""",con=conn)   
    
    print(f"""
select Airline,FlightNumber_Departure,ToAirport_IATACode,FlightType,Bay,destination_type,On_Block_Time,Off_Block_Time,BayType,BodyType,OperationName,Duration,timestampdiff(MINUTE,On_Block_Time,flogdate) as time_arrival,timestampdiff(MINUTE,flogdate,Off_Block_Time) as time_departure,ETD,FLogDate,TLogDate,

if(Airline='Spicejet',2,if(Airline in ('Air India','Etihad','SriLankan Airlines','Alliance Air','Thai Airways','Air Arabia','Cathay Pacific','FlyDubai','Go Air','Jazeera Airways','Silk Air','Vistara'),9,4)) as Entity,aircraft_group from

(select LogId,On_Block_Time,ETD,Off_Block_Time,Airline,FlightNumber_Departure,ToAirport_IATACode,FlightType,Bay,if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as destination_type,

if(Bay in {connected_bay},'Connected','Remote') as BayType,BodyType,aircraft_group

from (select * from (select * from DailyFlightSchedule_Merged where OperationUnit={OpName} and (date(On_Block_Time)=date(ADDTIME(now(), '05:30:00')) {yesterday_condition} and date(Off_Block_Time)=date(ADDTIME(now(), '05:30:00')){today_condition} ) {airline_filter_condition}  ) as t1

left join

(select Hexcode,BodyType,aircraftType,SUBSTRING_INDEX(aircraftType, "-", 1)as aircraft_group from FlightMaster) as t2

on t1.Aircraft = t2.Hexcode) as t3

where  Off_Block_Time is not null) as t4

inner join

(select FlightNo,OperationName,Duration,flight_pk,flogdate,tlogdate from EquipActivityLogs where  Assigned=1 and operationname not in 

('ETT','PCA','CAR','PCG') and OperationName not like 'ac:%' and (duration >30 or OperationName in ('CHO','CHF'))

order by FlightNo) as t5 on t4.Logid=t5.flight_pk {Condition1} 
""")
    
    df1_sub_cat = df1[['Airline', 'FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type','BayType', 'BodyType',
           'OperationName', 'Entity','aircraft_group','Bay']]
    
    
    df1_sub_cat = df1_sub_cat.fillna('Unknown')
    
    
    df1_numeric_date = df1[['On_Block_Time','ETD' ,'Off_Block_Time','Duration', 'time_arrival', 'time_departure',
           'FLogDate', 'TLogDate']]
    
    
    df3= pd.merge(df1_sub_cat,df1_numeric_date, left_index=True, right_index=True)
    if OpName == 4 or OpName == 13:
        df3['FlightType']='Domestic'
    print("ffffffffffffffffffffffff",df3.head())
    df_pts_source_merged = df3.copy()
    
    print("ggggggggggggggggggggggggggggggg",df_pts_source_merged.dtypes)
    ###############################################################################################
    #                                                                                             #
    #                                                                                             #
    #                             PTS TABLE ANS LOGIC TO FILTER aircraft_group and Entity         
    #                                                                                             #
    ###############################################################################################
    df_pts_source_merged.aircraft_group = df_pts_source_merged.aircraft_group.replace({'Unknown':'Boeing 777'})
    
    

    
    
    
    pts=pd.read_sql(f"""select Hash,Config,AirCraftType,Entity from (select * from PTSConfig ) as t1 left join (select PK.PTSConfigId,PM.Hash from PTSKey PK LEFT JOIN PTSMapping PM on PK.LogId=PM.PTSKey_logid where PM.Hash IS NOT NULL ) as t2 on t1.LogId=t2.PTSconfigId where Hash IS NOT NULL""",con=conn)

    
    #print("df_pts_source_merged",df_pts_source_merged.head())

    
    
    #pts=pd.read_sql("""select * from PTS_tabular""",con=conn)
    pts = pts.drop(index=0).reset_index(drop=True)
    
    list_AirCraftType_PTS = pts.AirCraftType.unique()
    pts_Entity_list=pts.Entity.unique()
    df_pts_source_merged['aircraft_group'] = df_pts_source_merged['aircraft_group'].apply(lambda x: 'Boeing 777' if x not in list_AirCraftType_PTS else x)
    df_pts_source_merged['Entity']=df_pts_source_merged['Entity'].apply(lambda x: 4 if x not in pts_Entity_list else x)
  
    
    for index, row in df_pts_source_merged.iterrows():
        
        airport=OpName
        airline='all'
        bodytype='all'
        flighttype='all'
        baytype='remote'
        bay='all'
    
        bodytype = str(df_pts_source_merged.iloc[index]['BodyType']).lower()
        flighttype=str(df_pts_source_merged.iloc[index]['FlightType']).lower()
        baytype = str(df_pts_source_merged.iloc[index]['BayType']).lower()
        ops = df_pts_source_merged.iloc[index]['OperationName']
        on_block_time=df_pts_source_merged.iloc[index]['On_Block_Time']
        off_block = df_pts_source_merged.iloc[index]['ETD']
        off_block_act = df_pts_source_merged.iloc[index]['Off_Block_Time']
        flog=df_pts_source_merged.iloc[index]['FLogDate']
        tlog = df_pts_source_merged.iloc[index]['TLogDate'] 
        
        if flighttype == 'unknown':
            flighttype = 'all'
    

        hashkey = "{}_{}_{}_{}_{}_{}".format(airport,airline,bodytype,flighttype,baytype,bay)
        #print("hashkey",hashkey)
        datetimeFormat = '%Y-%m-%d %H:%M:%S'
        ptsconfig=pts[pts['Hash']==hashkey]
        ptsconfig.reset_index(drop=True, inplace=True)
        
        
        if ptsconfig.empty == False:
            jsondt = ptsconfig['Config'].to_dict()[0]
            jsondat = json.loads(jsondt)
            pt_ops = jsondat.get(ops,{})["cmp"]
           # print("hhhhhhhhhhhhhhhhhhhhhhhhh",pt_ops)
    
    # start related
            start_delta = jsondat.get(ops,{})["start"]
            start_reference = jsondat.get(ops,{})["refs"]
    
    # end related
            end_delta = jsondat.get(ops,{})["end"]
            end_reference = jsondat.get(ops,{})["refe"]
        
            df_pts_source_merged.loc[df_pts_source_merged.index[index], 'start_refs'] = start_reference
            df_pts_source_merged.loc[df_pts_source_merged.index[index], 'start'] = start_delta
    
            df_pts_source_merged.loc[df_pts_source_merged.index[index], 'end_refs'] = end_reference
            df_pts_source_merged.loc[df_pts_source_merged.index[index], 'end'] = end_delta
    
    
    print("gggggggggggggggggggggggggggggg",df_pts_source_merged.head())

    

    
    #list_AirCraftType_PTS = pts.AirCraftType.unique()
    #pts_Entity_list=pts.Entity.unique()
    #df_pts_source_merged = df_pts_source_merged[df_pts_source_merged['aircraft_group'].isin(list_AirCraftType_PTS)]
    #df_pts_source_merged= df_pts_source_merged[df_pts_source_merged['Entity'].isin(pts_Entity_list)]
    #df3_grouped['aircraft_group'] = df3_grouped['aircraft_group'].apply(lambda x: 'Boeing 777' if x not in list_AirCraftType_PTS else x)
    #df3_grouped['Entity']=df3_grouped['Entity'].apply(lambda x: 4 if x not in pts_Entity_list else x)
    
    
    #df_pts_source_merged=pd.merge(df3_grouped, pts, left_on=['Entity','aircraft_group','OperationName'], right_on=['Entity','AirCraftType','operationname'],how='left')
    
    df_pts_source_merged_verify = df_pts_source_merged.copy()
   
    
    ####
    
    
    df_pts_source_merged.drop(df_pts_source_merged[(df_pts_source_merged.start_refs=='A')
                         & (df_pts_source_merged.start>0)
                         & (df_pts_source_merged.FLogDate< df_pts_source_merged.On_Block_Time)].index, inplace=True)
    
    
    #if ETD is NaT and Off_Block_Time is NaT then filter those rows
    df_pts_source_merged.drop(df_pts_source_merged[(df_pts_source_merged.ETD.isna()== True)&
                               (df_pts_source_merged.Off_Block_Time.isna()==True)].index, inplace=True)
    df_pts_source_merged['Max_TLogDate']=df_pts_source_merged.groupby(['Airline', 'FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'OperationName', 'Entity', 'aircraft_group','Bay'])['TLogDate'].transform(max)
    
    
    df_pts_source_merged_sorted = df_pts_source_merged.sort_values(by=['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'OperationName', 'Entity', 'aircraft_group','Bay','FLogDate'])
    
    
    df_pts_source_merged_sorted['Avg_Duration'] = df_pts_source_merged_sorted['Max_TLogDate']-df_pts_source_merged_sorted['FLogDate']
    
    df_pts_source_merged_sorted_non_duplicates = df_pts_source_merged_sorted.drop_duplicates(subset=['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'OperationName', 'Entity', 'aircraft_group','Bay'],keep='first')
    df5= df_pts_source_merged_sorted_non_duplicates.copy().reset_index(drop=True)
    
    x = []
    for each in df5.index:
        x.append(arrivalConditions(each))
    df_ser1 = pd.DataFrame(x, columns=['expected_Duration'])
    
    x = []
    for each in df5.index:
        x.append(ExpectedTlog(each))
    df_ser2 = pd.DataFrame(x, columns=['expected_Tlog'])
    
    x = []
    for each in df5.index:
        x.append(ExpectedFlog(each))
    df_ser3 = pd.DataFrame(x, columns=['expected_Flog'])
    
    
    df5_sub1 =pd.merge(df5,df_ser1,left_index=True,right_index=True)
    df5_sub2 =pd.merge(df5_sub1,df_ser2,left_index=True,right_index=True)
    df5_sub3 =pd.merge(df5_sub2,df_ser3,left_index=True,right_index=True)
    
    
    df6 = df5_sub3.copy()
    
    df6.to_csv("test.csv",index=False)
   
    
    df6['Expected_time_Arrival'] = df6['expected_Flog']-df6['On_Block_Time']
    
    
    df6['Expected_time_Departure'] =df6['Off_Block_Time'] - df6['expected_Flog']
    
    #print(df6['expected_Duration'])
    #print(df6['Expected_time_Arrival'])
    #print(df6['Expected_time_Departure'])
    
    df6['expected_Duration_int']=df6['expected_Duration'].dt.total_seconds().div(60) if df6.Expected_time_Arrival is not pd.NaT else None
    df6['Expected_time_Arrival_int']= df6['Expected_time_Arrival'].dt.total_seconds().div(60) if df6.Expected_time_Arrival is not pd.NaT else None
    df6['Expected_time_Departure_int']= df6['Expected_time_Departure'].dt.total_seconds().div(60) if df6.Expected_time_Departure is not pd.NaT else None
    
    df6['Avg_Duration_int']= df6['Avg_Duration'].dt.total_seconds().div(60) if df6.Avg_Duration is not None else None
    
    df6_sub = df6[['OperationName','Avg_Duration_int','expected_Duration','expected_Duration_int','time_arrival','Expected_time_Arrival_int','time_departure','Expected_time_Departure_int']]
    
    
    #start_time = pd.Timestamp('2019-01-01 00:00:00')
    #print("testttttttttttttttt",pd.to_datetime(df6['expected_Flog'][0]-start_time))
    #df6['Expected_sec_Flog_sec']= (pd.to_datetime(df6['expected_Flog']) - start_time)
    #df6['Flog_sec']= (pd.to_datetime(df6['FLogDate']) - start_time)
    #df6['Expected_sec_Flog_sec'] = df6['Expected_sec_Flog_sec'].dt.total_seconds().div(60)
    #df6['Flog_sec'] =  df6['Flog_sec'].dt.total_seconds().div(60)
    #df6['diff_min'] = (df6['expected_Flog'] - df6['FLogDate']).dt.total_seconds().div(60)
    #df6['ScheduleStatus'] = np.where(df6['diff_min']<=0, 'WithinPTS','Delayed')
    #df1_agg_1  = pd.DataFrame(df6['ScheduleStatus'].value_counts(normalize=True).round(2) * 100).reset_index()
    #df1_agg_Level1 = df1_agg_1.to_json(orient='split')
    
    #we can do this instead of that total
    #df6["test"]= (df6['expected_Flog'] - df6['FLogDate']).dt.total_seconds().div(60)
    
    
    ### Agg level 1 active list
    #df6_Active_operations = df6[df6.OperationName.isin(activity_list)]
    df6_Active_operations= df6.copy()
    #start_time = pd.Timestamp('2019-01-01 00:00:00')
    #df6_Active_operations['Expected_sec_Flog_sec']= (pd.to_datetime(df6_Active_operations['expected_Flog']) - start_time)
    #df6_Active_operations['Flog_sec']= (pd.to_datetime(df6_Active_operations['FLogDate']) - start_time)
    #df6_Active_operations['Expected_sec_Flog_sec'] = df6_Active_operations['Expected_sec_Flog_sec'].dt.total_seconds().div(60)
    #df6_Active_operations['Flog_sec'] =  df6_Active_operations['Flog_sec'].dt.total_seconds().div(60)
    #df6_Active_operations['diff_min'] = df6_Active_operations['Expected_sec_Flog_sec']-df6_Active_operations['Flog_sec']
    
    df6_Active_operations['diff_min'] = (df6_Active_operations['expected_Flog']-df6_Active_operations['FLogDate']).dt.total_seconds().div(60)
    
    df6_Active_operations['ScheduleStatus'] = np.where(df6_Active_operations['diff_min']<=0, 'WithinPTS','Delayed')
    df1_agg_Active_opp = pd.DataFrame(df6_Active_operations['ScheduleStatus'].value_counts(normalize=True).round(2) * 100).reset_index()
    df1_agg_Level1 = df1_agg_Active_opp.to_json(orient='split')
    
    
    ################ departure agg#############
    
    #start_time = pd.Timestamp('2019-01-01 00:00:00')
    #df6['Expected_sec_Tlog_sec']= (pd.to_datetime(df6['expected_Tlog']) - start_time)
    #df6['Tlog_sec']= (pd.to_datetime(df6['TLogDate']) - start_time)
    #df6['Expected_sec_Tlog_sec'] = df6['Expected_sec_Tlog_sec'].dt.total_seconds().div(60)
    #df6['Tlog_sec'] =  df6['Tlog_sec'].dt.total_seconds().div(60)
    #df6['diff_min_Tlog'] = df6['Expected_sec_Tlog_sec']-df6['Tlog_sec']
    df6['diff_min_Tlog'] = (df6_Active_operations['expected_Tlog']-df6_Active_operations['TLogDate']).dt.total_seconds().div(60)
    df6['ScheduleStatus_Tlog'] = np.where(df6['diff_min_Tlog']<=0, 'WithinPTS','Delayed')
    df1_agg_Tlog  = pd.DataFrame(df6['ScheduleStatus_Tlog'].value_counts(normalize=True).round(2) * 100).reset_index()
    
    ############## end #########
    
    ### Duration agg ###########
    #start_time = pd.Timestamp('2019-01-01 00:00:00')
    #df6['expected_Duration_sec']= (pd.to_datetime(df6['expected_Duration']) - start_time)
    #df6['Avg_Duration_sec']= (pd.to_datetime(df6['Avg_Duration']) - start_time)
    #df6['expected_Duration_sec'] = df6['expected_Duration_sec'].dt.total_seconds().div(60)
    #df6['Avg_Duration_sec'] =  df6['Avg_Duration_sec'].dt.total_seconds().div(60)
    #df6['diff_min_Duration'] = df6['expected_Duration_sec']-df6['Avg_Duration_sec']
    df6['diff_min_Duration'] = (df6_Active_operations['expected_Duration']-df6_Active_operations['Avg_Duration']).dt.total_seconds().div(60)
    df6['ScheduleStatus_Duration'] = np.where(df6['diff_min_Duration']<=0, 'WithinPTS','Delayed')
    df1_agg_Duration  = pd.DataFrame(df6['ScheduleStatus_Duration'].value_counts(normalize=True).round(2) * 100).reset_index()
    
    #df6_Active_operations.to_csv("test.csv")
    
    
    ### night halt ontime airline level-1
    
    turnaround_level1 = df6_Active_operations.copy()
    turnaround_level1['ScheduleStatus_final'] = np.where(turnaround_level1['ScheduleStatus']=="WithinPTS",1,0)

    turnaround_level1_groupby=turnaround_level1.groupby(['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'Entity', 'aircraft_group'])['ScheduleStatus_final'].agg(['count','sum']).reset_index()
    turnaround_level1_groupby['ontime_count']=((turnaround_level1_groupby["sum"]/turnaround_level1_groupby["count"])*100).round(2)
    turnaround_level1_final=turnaround_level1_groupby[['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'Entity', 'aircraft_group','ontime_count']]
    turnaround_level1_output = turnaround_level1_final.to_json(orient='split')
    
    
    ### night halt ontime airline level-2
    
    turnaround_level2 = df6_Active_operations.copy()
    turnaround_level2_filter=turnaround_level2[['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'OperationName', 'Entity', 'aircraft_group','Bay','FLogDate','expected_Flog']]
    turnaround_level2_filter['FLogDate'] = pd.to_datetime(turnaround_level2_filter['FLogDate'])
    turnaround_level2_filter['expected_Flog'] = pd.to_datetime(turnaround_level2_filter['expected_Flog'])
    turnaround_level2_groupby=turnaround_level2_filter.groupby(['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type','BayType', 'BodyType', 'Entity', 
               'aircraft_group']).agg({'FLogDate':np.max,'expected_Flog':np.max}).reset_index()
    turnaround_level2_groupby.dropna(subset=["FLogDate","expected_Flog"])
    turnaround_level2_output = turnaround_level2_groupby.to_json(orient='split')
    
    
    ### night halt ontime airline level-2
    turnaround_level3 = df6_Active_operations.copy()
    
    turnaround_level3_filter=turnaround_level3[['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'OperationName', 'Entity', 'aircraft_group','Bay','FLogDate','expected_Flog']]
    turnaround_level3_filter.dropna(subset=["FLogDate","expected_Flog"])
    turnaround_level3_filter['FLogDate'] = pd.to_datetime(turnaround_level3_filter['FLogDate'])
    turnaround_level3_filter['expected_Flog'] = pd.to_datetime(turnaround_level3_filter['expected_Flog'])
    
    turnaround_level3_sort = turnaround_level3_filter.sort_values(by=['Airline','FlightNumber_Departure', 'ToAirport_IATACode','FlightType', 'destination_type',
           'BayType', 'BodyType', 'OperationName', 'Entity', 'aircraft_group','Bay','FLogDate','expected_Flog'])
    turnaround_level3_output = turnaround_level3_sort.to_json(orient='split')

    
    
    return {
        "statusCode": 200,
        'Agg_Equip_to_Flight': json.loads(df1_agg_Level1),
        'turnaround_level1_output' : json.loads(turnaround_level1_output),#d6_name_change # its arrival
        'turnaround_level2_output' : json.loads(turnaround_level2_output),#df_airline
        'turnaround_level3_output' : json.loads(turnaround_level3_output)
    }