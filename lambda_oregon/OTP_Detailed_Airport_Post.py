import json
import boto3
import pymysql
import pandas as pd
import configparser
import io


def lambda_handler(event, context):
#nconnection links 
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
    
    TypeofUser = event['TypeofUser']
    OpName=event['OperationUnit']
    AirlineName = event['Airline']
    yesterday=event['yesterday']
    #AirlineName = "SpiceJet"
    
    #OpName=22
    #AirlineName="SpiceJet"
    #yesterday = "True"
    
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    ### Airline arrival OTP with magnitude calculation ### starts here ##########
    
    Condition1 = '' if AirlineName =="" else f"and Airline=\'{AirlineName}\'"
    
    flights = event['flights']
    
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    
    
    #ToAirport_IATACode|ToAirport_ICAOCode|FromAirport_IATACode
    df1_Arrival= pd.read_sql(f"""
    select 
    On_Block_Time,STA ,FromAirport_IATACode
    ,IFNULL(TerminalArrival,'DISABLED') as TerminalArrival,
    Airline,FlightNumber_Arrival from DailyFlightSchedule_Merged where OperationUnit={OpName} and
    (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition}) and On_Block_Time is not null {Condition1}
     {airline_filter_condition}
    
    """,con=conn) if (OpName=='4' or OpName=='13') else  pd.read_sql(f"""
    select 
    On_Block_Time,STA ,FromAirport_IATACode
    ,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival,
    Airline,FlightNumber_Arrival from DailyFlightSchedule_Merged where OperationUnit={OpName} and
    (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})  {Condition1} having TerminalArrival in ('T1','T2','T3') 
      {airline_filter_condition}
    """,con=conn) 
    
    df1_Arrival['diff']=pd.to_datetime(df1_Arrival['On_Block_Time'])- pd.to_datetime(df1_Arrival['STA'])
    df1_Arrival['diff_minutes_int']=df1_Arrival['diff'].dt.total_seconds().div(60).round(0)
    df1_Arrival1 = df1_Arrival.drop(['On_Block_Time','STA','diff'], axis=1)
    
    df1_Arrival = df1_Arrival1.copy() if AirlineName =="" else df1_Arrival1[df1_Arrival1['Airline']==AirlineName]
    
    df1_Arrival= df1_Arrival[['Airline', 'FlightNumber_Arrival', 'diff_minutes_int',
           'FromAirport_IATACode','TerminalArrival']]
    
    df1_Arrival_json = df1_Arrival.to_json(orient='split')
    
    ### Airline Arrival OTP with magnitude calculation ### ends here ##########
    
    ### Airline Departure OTP with magnitude calculation ### starts here ##########
    
    
    airline_dept_filter_condition= "" if flights=="" else f" and FlightNumber_Departure REGEXP '{flights}'"
    
    df1_Departure= pd.read_sql(f"""
    select 
    Off_Block_Time,STD,ToAirport_IATACode
    ,IFNULL(TerminalArrival,'DISABLED') as TerminalArrival,
    Airline,FlightNumber_Departure from DailyFlightSchedule_Merged where OperationUnit={OpName} and
    (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition}) and Off_Block_Time is not null  {Condition1}
    {airline_dept_filter_condition}
    """, con=conn) if (OpName=='4' or OpName=='13') else pd.read_sql(f"""
    select 
    Off_Block_Time,STD,ToAirport_IATACode
    ,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival,
    Airline,FlightNumber_Departure from DailyFlightSchedule_Merged where OperationUnit={OpName} and
    (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition}) and Off_Block_Time is not null {Condition1} having TerminalArrival in ('T1','T2','T3')  
    {airline_dept_filter_condition}
    """, con=conn)
    
    df1_Departure['diff']=pd.to_datetime(df1_Departure['Off_Block_Time'])- pd.to_datetime(df1_Departure['STD'])
    df1_Departure['diff_minutes_int']=df1_Departure['diff'].dt.total_seconds().div(60).round(0)
    df1_Departure1 = df1_Departure.drop(['Off_Block_Time','STD','diff'], axis=1)
    
    df1_Departure = df1_Departure1.copy() if AirlineName =="" else df1_Departure1[df1_Departure1['Airline']==AirlineName]
    
    df1_Departure= df1_Departure[['Airline', 'FlightNumber_Departure', 'diff_minutes_int','ToAirport_IATACode','TerminalArrival']]
    
    df1_Departure = df1_Departure[df1_Departure['FlightNumber_Departure'].notnull()].reset_index(drop=True)
    
    df1_Departure_json = df1_Departure.to_json(orient='split')
    
    ### Airline Departure OTP with magnitude calculation ### ends here ##########
    
    
    ###################### starts here #####################
    df3_1= pd.read_sql(f"""
    select hour(On_Block_Time) as hour
    ,Airline
    ,(sum(OTP_A0)/COUNT(OTP_A0))*100 as OTP_A00 
    ,(sum(OTP_A15)/COUNT(OTP_A15))*100 as OTP_A15
    ,(sum(OTP_D0)/COUNT(OTP_D0))*100 as OTP_D00
    ,(sum(OTP_D15)/COUNT(OTP_D15))*100 as OTP_D15
    ,IFNULL(TerminalArrival,'DISABLED') as TerminalArrival
    from (
    SELECT
    if(On_Block_Time >STA,
    0,
    1) as OTP_A0,
    if(On_Block_Time >DATE_ADD(STA,
    INTERVAL 15 MINUTE),
    0,
    1) as OTP_A15,
    if(Off_Block_Time >STD,
    0,
    1) as OTP_D0,
    if(Off_Block_Time >DATE_ADD(STD,
    INTERVAL 15 MINUTE),
    0,
    1) as OTP_D15,
    Airline,
    On_Block_Time,Off_Block_Time,TerminalArrival
    
    FROM
    AviLeap.DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition}
    ) as t1 where (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})  and Off_Block_Time is not null  group by airline, hour(On_Block_Time)
    """, con=conn) if (OpName=='4' or OpName=='13') else pd.read_sql(f"""
    select hour(On_Block_Time) as hour
    ,Airline
    ,(sum(OTP_A0)/COUNT(OTP_A0))*100 as OTP_A00 
    ,(sum(OTP_A15)/COUNT(OTP_A15))*100 as OTP_A15
    ,(sum(OTP_D0)/COUNT(OTP_D0))*100 as OTP_D00
    ,(sum(OTP_D15)/COUNT(OTP_D15))*100 as OTP_D15
    ,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival
    from (
    SELECT
    if(On_Block_Time >STA,
    0,
    1) as OTP_A0,
    if(On_Block_Time >DATE_ADD(STA,
    INTERVAL 15 MINUTE),
    0,
    1) as OTP_A15,
    if(Off_Block_Time >STD,
    0,
    1) as OTP_D0,
    if(Off_Block_Time >DATE_ADD(STD,
    INTERVAL 15 MINUTE),
    0,
    1) as OTP_D15,
    Airline,
    On_Block_Time,Off_Block_Time,TerminalArrival
    FROM
    AviLeap.DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition}
    ) as t1 where (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition}) 
    and TerminalArrival in ('T1','T2','T3')  {Condition1}
    group by airline, hour(On_Block_Time),TerminalArrival
    """, con=conn)
    
    df3 = df3_1.copy() if AirlineName =="" else df3_1[df3_1['Airline']==AirlineName]
    df3=df3[['hour', 'Airline', 'OTP_A00', 'OTP_A15', 'OTP_D00', 'OTP_D15','TerminalArrival']]
    df3_json = df3.to_json(orient='split')
    
    ###################### ends here #####################
    
    
    
    df1_M_NM= pd.read_sql(f"""
    select Airline,flightnumber_arrival,
    On_Block_Time,STA,
    if(FromAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as origin,
    flightnumber_departure,
    Off_Block_Time,STD,
    if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as destination,
    ToAirport_IATACode,FromAirport_IATACode
    ,IFNULL(TerminalArrival,'DISABLED') as TerminalArrival
    from DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition} and 
    (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})   {Condition1}
    
    """, con=conn) if (OpName=='4' or OpName=='13') else pd.read_sql(f"""
    select Airline,flightnumber_arrival,
    On_Block_Time,STA,
    if(FromAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA'), 'Metro', 'Non-Metro') as origin,
    flightnumber_departure,
    Off_Block_Time,STD,
    if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro') as destination,
    ToAirport_IATACode,FromAirport_IATACode
    ,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival
    from DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition} and flightnumber_departure is not null and 
    (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})   {Condition1} having TerminalArrival in ('T1','T2','T3') 
    
    """, con=conn)
    
    
    df1_M_NM['diff_arrival']=pd.to_datetime(df1_M_NM['On_Block_Time'])-pd.to_datetime(df1_M_NM['STA'])
    df1_M_NM['diff_departure']=pd.to_datetime(df1_M_NM['Off_Block_Time'])-pd.to_datetime(df1_M_NM['STD'])
    
    df1_M_NM['diff_arrival_minutes_int']=df1_M_NM['diff_arrival'].dt.total_seconds().div(60).round(0)
    df1_M_NM['diff_departure_minutes_int']=df1_M_NM['diff_departure'].dt.total_seconds().div(60).round(0)
    
    df1_M_NM_1 = df1_M_NM.drop(['On_Block_Time','STA','Off_Block_Time','STD','diff_arrival','diff_departure'], axis=1)
    
    
    df1_M_NM = df1_M_NM_1.copy() if AirlineName =="" else df1_M_NM_1[df1_M_NM_1['Airline']==AirlineName]
    
    df1_M_NM=df1_M_NM[['Airline', 'flightnumber_arrival', 'origin', 'flightnumber_departure',
           'destination', 'diff_arrival_minutes_int', 'diff_departure_minutes_int',
           'ToAirport_IATACode', 'FromAirport_IATACode','TerminalArrival']]
    
    
    df1_M_NM_json = df1_M_NM.to_json(orient='split')
    
    ###################### Metro Non Metro endds here ends here #####################
    
    ###################### starts here #####################
    
    #ToAirport_IATACode|ToAirport_ICAOCode|FromAirport_IATACode
    
    df_bodytype= pd.read_sql(f"""
    select t1.*,t2.Bodytype as bodytype from ( select Airline,flightnumber_arrival,Aircraft,flightnumber_departure,ToAirport_IATACode,
        FromAirport_IATACode,On_Block_Time,STA,Off_Block_Time,STD,IFNULL(TerminalArrival,'DISABLED') as TerminalArrival
        from
        DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition} and 
        (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})   {Condition1}) as t1 left join
        (select hexcode,Bodytype from FlightMaster) as t2 on t1.Aircraft=t2.hexcode
    
    """, con=conn) if (OpName=='4' or OpName=='13') else pd.read_sql(f"""
    select t1.*,t2.Bodytype as bodytype from ( select Airline,flightnumber_arrival,Aircraft,ToAirport_IATACode,flightnumber_departure,
        FromAirport_IATACode,On_Block_Time,STA,
        Off_Block_Time,STD,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival,AircraftRegistration_Arrival
        from
        DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition} and 
        (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})  {Condition1}) as t1 left join
        (select hexcode,Bodytype,REPLACE(RegNo, '-', '') as RegNo from FlightMaster)
        as t2 on t1.AircraftRegistration_Arrival=t2.RegNo having TerminalArrival in ('T1','T2','T3')
    """, con=conn) 
    
    df_bodytype['diff_arrival']=pd.to_datetime(df_bodytype['On_Block_Time'])-pd.to_datetime(df_bodytype['STA'])
    df_bodytype['diff_departure']=pd.to_datetime(df_bodytype['Off_Block_Time'])-pd.to_datetime(df_bodytype['STD'])
    
    df_bodytype['diff_arrival_minutes_int']=df_bodytype['diff_arrival'].dt.total_seconds().div(60).round(0)
    df_bodytype['diff_departure_minutes_int']=df_bodytype['diff_departure'].dt.total_seconds().div(60).round(0)
    
    df_bodytype_1 = df_bodytype.drop(['On_Block_Time','STA','Off_Block_Time','STD','diff_arrival','diff_departure'], axis=1)
    
    
    df_bodytype = df_bodytype_1.copy() if AirlineName =="" else df_bodytype_1[df_bodytype_1['Airline']==AirlineName]
    #df_bodytype_json = df_bodytype.to_json(orient='split')
    
    #Narrow body code
    df_bodytype_narrow= df_bodytype[df_bodytype.bodytype =='Narrow']
    
    df_bodytype_narrow = df_bodytype_narrow.drop(columns={'Aircraft'}, axis=1)
    
    df_bodytype_narrow=df_bodytype_narrow[['Airline', 'flightnumber_arrival', 'bodytype', 'flightnumber_departure',
           'diff_arrival_minutes_int', 'ToAirport_IATACode','TerminalArrival','diff_departure_minutes_int','FromAirport_IATACode']]
    
    df_bodytype_narrow_json= df_bodytype_narrow.dropna().to_json(orient='split')
    
    #bombardier code
    df_bodytype_TurboProp= df_bodytype[df_bodytype.bodytype.isin(['TurboProp','Bombardier'])]
    
    df_bodytype_TurboProp=df_bodytype_TurboProp[['Airline', 'flightnumber_arrival', 'bodytype','flightnumber_departure',
           'diff_arrival_minutes_int', 'ToAirport_IATACode','TerminalArrival','diff_departure_minutes_int','FromAirport_IATACode']]
    
    df_bodytype_TurboProp_json= df_bodytype_TurboProp.to_json(orient='split')
    
    
    ###################### ends here #####################
    
    return {
        'statusCode': 200,
        'opname': OpName,
        'AirlineName':AirlineName,
        'OTP_Arrival': json.loads(df1_Arrival_json),
        'OTP_Departure': json.loads(df1_Departure_json),
        'OTP_Hour': json.loads(df3_json),
        'OTP_M_NM_Arrival_Departure': json.loads(df1_M_NM_json),
        'OTP_Narrow_A_D': json.loads(df_bodytype_narrow_json),
        'OTP_TurboPrep_A_D': json.loads(df_bodytype_TurboProp_json)
    }
    
    
    ###################### ends here #####################
