import json
import boto3
import pymysql
import pandas as pd
import configparser
import boto3
import io




#client = boto3.client('rds')
def lambda_handler(event, context):
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
    
    #events
    TypeofUser=event['TypeofUser']
    OpName=int(event['OperationUnit'])
    AirlineName=event['Airline']
    yesterday=event['yesterday']
    
    #yesterday = ""
    yesterday_condition = "- INTERVAL 1 DAY" if yesterday=="True" else ""
    
    #test events
    #OpName =22
    #AirlineName = ""
    
    #airline condition
    Condition1 = '' if AirlineName =="" else f"and Airline=\'{AirlineName}\'"
    
    flights = event['flights']
    
    airline_filter_condition= "" if flights=="" else f" and FlightNumber_Arrival REGEXP '{flights}'"
    
    #overall OTP and bodytype OTP logic
    
    if (OpName==4 or OpName==13) :
        df_parent_og = pd.read_sql(f"""
    /*1_3*/
    /*all flights and body*/
    select
    Aircraft,
    OperationUnit,
    Airline,AircraftRegistration_Arrival,if(On_Block_Time >STA,0,1) as OTP_A00,if(On_Block_Time >DATE_ADD(STA,INTERVAL 15 MINUTE),0,1) as OTP_A15,
    if(Off_Block_Time >STD,0,1) as OTP_D00,if(Off_Block_Time >DATE_ADD(STD,INTERVAL 15 MINUTE),0,1) as OTP_D15,if(FromAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA'),'Metro','Non-Metro')
    as origin,if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro')as destination,IFNULL(TerminalArrival,'DISABLED') as TerminalArrival
    from
    DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition} and (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition}) 
    {Condition1}
    """, con=conn)
    else :
        df_parent_og =pd.read_sql(f"""
    select
    OperationUnit,
    Airline,AircraftRegistration_Arrival,if(On_Block_Time >STA,0,1) as OTP_A00,if(On_Block_Time >DATE_ADD(STA,INTERVAL 15 MINUTE),0,1) as OTP_A15,
    if(Off_Block_Time >STD,0,1) as OTP_D00,if(Off_Block_Time >DATE_ADD(STD,INTERVAL 15 MINUTE),0,1) as OTP_D15,if(FromAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA'),'Metro','Non-Metro')
    as origin,if(ToAirport_IATACode in ('HYD','DEL','BLR','BOM','PNQ', 'CCU', 'MAA') , 'Metro', 'Non-Metro')as destination,if(TerminalArrival in ('1C','1D'),'T1',TerminalArrival) as TerminalArrival
    from
    DailyFlightSchedule_Merged where OperationUnit={OpName} {airline_filter_condition} and (date(On_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition} 
     or date(Off_Block_Time) = date(ADDTIME(now(), '05:30:00')){yesterday_condition})  having TerminalArrival in ('T1','T2','T3') {Condition1}
    """,con=conn)
        
        
    

    
    
    df_parent = df_parent_og[['Aircraft','OperationUnit','Airline','AircraftRegistration_Arrival','OTP_A00','OTP_A15','OTP_D00','OTP_D15','TerminalArrival']] if (OpName==4 or OpName==13) else df_parent_og[['AircraftRegistration_Arrival','OperationUnit','Airline','OTP_A00','OTP_A15','OTP_D00','OTP_D15','TerminalArrival']]
    
    #flightmaster table
    df_FlightMaster= pd.read_sql("select REPLACE(RegNo, '-', '') as RegNo,Hexcode,BodyType,aircraftType from FlightMaster",con=conn)
    #df_FlightMaster['AircraftRegistration_Arrival'] = df_FlightMaster.RegNo.str.split('-').str.join("")
    
    #if opName 22 and 4 case to merge dataframes for getting bodytype
    df_merged = pd.merge(df_parent,df_FlightMaster, left_on=['AircraftRegistration_Arrival'],right_on=['RegNo'], how='left') if OpName==22 else pd.merge(df_parent,df_FlightMaster, left_on=['Aircraft'],right_on=['Hexcode'],how='left')
    print(df_merged.BodyType.unique())
    #airline OTP details
    df_merged_OTP_All = (df_merged.groupby(['Airline','TerminalArrival']).agg('mean').round(1)*100).reset_index().drop(columns={'OperationUnit'},axis=1)
    df_merged_OTP_All = df_merged_OTP_All.reindex(columns=['Airline','OTP_A00','OTP_A15','OTP_D00','OTP_D15','TerminalArrival'])
    df_merged_OTP_All_grouped = df_merged_OTP_All.groupby('TerminalArrival').agg('mean').reset_index().round(1)  
    df_merged_OTP_All_grouped=df_merged_OTP_All_grouped[['OTP_A00', 'OTP_A15', 'OTP_D00', 'OTP_D15','TerminalArrival']]
    df_merged_OTP_All_json = df_merged_OTP_All.to_json(orient='split')
    
    
    #OTP_Avg_Airport = df_merged_OTP_All.mean().round(1).reset_index().rename(columns={'index':'Airport', 0:'Value'}) if OpName==4 else OTP_Avg_Airport.groupby(['TerminalArrival']).agg('mean')
    OTP_Avg_Airport_11 = df_merged_OTP_All_grouped.transpose().reset_index().drop(4).rename(columns={'index':'OTP', 0:'Avg'})
    
    OTP_Avg_Airport_11['TerminalArrival']='DISABLED'
    df_Delhi_avg= df_merged_OTP_All_grouped.melt(id_vars='TerminalArrival').rename(columns={'variable':'OTP', 'value':'Avg'})
    df_Delhi_avg = df_Delhi_avg[['OTP','Avg','TerminalArrival']]
    
    OTP_Avg_Airport_11_json = OTP_Avg_Airport_11.to_json(orient='split') if OpName==4 else df_Delhi_avg.to_json(orient='split')
    
    ########################################## BODYTPE START HERE ####################
    
    df_merged.BodyType.replace({'Turboprop':'Bombardier'},inplace=True)
    df_merged_bodytype = (df_merged.groupby(['BodyType','TerminalArrival','Airline']).agg('mean').round(1)*100).reset_index().drop(columns={'OperationUnit'},axis=1) if AirlineName=="" else (df_merged.groupby(['BodyType','Airline','TerminalArrival']).agg('mean').round(1)*100).reset_index().drop(columns={'OperationUnit'},axis=1)
    df_merged_bodytype_agg = df_merged_bodytype[['BodyType', 'OTP_A00', 'OTP_A15', 'OTP_D00',
           'OTP_D15', 'TerminalArrival']] if AirlineName=="" else df_merged_bodytype[['BodyType','Airline','OTP_A00', 'OTP_A15', 'OTP_D00',
           'OTP_D15', 'TerminalArrival']]
    df_merged_bodytype_json = df_merged_bodytype_agg.groupby(['BodyType','TerminalArrival']).agg({'OTP_A00':'mean', 'OTP_A15':'mean', 'OTP_D00':'mean','OTP_D15':'mean'}).reset_index().to_json(orient='split')        
    
    # df_merged_bodytype_json = df_merged_bodytype_agg.to_json(orient='split')
    
    df_merged_bodytype_Narrow=df_merged_bodytype[df_merged_bodytype.BodyType=='Narrow'].reset_index(drop=True)
    df_merged_bodytype_Narrow_json = df_merged_bodytype_Narrow.to_json(orient='split')
    
    df_merged_bodytype_Bombardier= df_merged_bodytype[df_merged_bodytype.BodyType=='Bombardier'].reset_index(drop=True)
    df_merged_bodytype_Bombardier_json = df_merged_bodytype_Bombardier.to_json(orient='split')
    
    df_merged_bodytype_Wide= df_merged_bodytype[df_merged_bodytype.BodyType=='Wide'].reset_index(drop=True)
    df_merged_bodytype_Wide_json = df_merged_bodytype_Wide.to_json(orient='split')
    
    ############################ BODYTYPE END HERE ##############################
    
    
    ########################### Metro Non Metro #################
    
    df_mnm = df_parent_og[['origin','Airline','OTP_A00','OTP_A15','destination','OTP_D00','OTP_D15','TerminalArrival']]
    df_mnm_or = df_mnm[['origin','Airline','OTP_A00','OTP_A15']]
    df_mnm_ds  = df_mnm[['destination','Airline','OTP_D00','OTP_D15','TerminalArrival']]
    df_mnm_ds = df_mnm_ds.groupby(['destination','Airline','TerminalArrival']).agg('mean').reset_index().round(1)
    df_mnm_or = df_mnm_or.groupby(['origin','Airline']).agg('mean').reset_index().round(1)
    df_M_NM = pd.merge(df_mnm_or, df_mnm_ds, left_on=['origin'],right_on=['destination'],how='inner')
    df_M_NM.rename(columns = {'Airline_x' : 'Airline'},inplace=True)
    df_M_NM.drop('Airline_y', axis=1, inplace=True)
    # df_M_NM.rename(columns = {'TerminalArrival_x' : 'TerminalArrival','TerminalArrival_y' : 'TerminalArrival'},inplace=True)
    df_mnm_ds['OTP_D00'] =  df_mnm_ds.OTP_D00 * 100
    df_mnm_ds['OTP_D15'] =  df_mnm_ds.OTP_D15 * 100
    df_mnm_or['OTP_A00'] =  df_mnm_or.OTP_A00 * 100
    df_mnm_or['OTP_A15'] =  df_mnm_or.OTP_A15 * 100
    df_M_NM['OTP_D00'] =  df_M_NM.OTP_D00 * 100
    df_M_NM['OTP_D15'] =  df_M_NM.OTP_D15 * 100
    df_M_NM['OTP_A00'] =  df_M_NM.OTP_A00 * 100
    df_M_NM['OTP_A15'] =  df_M_NM.OTP_A15 * 100
    df_M_NM = df_M_NM[['origin', 'Airline', 'OTP_A00', 'OTP_A15', 'destination',
            'OTP_D00', 'OTP_D15','TerminalArrival']]
    df_temp=df_M_NM.groupby(['Airline','TerminalArrival','origin','destination']).agg({'OTP_A00':'mean', 'OTP_A15':'mean', 'OTP_D00':'mean','OTP_D15':'mean'}).reset_index().to_json(orient='split')        
    df_M_NM_json = df_M_NM.to_json(orient='split')
    #metro and non metro aggregate
    df_M_NM_Agg = df_M_NM.groupby(['origin','TerminalArrival']).agg('mean').reset_index()
    df_M_NM_Agg = df_M_NM_Agg[['origin', 'OTP_A00', 'OTP_A15', 'OTP_D00','OTP_D15','TerminalArrival']]
    df_M_NM_Agg_json = df_M_NM_Agg.to_json(orient='split')
    return {
        'statusCode': 200,
        #"headers": {"Access-Control-Allow-Origin": "*" },
        'OTP_ALL_D_A': json.loads(df_merged_OTP_All_json),
        'OTP_Avg_Airport' : json.loads(OTP_Avg_Airport_11_json),#exception with the json since dataframe is dynamic for delhi and hyd
        'OTP_M_NM_D_A_Agg': json.loads(df_M_NM_Agg_json),
        'OTP_M_NM_D_A': json.loads(df_M_NM_json),
        'OTP_N_B_D_A_Agg': json.loads(df_merged_bodytype_json),
        'OTP_Narrow_D_A': json.loads(df_merged_bodytype_Narrow_json),
        'OTP_Bomnardier_D_A': json.loads(df_merged_bodytype_Bombardier_json),
        'Opname': OpName,
        'AirlineName':AirlineName,
        'temp':json.loads(df_temp)
    
    }
    
