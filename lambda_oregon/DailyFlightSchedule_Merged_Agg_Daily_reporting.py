import pandas as pd
import pymysql
#!pip install mysql-connector
import mysql.connector
from sqlalchemy import create_engine

def lambda_handler(event, context):
    
    '''
    Create a mapping of df dtypes to mysql data types (not perfect, but close enough)
    '''
    def dtype_mapping():
        return {'object' : 'TEXT',
            'int64' : 'INT',
            'float64' : 'FLOAT',
            'datetime64[ns]' : 'DATETIME',
            'bool' : 'TINYINT',
            'category' : 'TEXT',
            'timedelta[ns]' : 'TEXT'}
            
            
    username='dheeraj_tbb'
    password='DSHW6wNx7e46uSNK'
    hostname = 'aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com'
    jdbcPort = 3306
    dbname = 'AviLeap'
    db_write= 'Reporting'
    conn = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname,connect_timeout=700)
    conn_write = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=db_write,connect_timeout=700)
    
    df = pd.read_sql("""
    select airline,flightnumber_arrival,FlightNumber_Departure,ATA, ATD,
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
     if(FromAirport_IATACode IN ('HYD','DEL','BLR','BOM') , 'Metro', 'Non-Metro') AS origin, 
     if(ToAirport_IATACode IN ('HYD','DEL','BLR','BOM') , 'Metro', 'Non-Metro') AS destination, 
     if(length(FlightNumber_Arrival)=6,'Narrow','TurboProp') AS bodytype,
     cast(month(ATA) as SIGNED) AS ata_month,
     cast(month(ATD) as SIGNED) AS atd_month,
     cast(week(ATA) as SIGNED) AS ata_week, 
     cast(week(ATD) as SIGNED) AS atd_week, 
     cast(day(ATA) as SIGNED) AS ata_day, 
     cast(day(ATD) as SIGNED) AS atd_day, 
     cast(hour(ATA) as SIGNED) AS ata_hour, 
     cast(hour(ATD) as SIGNED) AS atd_hour
    from DailyFlightSchedule_Merged 
    where OperationUnit=4 and 
    date(On_Block_Time) =DATE_ADD(date(ADDTIME(now(), '05:30:00')), INTERVAL -1 DAY)
    """, con=conn)
    
    engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(username, password, hostname, jdbcPort, db_write), echo=False)
    #mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8
    
    tbl_name = 'DailyFlightScheduled_Merged_Agg_1'
    def df_to_mysql(df, engine, tbl_name):
        df.to_sql(tbl_name, engine, if_exists='append',index=False, chunksize=1000)
        
    df_to_mysql(df,engine,tbl_name)