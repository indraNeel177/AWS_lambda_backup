import pandas as pd
import pymysql
import json
import numpy as np
import configparser
import boto3
import io
from datetime import timedelta, date, datetime


def lambda_handler(event, context):
    hostname = 'prdouctionavileapvpc.cscnpwfy9ubd.ap-south-1.rds.amazonaws.com'
    jdbcPort = 3306
    username = 'avileap_curd'
    password = 'avileap^7t*ALP'
    dbname = 'AviLeap'

    try:
        connection = pymysql.connect(host=hostname, port=int(jdbcPort), user=username, passwd=password, db=dbname,
                                     connect_timeout=5)
    except Exception as e:
        print("Sql connection error")

    OpName = int(event['OperationUnit'])
    Airline = event['Airline']
    Min_date = event['Mindate']
    Max_date = event["Maxdate"]
    Equipments = event['equipments']
    airline_filter_condition = ""
    # Device_id=""

    Equipment_list = ''

    Device_id = "and e2.DevId like 'ASAT%'"

    data_min = datetime.strptime(Min_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')

    data_max = datetime.strptime(Max_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')

    data_current = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    # data_current = (datetime.now() + timedelta(hours=5,minutes=30) ).strftime('%Y-%m-%d %H:%M:%S')

    current_date = str(date.today().strftime('%Y-%m-%d'))

    max_date_value = datetime.strptime(Max_date, '%Y-%m-%d') + timedelta(1)

    Card_data = pd.DataFrame()
    Equipment_data = pd.DataFrame()
    Device_data = pd.DataFrame()
    Non_com_speed_card = pd.DataFrame()
    Non_com_Equipment = pd.DataFrame()
    Non_com_device = pd.DataFrame()

    # airline_filter_condition= "" if OpName!= 13 else "and Airline in ('Air India','Etihad','Singapore Airlines','Alliance Air','Thai Airways','Air Arabia','Cathay Pacific','FlyDubai','Go Air','Jazeera Airways','Silk Air','Vistara')"

    data_Movement = pd.read_sql(
        f""" select e2.DevId,em.Equipment,count(e2.DevId) as counts,if(em.Ownership_Category=1,'Own','Leased') as Ownership_Category,if(em.Equipment_Category=1,"Motorized","Non-Motorized") as Equipment_Category from  NonCompliance e2 inner join EquipmentMaster em on em.DevId=e2.DevId  where em.OperationUnit={OpName} and (date(e2.LogDate) between '{Min_date}' and '{Max_date}') and e2.`type` = "Overspeed" {airline_filter_condition} {Device_id} {Equipment_list} group by e2.DevId """,
        con=connection)

    device_level = pd.read_sql(
        f""" select e2.DevId,e2.OperationName,e2.FLogDate,e2.Duration as timedata,e2.ActivityCompleted,e2.TLogDate,em.Equipment,e2.`Type`  as On_off ,if(em.Ownership_Category=1,'Own','Leased') as Ownership_Category,if(em.Equipment_Category=1,"Motorized","Non-Motorized") as Equipment_Category from EquipActivityLogs e2 left join EquipmentMaster em on e2.DevId=em.DevId where ((date(FLogDate) between '{Min_date}' and '{Max_date}') or (date(TLogDate) between '{Min_date}' and '{Max_date}' )) and OperationName in('IGO','IGF') and em.Equipment_Category=1  and em.OperationUnit={OpName} {Device_id} {airline_filter_condition}{Equipment_list} """,
        con=connection)

    device_set = list(device_level["DevId"])

    if len(device_set) == 1:
        device_set.append(device_set[0])

    device_set = tuple(device_set)

    adding_join = pd.read_sql(
        f""" select e2.DevId,e2.OperationName,max(e2.FLogDate) as FLogDate,max(e2.TLogDate) as TLogDate,e2.ActivityCompleted,em.Equipment,e2.`Type`  as On_off ,if(em.Ownership_Category=1,'Own','Leased') as Ownership_Category,if(em.Equipment_Category=1,"Motorized","Non-Motorized") as Equipment_Category from EquipActivityLogs e2 left join EquipmentMaster em on e2.DevId=em.DevId where e2.ActivityCompleted=0 and e2.OperationName in('IGO','IGF') and (date(e2.FLogDate) BETWEEN (date('{Min_date}') - INTERVAL 1 DAY) and (date('{Max_date}')- INTERVAL 1 DAY)) {Equipment_list} and e2.DevId not like "ASAT_BIAL_TEST%" and e2.DevId not in {device_set} group by e2.DevId """,
        con=connection)

    if not device_level.empty or not adding_join.empty:

        adding_join["FLogDate"] = data_min
        adding_join["TLogDate"] = data_max

        device_level = pd.concat([device_level, adding_join])
        device_level['Equipment'] = np.where(device_level['Equipment'] == '', device_level['OperationName'],
                                             device_level['Equipment'])

        device_level["activitystart"] = np.where(device_level["FLogDate"] < Min_date, data_min,
                                                 device_level["FLogDate"].astype(str))

        if current_date == Max_date:
            device_level['Maximum_Date'] = device_level.groupby(['DevId', 'Equipment'])['TLogDate'].transform(max)

            device_level['Activityend'] = np.where(
                (device_level['ActivityCompleted'] == 0) & (device_level['TLogDate'] == device_level['Maximum_Date']),
                data_current, device_level["TLogDate"].astype(str))
            device_level['Activityend'] = pd.to_datetime(device_level['Activityend'])

        else:
            device_level['Maximum_Date'] = device_level.groupby(['DevId', 'Equipment'])['TLogDate'].transform(max)

            device_level["Activityend"] = np.where(device_level["TLogDate"] > max_date_value, data_max,
                                                   device_level["TLogDate"].astype(str))

            device_level['Activityend'] = np.where(
                (device_level['ActivityCompleted'] == 0) & (device_level['TLogDate'] == device_level['Maximum_Date']),
                data_max, device_level["Activityend"].astype(str))

            device_level['Activityend'] = pd.to_datetime(device_level['Activityend'])

        device_level["Duration"] = (device_level.Activityend - device_level.activitystart).astype('timedelta64[s]')

        device_level = \
        device_level.groupby(['DevId', 'Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category'])[
            'Duration'].agg({"sum"}).round(2).reset_index()

        device_level.rename(columns={"sum": "TimeServed"}, inplace=True)

        device_level["TimeServed"] = device_level["TimeServed"] / 3600

        ########### Chart data #############

        data_on = device_level[device_level['On_off'] == 'ig_on']
        data_off = device_level[device_level['On_off'] == 'ig_off']
        data = pd.merge(data_on, data_off, how='left', on='Equipment')
        data.rename(columns={"On_off_x": "Ignition_on", "On_off_y": "Ignition_off", "DevId_x": "DevId_on",
                             "DevId_y": "DevId_off", "TimeServed_x": "TimeServed_on", "TimeServed_y": "TimeServed_off"},
                    inplace=True)
        data[['DevId_on', 'TimeServed_on', 'Ignition_on', 'Equipment', 'DevId_off', 'TimeServed_off', 'Ignition_off']]

        total_data_on = data.groupby(["Equipment", "Ignition_on"])['TimeServed_on'].agg({"mean"}).round(2).reset_index()
        total_data_off = data.groupby(["Equipment", "Ignition_off"])['TimeServed_off'].agg({"mean"}).round(
            2).reset_index()
        level = pd.merge(total_data_on, total_data_off, how='outer', on='Equipment')
        Card_data = level.sort_values(by='mean_x', ascending=False).reset_index(drop=True).head(5)

        Card_data.drop(['Ignition_on', 'Ignition_off'], axis=1, inplace=True)
        Card_data.rename(columns={"mean_x": "Ig_on", "mean_y": "Ig_off"}, inplace=True)
        Card_data = Card_data[Card_data['Ig_on'] > 0.01]

        ############ Level charts ##################

        Equipment_data_sum = device_level.groupby(['Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category'])[
            'TimeServed'].agg({"sum"}).round(2).reset_index()

        Equipment_data_value = device_level[["DevId"]].groupby(['DevId'])['DevId'].agg({'count'}).reset_index()
        Equipment_data_value.rename(columns={"count": "dev_count"}, inplace=True)
        Equipment_data_value["final_count"] = np.where(Equipment_data_value['dev_count'] == 1, 2,
                                                       Equipment_data_value['dev_count'])

        Operation_name_out = pd.merge(device_level, Equipment_data_value, on=['DevId'])

        Equipment_data_count = \
        Operation_name_out.groupby(['Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category', 'dev_count'])[
            'DevId'].agg({"count"}).round(2).reset_index()

        Equipment_data = pd.merge(Equipment_data_sum, Equipment_data_count,
                                  on=['Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category'])

        Equipment_data["count"] = np.where(Equipment_data['dev_count'] == 1, Equipment_data['count'] * 2,
                                           Equipment_data['count'])

        Equipment_data.rename(columns={"sum": "Total"}, inplace=True)

        Equipment_data = \
        Equipment_data.groupby(['Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category', 'Total'])[
            'count'].agg({"sum"}).round(2).reset_index()
        # Equipment_data["sum"]= (Equipment_data['Total']/Equipment_data['sum']).round(2)

        Equipment_data = Equipment_data[
            ['Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category', 'Total', 'sum']]
        Equipment_data = Equipment_data[Equipment_data['Total'] > 0.01]

        Device_data = \
        device_level.groupby(['DevId', 'Equipment', 'On_off', 'Ownership_Category', 'Equipment_Category'])[
            'TimeServed'].agg({"mean"}).round(2).reset_index()
        Device_data.rename(columns={"mean": "sum"}, inplace=True)
        Device_data = Device_data[Device_data['sum'] > 0.01]

    Ig_Card_data = Card_data.to_json(orient='split')
    Ig_Equipment_data = Equipment_data.to_json(orient='split')
    Ig_Device_data = Device_data.to_json(orient='split')

    ##################Non compliance################

    if not device_level.empty:
        Non_com_speed = data_Movement.groupby(['Equipment'])['counts'].agg({"sum"}).round(2).reset_index()
        Non_com_speed_card = Non_com_speed.sort_values(by='sum', ascending=False).reset_index(drop=True).head(5)
        Non_com_speed_card = Non_com_speed_card[Non_com_speed_card['sum'] > 0.01]

        Non_com_Equipment = data_Movement.groupby(['Equipment', 'Ownership_Category', 'Equipment_Category'])[
            'counts'].agg({"sum"}).round(2).reset_index()
        Non_com_Equipment = Non_com_Equipment[Non_com_Equipment['sum'] > 0]
        Non_com_device = data_Movement.groupby(['DevId', 'Equipment', 'Ownership_Category', 'Equipment_Category'])[
            'counts'].agg({"sum"}).round(2).reset_index()
        Non_com_device = Non_com_device[Non_com_device['sum'] > 0]

    Non_com_speed_card = Non_com_speed_card.to_json(orient='split')
    Non_com_Equipment = Non_com_Equipment.to_json(orient='split')
    Non_com_device = Non_com_device.to_json(orient='split')

    return {
        "statusCode": 200,
        'Ig_Card_data': json.loads(Ig_Card_data),
        'Ig_Equipment_data': json.loads(Ig_Equipment_data),
        'Ig_Device_data': json.loads(Ig_Device_data),
        'Non_com_speed_card': json.loads(Non_com_speed_card),
        'Non_com_Equipment': json.loads(Non_com_Equipment),
        'Non_com_device': json.loads(Non_com_device)

    }