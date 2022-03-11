import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import numpy as np

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)

def insert_table_data(connection, cursor, table_name, col_names, data):
    for index, rows in data.iterrows():
        character_vals = '%s,'*len(tuple(rows))
        character_vals = character_vals[:-1]
        sql_str = "insert into traffic_crash_v2.%s(%s) " %(table_name, ','.join(col_names))  
        sql = (sql_str + "values (%s)" %(character_vals))

        #value of a row represented as a tuple
        val = tuple(rows)

        try:
            #inserting the values into the table
            cursor.execute(sql,val)
        
            #commit the transaction
            connection.commit()
        except Exception as e:
            connection.rollback()

def insert_one_to_one_table_data(connection, cursor, table_name, col_names, data):
    for index, rows in data.iterrows():
        character_vals = '%s,'*len(tuple(rows))
        character_vals = character_vals[:-1]
        sql_str = "insert into traffic_crash_v2.%s(%s) " %(table_name, ','.join(col_names))  
        sql = (sql_str + "values (%s)" %(character_vals))

        #value of a row represented as a tuple
        val = tuple(rows)

        try:
            #inserting the values into the table
            cursor.execute(sql,val)
        
            #commit the transaction
            connection.commit()
        except Exception as e:
            connection.rollback()

def insert_main_table_data(connection, cursor, table_name, col_names, data, info_lst, time_lst):
    for index, rows in data.iterrows():
        character_vals = '%s,'*len(tuple(rows))
        character_vals = character_vals[:-1]
        sql_str = "insert into traffic_crash_v2.%s(%s) " %(table_name, ','.join(col_names))  
        sql = (sql_str + "values (%s)" %(character_vals))

        #value of a row represented as a tuple
        val = tuple(rows)

        try:
            #inserting the values into the table
            cursor.execute(sql,val)
        
            #commit the transaction
            connection.commit()

            print('Insert to crash crash ' + str(index))

            last_row_id = cursor.lastrowid
            if last_row_id:
                info_lst[index]['crash_crash_id'] = last_row_id
                time_lst[index]['crash_crash_id'] = last_row_id
            

        except Exception as e:
            connection.rollback()
    print('Insert to crash info table')
    dlist_info_frame = pd.DataFrame(info_lst)  
    insert_one_to_one_table_data(connection, cursor, 'crash_info', [
    'crash_crash_id',
    'crash_street_name',
    'crash_latitude',
    'crash_longitude',
    'prim_contributory_cause',
    'sec_contributory_cause'
    ], dlist_info_frame)

    print('Insert to crash time table ')
    dlist_time_frame = pd.DataFrame(time_lst)  
    insert_one_to_one_table_data(connection, cursor, 'crash_time', [
    'crash_crash_id',
    'crash_date',
    'crash_hour',
    'crash_day',
    'crash_month',
    ], dlist_time_frame)


def get_table_foreign_key(id, table, name_criteria, value_criteria):
    sql_str = "select %s FROM traffic_crash_v2.%s where %s = '%s';" %(id, table, name_criteria, value_criteria)
    cursor.execute(sql_str)
    row = cursor.fetchone()
    if row:
        return int(row[0])
    return None


path=os.path.abspath("Traffic_Crashes_Crashes.csv")
df=pd.read_csv(path)

traffic_control_data = df[['TRAFFIC_CONTROL_DEVICE']].drop_duplicates()
speed_limit_data=df[['POSTED_SPEED_LIMIT']].drop_duplicates()
factor_weather_data = df[['WEATHER_CONDITION']].drop_duplicates()
factor_lighting_data=df[['LIGHTING_CONDITION']].drop_duplicates()
factor_surface_condition_data=df[['ROADWAY_SURFACE_COND']].drop_duplicates()
factor_alignment_data=df[['ALIGNMENT']].drop_duplicates()
factor_road_defect_data=df[['ROAD_DEFECT']].drop_duplicates()
# info_data=df[['1','2','3']].drop_duplicates()
# time_data=df[['']].drop_duplicates()
traffic_control_condition_data = df[['DEVICE_CONDITION']].drop_duplicates()
type_data=df[['CRASH_TYPE']].drop_duplicates()
most_severe_injury_data=df[['MOST_SEVERE_INJURY']].dropna().drop_duplicates()
# crash_crash=df[['DAMAGE','INJURIES_TOTAL','INJURIES_FATAL','INJURIES_INCAPACITATING','INJURIES_NON_INCAPACITATING','INJURIES_REPORTED_NOT_EVIDENT','INJURIES_NO_INDICATION']].drop_duplicates()

crash_data = df.dropna()
try:
    connection = mysql.connector.connect(host='localhost',
                                         database='traffic_crash_v2',
                                         user='root',
                                         password='123456')
    connection.set_converter_class(NumpyMySQLConverter)

    if connection.is_connected():
        cursor = connection.cursor(buffered=True)
        print('Connected to database')
        # delete table data first
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_crash;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_traffic_control;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_speed_limit;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_factor_alignment;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_factor_road_defect;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_factor_surface_condition;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_factor_weather;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_most_severe_injury;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_traffic_control_condition;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_type;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_info;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_time;" )
        cursor.execute("TRUNCATE traffic_crash_v2.crash_factor_lighting;" )
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;" )
        connection.commit()

        print('Clear table data successfully')

        # crash traffic control
        insert_table_data(connection, cursor, 'crash_traffic_control', ['traffic_control_device'], traffic_control_data)
        # crash_speed_limit
        insert_table_data(connection, cursor, 'crash_speed_limit', ['crash_speed_limit_value'], speed_limit_data)
        # crash factor weather
        insert_table_data(connection, cursor, 'crash_factor_weather', ['weather_condition'], factor_weather_data)
        # crash_factor_lighting
        insert_table_data(connection, cursor, 'crash_factor_lighting', ['lighting_condition'], factor_lighting_data)
        # crash_factor_surface_condition
        insert_table_data(connection, cursor, 'crash_factor_surface_condition', ['roadway_surface_condition'], factor_surface_condition_data)
        # crash_factor_alignment
        insert_table_data(connection, cursor, 'crash_factor_alignment', ['alignment'], factor_alignment_data)
        # crash_factor_road_defect
        insert_table_data(connection, cursor, 'crash_factor_road_defect', ['road_defect'], factor_road_defect_data)
        # crash_info
        # insert_table_data(connection, cursor, 'crash_info', ['crash_street_name','crash_latitude','crash_longitude','prim_contributory_cause','sec_contributory_cause'], info_data)
        # crash_time
        # insert_table_data(connection, cursor, 'crash_time', ['crash_date','crash_hour','crash_day','crash_month'], time_data)
        # crash traffic control condition
        insert_table_data(connection, cursor, 'crash_traffic_control_condition', ['device_condition'], traffic_control_condition_data)
        # crash_type
        insert_table_data(connection, cursor, 'crash_type', ['crash_type'], type_data)
        # crash_most_severe_injury
        insert_table_data(connection, cursor, 'crash_most_severe_injury', ['most_severe_injury'], most_severe_injury_data)
        # crash_crash

        lst = []
        time_lst = []
        info_lst = []
        print('Create master data successfully')
        print('Start read file csv')
        #for loop through rows in file csv
        for index, row in crash_data.iterrows():
            print('Reading index: ' + str(index))
            # get foreign key from master table for value of the column in file csv
            crash_traffic_control_id = get_table_foreign_key('crash_traffic_control_id', 'crash_traffic_control', 'traffic_control_device', row['TRAFFIC_CONTROL_DEVICE'])
            crash_traffic_control_condition_id = get_table_foreign_key('crash_traffic_control_condition_id', 'crash_traffic_control_condition', 'device_condition', row['DEVICE_CONDITION'])
            crash_speed_limit_id = get_table_foreign_key('crash_speed_limit_id', 'crash_speed_limit', 'crash_speed_limit_value', row['POSTED_SPEED_LIMIT'])
            crash_type_id = get_table_foreign_key('crash_type_id', 'crash_type', 'crash_type', row['CRASH_TYPE'])
            crash_most_severe_injury_id = get_table_foreign_key('crash_most_severe_injury_id', 'crash_most_severe_injury', 'most_severe_injury', row['MOST_SEVERE_INJURY'])
            crash_factor_weather_id = get_table_foreign_key('crash_factor_weather_id', 'crash_factor_weather', 'weather_condition', row['WEATHER_CONDITION'])
            crash_factor_lighting_id = get_table_foreign_key('crash_factor_lighting_id', 'crash_factor_lighting', 'lighting_condition', row['LIGHTING_CONDITION'])
            crash_factor_surface_condition_id = get_table_foreign_key('crash_factor_surface_condition_id', 'crash_factor_surface_condition', 'roadway_surface_condition', row['ROADWAY_SURFACE_COND'])
            crash_factor_alignment_id = get_table_foreign_key('crash_factor_alignment_id', 'crash_factor_alignment', 'alignment', row['ALIGNMENT'])
            crash_factor_road_defect_id = get_table_foreign_key('crash_factor_road_defect_id', 'crash_factor_road_defect', 'road_defect', row['ROAD_DEFECT'])

            lst.append({'crash_traffic_control_id': crash_traffic_control_id,
                'crash_traffic_control_condition_id': crash_traffic_control_condition_id,
                'crash_speed_limit_id': crash_speed_limit_id,
                'crash_type_id': crash_type_id,
                'crash_most_severe_injury_id': crash_most_severe_injury_id,
                'crash_factor_weather_id': crash_factor_weather_id,
                'crash_factor_lighting_id': crash_factor_lighting_id,
                'crash_factor_surface_condition_id': crash_factor_surface_condition_id,
                'crash_factor_alignment_id': crash_factor_alignment_id,
                'crash_factor_road_defect_id': crash_factor_road_defect_id,
                'crash_damage': row['DAMAGE'],
                'crash_injuries_total': row['INJURIES_TOTAL'],
                'crash_injuries_fatal': row['INJURIES_FATAL'],
                'crash_injuries_incapacitating': row['INJURIES_INCAPACITATING'],
                'crash_injuries_non_incapacitating': row['INJURIES_NON_INCAPACITATING'],
                'crash_injuries_not_evident': row['INJURIES_REPORTED_NOT_EVIDENT'],
                'crash_injuries_no_indication': row['INJURIES_NO_INDICATION'],
                'crash_injuries_unknown': row['INJURIES_UNKNOWN'],
            })
            info_lst.append({
                'crash_crash_id': -1,
                'crash_street_name': row['STREET_NAME'],
                'crash_latitude': str(row['LATITUDE']),
                'crash_longitude': str(row['LONGITUDE']),
                'prim_contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
                'sec_contributory_cause': row['SEC_CONTRIBUTORY_CAUSE']
            })
            date = datetime.strptime(row['CRASH_DATE'], '%m/%d/%Y %H:%M:%S %p')
            time_lst.append({
                'crash_crash_id': -1,
                'crash_date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'crash_hour': date.hour,
                'crash_day': date.day,
                'crash_month': date.month,
            })

        dframe = pd.DataFrame(lst)  
        # dframe: foreign link tới bảng crash_traffic_control
        print('Start insert to main table')
        insert_main_table_data(connection, cursor, 'crash_crash', ['crash_traffic_control_id',
                'crash_traffic_control_condition_id',
                'crash_speed_limit_id',
                'crash_type_id',
                'crash_most_severe_injury_id',
                'crash_factor_weather_id',
                'crash_factor_lighting_id',
                'crash_factor_surface_condition_id',
                'crash_factor_alignment_id',
                'crash_factor_road_defect_id',
                'crash_damage',
                'crash_injuries_total',
                'crash_injuries_fatal',
                'crash_injuries_incapacitating',
                'crash_injuries_non_incapacitating',
                'crash_injuries_not_evident',
                'crash_injuries_no_indication',
                'crash_injuries_unknown' ], dframe, info_lst, time_lst)



except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


