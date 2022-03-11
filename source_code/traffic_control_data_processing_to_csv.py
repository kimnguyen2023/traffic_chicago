import pandas as pd
import os
# path=os.path.abspath("Traffic_Crashes_Crashes.csv")
# df=pd.read_csv(path)
# print(type(df))
dirname = os.path.dirname(__file__)
path = os.path.join(dirname, 'Traffic_Crashes_Crashes.csv')
df=pd.read_csv(path)
new_df=df[['CRASH_RECORD_ID','CRASH_DATE','CRASH_HOUR','CRASH_DAY_OF_WEEK','CRASH_MONTH','WEATHER_CONDITION','LIGHTING_CONDITION','ROADWAY_SURFACE_COND','TRAFFIC_CONTROL_DEVICE','DEVICE_CONDITION','ROAD_DEFECT','ALIGNMENT','POSTED_SPEED_LIMIT','CRASH_TYPE','DAMAGE','MOST_SEVERE_INJURY','INJURIES_TOTAL','INJURIES_FATAL','INJURIES_INCAPACITATING','INJURIES_NON_INCAPACITATING','INJURIES_REPORTED_NOT_EVIDENT','INJURIES_NO_INDICATION','INJURIES_UNKNOWN','PRIM_CONTRIBUTORY_CAUSE','SEC_CONTRIBUTORY_CAUSE','STREET_NAME','LATITUDE','LONGITUDE']]
new_path=os.path.join(dirname, 'Processed_Traffic_Crashes_Crashes.csv')
new_df.to_csv(new_path)