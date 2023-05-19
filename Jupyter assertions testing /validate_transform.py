import pandas as pd
from datetime import datetime, timedelta

# Get csv from command line or from consumed_data/ **currently hardcoded to 4-30 data: 

# *************
# Read csv and transform it: 
def transform_csv(csv):
    df = pd.read_csv(csv)
    df = df.drop(columns=['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP'])

    def create_timestamp(i):
        date = datetime.strptime(i['OPD_DATE'],'%d%b%Y:%H:%M:%S')
        time = timedelta(seconds=i['ACT_TIME'])
        return date + time 
        
    df['TIMESTAMP'] = df.apply(create_timestamp,axis=1)
    df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

    def speed(meters, time):
        speed = meters/time
        return speed

    df['dMETERS'] = df['METERS'].diff()
    df['dTIMESTAMP'] = df['TIMESTAMP'].diff()
    df['SPEED'] = df.apply(lambda row: speed(row['dMETERS'],row['dTIMESTAMP'].total_seconds()), axis=1)
    df = df.drop(columns=['dMETERS','dTIMESTAMP'])
    return df 


# ****************
# Transform data into postgres table format:  
def transform_BreadCrumb(df): 
    BreadCrumb = df[['TIMESTAMP','GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].copy()
    BreadCrumb.rename({'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE':'longitude', 'SPEED':'speed', 'EVENT_NO_TRIP':'trip_id'}, axis='columns', inplace=True)
    return BreadCrumb

def transform_Trip(df):
    Trip = df[['EVENT_NO_TRIP', 'VEHICLE_ID']].copy()
    Trip.rename({'EVENT_NO_TRIP':'trip_id', 'VEHICLE_ID':'vehicle_id'},axis='columns',inplace=True)
    Trip.insert(loc=1, column='route_id', value=-1)
    Trip.insert(loc=3, column='service_key', value=None)
    Trip.insert(loc=4, column='direction', value=None)
    return Trip


# Sample flow: (also can be run as script w/ no functions)
# csv = '2023-04-30.csv'

# df = transform_csv(csv)
# transform_BreadCrumb(df)
# transform_Trip(df)