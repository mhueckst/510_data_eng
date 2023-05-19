import time
import psycopg2
import argparse
import io
import pandas as pd
import validate_transform as vt


DBname = "postgres"
DBuser = "postgres"
DBpwd = "dtm"
# TableName = 'BreadCrumb'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
# CreateDB = False  # indicates whether the DB table should be (re)-created

def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
#   parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

def dbconnect():
    connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
    return connection

# create DF from csv, transform into sql ready dfs, load into postgres: 
def copy_from_stringio(conn):
    csv = Datafile
    # df = pd.read_csv(csv)
    df = vt.transform_csv(csv)
    breadcrumb = vt.transform_BreadCrumb(df)
    trip = vt.transform_Trip(df)

    # Load breadcrumb table using copy_from and stringIO buffer: 
    buffer = io.StringIO()
    breadcrumb.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    start = time.perf_counter()

    cursor.copy_from(buffer, 'breadcrumb', sep=",")

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

    # Load trip table using copy_from and stringIO buffer: 
    buffer = io.StringIO()
    trip.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    start = time.perf_counter()

    cursor.copy_from(buffer, 'trip', sep=",")

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
        

def main():
	initialize()
	conn = dbconnect()
	copy_from_stringio(conn)
	conn.commit()

if __name__ == "__main__":
	main()
