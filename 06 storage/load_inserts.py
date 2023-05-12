# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv
import io
import pandas as pd

DBname = "postgres"
DBuser = "postgres"
DBpwd = "max"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

# csv = io.StringIO('./acs2015_census_tract_data_part1.csv')

def row2vals(row):
	for key in row:
		if not row[key]:
			row[key] = 0  # ENHANCE: handle the null vals
		row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

	ret = f"""
	   {row['CensusTract']},            -- CensusTract
	   '{row['State']}',                -- State
	   '{row['County']}',               -- County
	   {row['TotalPop']},               -- TotalPop
	   {row['Men']},                    -- Men
	   {row['Women']},                  -- Women
	   {row['Hispanic']},               -- Hispanic
	   {row['White']},                  -- White
	   {row['Black']},                  -- Black
	   {row['Native']},                 -- Native
	   {row['Asian']},                  -- Asian
	   {row['Pacific']},                -- Pacific
	   {row['Citizen']},                -- Citizen
	   {row['Income']},                 -- Income
	   {row['IncomeErr']},              -- IncomeErr
	   {row['IncomePerCap']},           -- IncomePerCap
	   {row['IncomePerCapErr']},        -- IncomePerCapErr
	   {row['Poverty']},                -- Poverty
	   {row['ChildPoverty']},           -- ChildPoverty
	   {row['Professional']},           -- Professional
	   {row['Service']},                -- Service
	   {row['Office']},                 -- Office
	   {row['Construction']},           -- Construction
	   {row['Production']},             -- Production
	   {row['Drive']},                  -- Drive
	   {row['Carpool']},                -- Carpool
	   {row['Transit']},                -- Transit
	   {row['Walk']},                   -- Walk
	   {row['OtherTransp']},            -- OtherTransp
	   {row['WorkAtHome']},             -- WorkAtHome
	   {row['MeanCommute']},            -- MeanCommute
	   {row['Employed']},               -- Employed
	   {row['PrivateWork']},            -- PrivateWork
	   {row['PublicWork']},             -- PublicWork
	   {row['SelfEmployed']},           -- SelfEmployed
	   {row['FamilyWork']},             -- FamilyWork
	   {row['Unemployment']}            -- Unemployment
	"""

	return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable


# read the input data file into a list of row strings
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = csv.DictReader(fil)
		
		rowlist = []
		for row in dr:
			rowlist.append(row)

	return rowlist
	# return dr

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
	cmdlist = []
	for row in rowlist:
		valstr = row2vals(row)
		cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
		cmdlist.append(cmd)
	return cmdlist

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
#	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE TABLE {TableName} (
				CensusTract         NUMERIC,
				State               TEXT,
				County              TEXT,
				TotalPop            INTEGER,
				Men                 INTEGER,
				Women               INTEGER,
				Hispanic            DECIMAL,
				White               DECIMAL,
				Black               DECIMAL,
				Native              DECIMAL,
				Asian               DECIMAL,
				Pacific             DECIMAL,
				Citizen             DECIMAL,
				Income              DECIMAL,
				IncomeErr           DECIMAL,
				IncomePerCap        DECIMAL,
				IncomePerCapErr     DECIMAL,
				Poverty             DECIMAL,
				ChildPoverty        DECIMAL,
				Professional        DECIMAL,
				Service             DECIMAL,
				Office              DECIMAL,
				Construction        DECIMAL,
				Production          DECIMAL,
				Drive               DECIMAL,
				Carpool             DECIMAL,
				Transit             DECIMAL,
				Walk                DECIMAL,
				OtherTransp         DECIMAL,
				WorkAtHome          DECIMAL,
				MeanCommute         DECIMAL,
				Employed            INTEGER,
				PrivateWork         DECIMAL,
				PublicWork          DECIMAL,
				SelfEmployed        DECIMAL,
				FamilyWork          DECIMAL,
				Unemployment        DECIMAL
			);	
			ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
		""")

		print(f"Created {TableName}")

	# with conn.cursor() as cursor:
	# 	cursor.execute(f"""
	# 		CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    #     """)


# create a temp table 
# assumes that conn is a valid, open connection to a Postgres database
def create_staging_table(cursor) -> None:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE UNLOGGED TABLE {TableName} (
				CensusTract         NUMERIC,
				State               TEXT,
				County              TEXT,
				TotalPop            INTEGER,
				Men                 INTEGER,
				Women               INTEGER,
				Hispanic            DECIMAL,
				White               DECIMAL,
				Black               DECIMAL,
				Native              DECIMAL,
				Asian               DECIMAL,
				Pacific             DECIMAL,
				Citizen             DECIMAL,
				Income              DECIMAL,
				IncomeErr           DECIMAL,
				IncomePerCap        DECIMAL,
				IncomePerCapErr     DECIMAL,
				Poverty             DECIMAL,
				ChildPoverty        DECIMAL,
				Professional        DECIMAL,
				Service             DECIMAL,
				Office              DECIMAL,
				Construction        DECIMAL,
				Production          DECIMAL,
				Drive               DECIMAL,
				Carpool             DECIMAL,
				Transit             DECIMAL,
				Walk                DECIMAL,
				OtherTransp         DECIMAL,
				WorkAtHome          DECIMAL,
				MeanCommute         DECIMAL,
				Employed            INTEGER,
				PrivateWork         DECIMAL,
				PublicWork          DECIMAL,
				SelfEmployed        DECIMAL,
				FamilyWork          DECIMAL,
				Unemployment        DECIMAL
			);	
		""")

def load(conn, icmdlist):

	with conn.cursor() as cursor:
		print(f"Loading {len(icmdlist)} rows")
		start = time.perf_counter()
	
		for cmd in icmdlist:
			cursor.execute(cmd)

		cursor.execute(f"""
			CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)

		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

def copy_from_stringio(conn):
	csv = Datafile
	df = pd.read_csv(csv)

	#transform missing NaN values in row 44:
	df.fillna(0, inplace=True)

	buffer = io.StringIO()
	df.to_csv(buffer, index=False, header=False)
	buffer.seek(0)

	cursor = conn.cursor()
	# try:
	start = time.perf_counter()

	cursor.copy_from(buffer, 'censusdata', sep=",")
	
	elapsed = time.perf_counter() - start
	print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
        # conn.commit()
    # except (Exception, psycopg2.DatabaseError) as error:
        
    #     print("Error: %s" % error)
    #     conn.rollback()
    #     cursor.close()
    #     return 1
    # print("copy_from_stringio() done")
    # cursor.close()

def main():
	initialize()
	conn = dbconnect()
	# rlis = readdata(Datafile)
	# cmdlist = getSQLcmnds(rlis)

	if CreateDB:
		createTable(conn)

	# load(conn, cmdlist)
	# csv.seek(0)
	
	# with conn.cursor() as cursor:
	# 	cursor.copy_from(csv, 'censusdata', sep=",")
	copy_from_stringio(conn)
	conn.commit()

if __name__ == "__main__":
	main()
