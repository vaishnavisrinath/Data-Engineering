import psycopg2
import argparse
import csv
import time

DBname = "postgres"
DBuser = "postgres"
DBpwd = "chun04"
TableName = 'censusdata'
Datafile = "acs2015_census_tract_data_part2.csv"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
    global CreateDB
    CreateDB = args.createtable

# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    return connection

# create the target table if it doesn't exist
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                CensusTract         NUMERIC PRIMARY KEY,
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
        print(f"Created {TableName}")

# load data into the table using the COPY command
def load(conn):
    with open(Datafile, 'r') as f:
        next(f)  # Skip the header row
        with conn.cursor() as cursor:
            print(f"Loading data into {TableName}")
            start = time.perf_counter()
            cursor.copy_from(f, TableName, sep=',', null='')
            elapsed = time.perf_counter() - start
            print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

    conn.commit()
    print("Data loaded successfully.")


def main():
    initialize()
    conn = dbconnect()

    if CreateDB:
        createTable(conn)
    
    load(conn)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
