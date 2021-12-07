#!/bin/bash

# before executing do: chmod 755 init_dbs.sh
# https://www.manniwood.com/postgresql_and_bash_stuff/

# Get environ variables
set -o allexport
source .env
set +o allexport

echo "




    INITIALISING DBS  --------------------



"

# If psql is not available, then exit
if ! command -v psql > /dev/null; then
  echo "This script requires psql to be installed and on your PATH ..."
  exit 1
fi

echo "psql check done"
echo "user: "$POSTGRES_USER
echo "password:" $POSTGRES_PASSWORD
echo "host: "$POSTGRES_HOST
echo "port: "$POSTGRES_PORT
echo "db:" $POSTGRES_DB
echo "postgresql://"$POSTGRES_USER":"$POSTGRES_PASSWORD"@"$POSTGRES_HOST":"$POSTGRES_PORT"/"$POSTGRES_DB

echo "pguser: "$PGUSER 
echo "pgpassword: "$PGPASSWORD
echo "pgport: "$PGPORT
echo "pghost: "$PGHOST
echo "pgdatabase: "$PGDATABASE


# POSTGRES_HOST="0.0.0.0"
# POSTGRES_PORT=5433



# ----------------
# -- DATA LAKE  --
# ----------------

echo " 

DATA LAKE
"
DATA_LAKE_DB="data_lake2"

echo "TEST1"
psql -d warehouse -U  warehouse -p 5432 -h "0.0.0.0" -c "SELECT version();"
echo "TEST2"
psql postgresql://warehouse:warehouse@0.0.0.0:5432/warehouse -c "SELECT version();"

echo "TEST3"
psql -d warehouse -U  warehouse -p 5432 -h "postgres-dw" -c "SELECT version();"
echo "TEST4"
psql postgresql://warehouse:warehouse@postgres-dw:5432/warehouse -c "SELECT version();"

echo "TEST5"
psql -d warehouse -U  warehouse -p 5432 -h 172.18.0.9 -c "SELECT version();"
echo "TEST6"
psql postgresql://warehouse:warehouse@172.18.0.9:5432/warehouse -c "SELECT version();"



172.18.0.9
# Create data_lake database  # https://zaiste.net/databases/postgresql/howtos/create-database-if-not-exists/
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB -tc "SELECT 1 FROM pg_database WHERE datname = '"$DATA_LAKE_DB"'" | grep -q 1 | psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB -c "CREATE DATABASE "$DATA_LAKE_DB
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB -tc "SELECT 1 FROM pg_database WHERE datname = '"$DATA_LAKE_DB"'" | grep -q 1 | psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB -c "CREATE DATABASE "$DATA_LAKE_DB

# Create data_lake schema
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$DATA_LAKE_DB --command="$(<"db_init/data_lake/create_data_lake_schema.sql")"

# Create tables
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$DATA_LAKE_DB --command="$(<"db_init/data_lake/create_twitter_data_table.sql")"
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$DATA_LAKE_DB --command="$(<"db_init/data_lake/create_weather_data_table.sql")"



# ---------------------
# -- DATA WAREHOUSE  --
# ---------------------

echo " 

WAREHOUSE
"
# Create warehouse database 
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB -tc "SELECT 1 FROM pg_database WHERE datname = 'warehouse2'" | grep -q 1 | psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB -c "CREATE DATABASE warehouse2"

POSTGRES_DB="warehouse2"
# Create data_lake schema
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB --command="$(<"db_init/warehouse/create_warehouse_schema.sql")"

# Create tables
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB --command="$(<"db_init/warehouse/create_twitter_table.sql")"
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB --command="$(<"db_init/warehouse/create_weather_table.sql")"
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB --command="$(<"db_init/warehouse/create_sentiment_analysis_table.sql")"



# -------------------------------------
# -- CREATE FOREIGN DATABASE WRAPPER --
# -------------------------------------
# DATA_LAKE_FDW_SCHEMA="data_lake_fdw2"
# # Create connection from the warehouse database to the data_lake database
# psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$DATA_LAKE_DB --command="CREATE SCHEMA IF NOT EXISTS "$DATA_LAKE_FDW_SCHEMA";"

# CREATE EXTENSION IF NOT EXISTS postgres_fdw2;

# CREATE SERVER IF NOT EXISTS data_lake2 
# FOREIGN DATA WRAPPER postgres_fdw2 
# OPTIONS (dbname 'data_lake', port '5432', host 'localhost');

# CREATE USER MAPPING IF NOT EXISTS FOR warehouse
# SERVER data_lake2 
# OPTIONS (user 'warehouse', password 'warehouse');  -- TODO: refractor, hide passwords


# IMPORT FOREIGN SCHEMA data_lake2 FROM SERVER data_lake2 INTO data_lake_fdw2;

# -- # TODO: programatically add connections data_warehouse and data_lake into Airflow


