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


POSTGRES_HOST=localhost
POSTGRES_PORT=5433



# ----------------
# -- DATA LAKE  --
# ----------------

echo " 

DATA LAKE
"
DATA_LAKE_DB="data_lake2"

# echo "TEST1"
# psql -d warehouse -U  warehouse -p 5432 -h "0.0.0.0" -c "SELECT version();"
# echo "TEST2"
# psql postgresql://warehouse:warehouse@0.0.0.0:5432/warehouse -c "SELECT version();"

# echo "TEST3"
# psql -d warehouse -U  warehouse -p 5432 -h "postgres-dw" -c "SELECT version();"
# echo "TEST4"
# psql postgresql://warehouse:warehouse@postgres-dw:5432/warehouse -c "SELECT version();"

# echo "TEST5"
# psql -d warehouse -U  warehouse -p 5432 -h 172.18.0.9 -c "SELECT version();"
# echo "TEST6"
# psql postgresql://warehouse:warehouse@172.18.0.9:5432/warehouse -c "SELECT version();"

# echo "TEST7"
# psql -d warehouse -U  warehouse -p 5432 -h "127.0.0.1" -c "SELECT version();"
# echo "TEST8"
# psql postgresql://warehouse:warehouse@127.0.0.1:5432/warehouse -c "SELECT version();"


# Create data_lake database  # https://zaiste.net/databases/postgresql/howtos/create-database-if-not-exists/
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

echo " 

FOREIGN DATABASE WRAPPER
"
DATA_LAKE_FDW_SCHEMA="data_lake_fdw2"
WAREHOUSE_EXTENSION="postgres_fdw"
DATA_LAKE_SERVER="server_data_lake2"


# Create schema to store the connection
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB --command="CREATE SCHEMA IF NOT EXISTS "$DATA_LAKE_FDW_SCHEMA";"

# Install extension 
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB --command="CREATE EXTENSION IF NOT EXISTS "$WAREHOUSE_EXTENSION";"

# Creating server and user mappings
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB \
  --command="CREATE SERVER IF NOT EXISTS "$DATA_LAKE_SERVER" FOREIGN DATA WRAPPER "$WAREHOUSE_EXTENSION" OPTIONS (dbname '"$DATA_LAKE_DB"', port '"$POSTGRES_PORT"', host '"$POSTGRES_HOST"');"

psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB \
  --command="CREATE USER MAPPING IF NOT EXISTS FOR "$POSTGRES_USER" SERVER "$DATA_LAKE_SERVER" OPTIONS (user '"$POSTGRES_USER"', password '"$POSTGRES_PASSWORD"');"

# psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB \
#   --command="GRANT USAGE ON "$DATA_LAKE_FDW_SCHEMA" TO "$POSTGRES_USER";"

# echo " 
# TEST
# "
# psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB \
#   --command="select * from data_lake_fdw2.twitter_data;"

# Create connection from the warehouse database to the data_lake database -- fake tables should appear
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB \
  --command="IMPORT FOREIGN SCHEMA "$DATA_LAKE_DB" FROM SERVER "$DATA_LAKE_SERVER" INTO "$DATA_LAKE_FDW_SCHEMA";"

echo "
postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
echo "CREATE SCHEMA IF NOT EXISTS "$DATA_LAKE_FDW_SCHEMA";"
echo "CREATE EXTENSION IF NOT EXISTS "$WAREHOUSE_EXTENSION";"
echo "CREATE SERVER IF NOT EXISTS "$DATA_LAKE_SERVER" FOREIGN DATA WRAPPER "$WAREHOUSE_EXTENSION" OPTIONS (dbname '"$DATA_LAKE_DB"', port '"$POSTGRES_PORT"', host '"$POSTGRES_HOST"');"
echo "CREATE USER MAPPING IF NOT EXISTS FOR "$POSTGRES_USER" SERVER "$DATA_LAKE_SERVER" OPTIONS (user '"$POSTGRES_USER"', password '"$POSTGRES_PASSWORD"');"
echo "IMPORT FOREIGN SCHEMA "$DATA_LAKE_DB" FROM SERVER "$DATA_LAKE_SERVER" INTO "$DATA_LAKE_FDW_SCHEMA";"



