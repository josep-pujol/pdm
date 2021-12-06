#!/bin/bash

# before executing do: chmod 755 init_dbs.sh
# https://www.manniwood.com/postgresql_and_bash_stuff/

# get environ variables
set -o allexport
source .env
set +o allexport


# If psql is not available, then exit
if ! command -v psql > /dev/null; then
  echo "This script requires psql to be installed and on your PATH ..."
  exit 1
fi
echo "psql check done"
echo $POSTGRES_USER
echo $POSTGRES_DB
echo $POSTGRES_HOST
echo $POSTGRES_PASSWORD



# ----------------
# -- DATA LAKE  --
# ----------------
DATA_LAKE_DB="data_lake2"


# Create data_lake Database  # https://zaiste.net/databases/postgresql/howtos/create-database-if-not-exists/
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@0.0.0.0:5433/$POSTGRES_DB -tc "SELECT 1 FROM pg_database WHERE datname = '"$DATA_LAKE_DB"'" | grep -q 1 | psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@0.0.0.0:5433/$POSTGRES_DB -c "CREATE DATABASE "$DATA_LAKE_DB

# Create data_lake Schema
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@0.0.0.0:5433/$DATA_LAKE_DB --command="$(<"db_init/data_lake/create_data_lake_schema.sql")"

# Create twitter_data table
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@0.0.0.0:5433/$DATA_LAKE_DB --command="$(<"db_init/data_lake/create_twitter_data_table.sql")"

# Create weather_data table
psql postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@0.0.0.0:5433/$DATA_LAKE_DB --command="$(<"db_init/data_lake/create_weather_data_table.sql")"



# ---------------------
# -- DATA WAREHOUSE  --
# ---------------------

# just for testing
POSTGRES_DB="warehouse2"

# CREATE TABLE IF NOT EXISTS data_warehouse2.twitter (
# 	tweet_id bpchar(20) NOT NULL,
# 	created_at bpchar(50) NOT NULL,
# 	is_truncated bool NULL,
# 	tweet_text varchar NULL,
# 	tweet_location bpchar(50) NULL,
# 	description varchar NULL,
# 	CONSTRAINT twitter_un UNIQUE (tweet_id)
# );

# CREATE TABLE IF NOT EXISTS data_warehouse2.weather (
# 	weather_location bpchar(50) NOT NULL,
# 	detailed_status bpchar(50) NULL,
# 	humidity int2 NULL,
# 	pressure int2 NULL,
# 	temperature_kelvins numeric(5, 2) NULL,
# 	weather_id bpchar(10) NOT NULL,
# 	CONSTRAINT weather_un UNIQUE (weather_id)
# );

# CREATE TABLE IF NOT EXISTS data_warehouse2.sentiment_analysis (
# 	tweet_id bpchar(20) NOT NULL,
# 	tweet_day bpchar(10) NOT NULL,
# 	weather_status bpchar(50) NULL,
# 	tweet_text varchar NULL,
# 	tweet_sentiment bpchar(10) NULL,
# 	CONSTRAINT sentiment_analysis_un UNIQUE (tweet_id)
# );

# -- # TODO: programatically add connections data_warehouse and data_lake into Airflow



# -------------------------------------
# -- CREATE FOREIGN DATABASE WRAPPER --
# -------------------------------------

# -- Connection created in the Warehouse database
# CREATE SCHEMA IF NOT EXISTS data_lake_fdw2;

# CREATE EXTENSION IF NOT EXISTS postgres_fdw2;

# CREATE SERVER IF NOT EXISTS data_lake2 
# FOREIGN DATA WRAPPER postgres_fdw2 
# OPTIONS (dbname 'data_lake', port '5432', host 'localhost');

# CREATE USER MAPPING IF NOT EXISTS FOR warehouse
# SERVER data_lake2 
# OPTIONS (user 'warehouse', password 'warehouse');  -- TODO: refractor, hide passwords


# IMPORT FOREIGN SCHEMA data_lake2 FROM SERVER data_lake2 INTO data_lake_fdw2;
