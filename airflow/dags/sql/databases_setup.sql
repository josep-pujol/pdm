----------------------
-- DATA LAKE TABLES --
----------------------

CREATE SCHEMA data_lake AUTHORIZATION warehouse;

CREATE TABLE data_lake.twitter_data (
	tweet_id bpchar(20) NOT NULL,
	tweet_json jsonb NOT NULL,
	CONSTRAINT twitter_data_un UNIQUE (tweet_id)
);

CREATE TABLE data_lake.weather_data (
	weather_id bpchar(10) NOT NULL,
	weather_json jsonb NOT NULL,
	CONSTRAINT weather_data_un UNIQUE (weather_id)
);



---------------------------
-- DATA WAREHOUSE TABLES --
---------------------------

CREATE SCHEMA data_warehouse AUTHORIZATION warehouse;

CREATE TABLE data_warehouse.twitter (
	tweet_id bpchar(20) NOT NULL,
	created_at bpchar(50) NOT NULL,
	is_truncated bool NULL,
	tweet_text varchar NULL,
	tweet_location bpchar(50) NULL,
	description varchar NULL,
	CONSTRAINT twitter_un UNIQUE (tweet_id)
);

CREATE TABLE data_warehouse.weather (
	weather_location bpchar(50) NOT NULL,
	detailed_status bpchar(50) NULL,
	humidity int2 NULL,
	pressure int2 NULL,
	temperature_kelvins numeric(5, 2) NULL,
	weather_id bpchar(10) NOT NULL,
	CONSTRAINT weather_un UNIQUE (weather_id)
);

CREATE TABLE data_warehouse.sentiment_analysis (
	tweet_id bpchar(20) NOT NULL,
	tweet_day bpchar(10) NOT NULL,
	weather_status bpchar(50) NULL,
	tweet_text varchar NULL,
	tweet_sentiment bpchar(10) NULL,
	CONSTRAINT sentiment_analysis_un UNIQUE (tweet_id)
);

-- # TODO: programatically add connection to data_warehouse and data_lake in Airflow



-------------------------------------
-- CREATE FOREIGN DATABASE WRAPPER --
-------------------------------------

-- Connection created in the Warehouse database
CREATE SCHEMA data_lake_fdw;

CREATE EXTENSION postgres_fdw;  -- should I do it the data_lake too?


CREATE SERVER data_lake 
FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (dbname 'data_lake', port '5432', host 'localhost');


CREATE USER MAPPING for warehouse
SERVER data_lake 
OPTIONS (user 'warehouse', password 'warehouse');  -- TODO: refractor, hide passwords


IMPORT FOREIGN SCHEMA data_lake from server data_lake into data_lake_fdw;


-- Sample to query the table sitting in the schema we imported 
select * from data_lake_fdw.test t 
