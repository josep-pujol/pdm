CREATE TABLE IF NOT EXISTS data_lake2.weather_data (
	weather_id bpchar(10) NOT NULL,
	weather_json jsonb NOT NULL,
	CONSTRAINT weather_data_un UNIQUE (weather_id)
);