CREATE TABLE IF NOT EXISTS data_warehouse2.weather (
	weather_location bpchar(50) NOT NULL,
	detailed_status bpchar(50) NULL,
	humidity int2 NULL,
	pressure int2 NULL,
	temperature_kelvins numeric(5, 2) NULL,
	weather_id bpchar(10) NOT NULL,
	CONSTRAINT weather_un UNIQUE (weather_id)
);