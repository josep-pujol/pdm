
INSERT INTO data_warehouse.weather (weather_id, weather_location, detailed_status, humidity, pressure, temperature_kelvins)

SELECT                                                                             	   		       weather_id
    ,(weather_json #>> '{}')::jsonb -> 'location' ->> 'name' 									AS weather_location
    ,(weather_json #>> '{}')::jsonb -> 'weather' ->> 'detailed_status' 							AS detailed_status
    ,CAST((weather_json #>> '{}')::jsonb -> 'weather' ->> 'humidity' AS INTEGER) 				AS humidity
    ,CAST((weather_json #>> '{}')::jsonb -> 'weather' -> 'pressure' ->> 'press'	AS	INTEGER)    AS pressure
    ,CAST((weather_json #>> '{}')::jsonb -> 'weather' -> 'temperature' ->> 'temp' AS NUMERIC)	AS temperature_kelvins
FROM data_lake_fdw.weather_data

ON CONFLICT ON CONSTRAINT weather_un DO NOTHING;
