
INSERT INTO data_warehouse.weather (weather_id, weather_location, detailed_status, humidity, pressure, temperature_kelvins)

SELECT                                                                             weather_id
    ,(weather_json #>> '{}')::jsonb -> 'location' ->> 'name' 					AS weather_location
    ,(weather_json #>> '{}')::jsonb -> 'weather' ->> 'detailed_status' 			AS detailed_status
    ,(weather_json #>> '{}')::jsonb -> 'weather' ->> 'humidity' 				AS humidity
    ,(weather_json #>> '{}')::jsonb -> 'weather' -> 'pressure' ->> 'press'		AS pressure
    ,(weather_json #>> '{}')::jsonb -> 'weather' -> 'temperature' ->> 'temp'	AS temperature_kelvins
FROM data_lake.weather_data

ON CONFLICT ON CONSTRAINT weather_un DO NOTHING;

