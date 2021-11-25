
INSERT INTO data_warehouse.sentiment_analysis (tweet_id, tweet_day, weather_status, tweet_text)

SELECT 					   t.tweet_id 
    ,w.weather_id   	AS tweet_day
    ,w.detailed_status  AS weather_status
    ,t.tweet_text 		AS tweet_text 
FROM data_warehouse.twitter t
INNER JOIN data_warehouse.weather w 
    ON to_char(to_date(created_at, 'Dy Mon DD HH24:MI:SS TZHTZM YYYY'), 'YYYY-MM-DD') = w.weather_id

ON CONFLICT ON CONSTRAINT sentiment_analysis_un DO NOTHING;
