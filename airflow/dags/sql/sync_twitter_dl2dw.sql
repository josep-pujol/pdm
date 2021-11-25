
INSERT INTO data_warehouse.twitter (tweet_id, created_at, is_truncated, tweet_text, tweet_location, description)

SELECT 													    	       tweet_id
    ,(tweet_json #>>'{}')::jsonb ->> 'created_at' 					AS created_at
    ,CAST((tweet_json #>>'{}')::jsonb ->> 'truncated' AS BOOLEAN)	AS is_truncated
    ,(tweet_json #>>'{}')::jsonb ->> 'text' 						AS tweet_text
    ,(tweet_json #>>'{}')::jsonb -> 'user' ->> 'location' 			AS tweet_location
    ,(tweet_json #>>'{}')::jsonb -> 'user' ->> 'description' 		AS description
FROM data_lake_fdw.twitter_data

ON CONFLICT ON CONSTRAINT twitter_un DO NOTHING;
