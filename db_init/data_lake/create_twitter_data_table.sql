CREATE TABLE IF NOT EXISTS data_lake2.twitter_data (
	tweet_id bpchar(20) NOT NULL,
	tweet_json jsonb NOT NULL,
 	CONSTRAINT twitter_data_un UNIQUE (tweet_id)
);