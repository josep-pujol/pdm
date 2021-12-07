CREATE TABLE IF NOT EXISTS data_warehouse2.twitter (
	tweet_id bpchar(20) NOT NULL,
	created_at bpchar(50) NOT NULL,
	is_truncated bool NULL,
    tweet_text varchar NULL,
	tweet_location bpchar(50) NULL,
	description varchar NULL,
    CONSTRAINT twitter_un UNIQUE (tweet_id)
);