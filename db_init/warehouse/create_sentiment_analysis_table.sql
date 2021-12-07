CREATE TABLE IF NOT EXISTS data_warehouse2.sentiment_analysis (
	tweet_id bpchar(20) NOT NULL,
	tweet_day bpchar(10) NOT NULL,
	weather_status bpchar(50) NULL,
	tweet_text varchar NULL,
	tweet_sentiment bpchar(10) NULL,
	CONSTRAINT sentiment_analysis_un UNIQUE (tweet_id)
);