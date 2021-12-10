-- python part: # https://www.tangramvision.com/blog/creating-postgresql-test-data-with-sql-pl-pgsql-and-python#importing-external-py-files
CREATE OR REPLACE FUNCTION generate_sentiment_score (tweet_text text)
RETURNS TEXT
AS $$
    import importlib.util
    spec = importlib.util.spec_from_file_location("sentiment_analysis", "/dags_python_dependencies/sentiment_analysis.py")
    sentiment_analysis = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sentiment_analysis)
    return sentiment_analysis.get_sentiment_score(tweet_text)
$$ LANGUAGE 'plpython3u';

UPDATE data_warehouse.sentiment_analysis AS sa
SET tweet_sentiment = generate_sentiment_score(ta.tweet_text)
FROM (
    SELECT tweet_id 
        ,tweet_text 
    FROM data_warehouse.sentiment_analysis
    WHERE (tweet_text IS NOT NULL AND tweet_sentiment IS NULL)
) AS ta
WHERE sa.tweet_id = ta.tweet_id;
