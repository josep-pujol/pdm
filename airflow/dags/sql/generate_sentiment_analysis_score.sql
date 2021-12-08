
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

SELECT generate_sentiment_score('hello amazing bad car');
