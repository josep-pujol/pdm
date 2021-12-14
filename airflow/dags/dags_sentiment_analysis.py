from datetime import date, datetime, timedelta

from deps.psql_api import Psql
from deps.twitter_api import TwitterClient
from deps.weather_api import get_weather

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import json
from psycopg2.extras import Json

# These args will get passed on to each operator
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    "dags_sentiment_analysis",
    default_args=default_args,
    description="Fetches data from Twitter and Open Weather Map and performs a sentiment analysis of the tweets",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # tags=['test'],
) as dag:

    def print_context(context):
        """Just to print information about the following task to execute"""
        print(context)
        return "Context printed"

    def fetch_tweets():
        """Calls the Twitter API to fetch tweets and then store results in postgres-dw"""
        yesterday = str(date.today() - timedelta(days=-1))
        api = TwitterClient()
        # geo parameter: center of Barcelona plus 5km radius  41.3850639, 2.1734035, 5km
        tweets = api.get_tweets(
            query="",
            count=10000,
            geo="41.3850639,2.1734035,5km",
            lang="en",
            until=yesterday,
        )
        with Psql(db_name="data_lake") as conn:
            # Issues encoding emoticons. To solve, convert json to Postgres Json object 
            print(f"Returned {len(tweets)} tweets")
            tweets_to_insert = [
                {"tweet_id": t.id, "tweet_json": Json(json.dumps(t._json))}
                for t in tweets
            ]
            Psql.insert_rows(
                connection=conn,
                schema_table_name="data_lake.twitter_data",
                data_to_insert=tweets_to_insert,
                sql_constraint="twitter_data_un"
            )
        return "Tweets fetched and stored in postgres-dw"

    def fetch_weather():
        weather = get_weather(owm_location="Barcelona,ES").to_dict()
        with Psql(db_name="data_lake") as conn:
            print(weather)
            # weather_to_insert converted to list as Psql.insert_rows requires a list
            weather_to_insert = [
                {"weather_id": str(date.today()), "weather_json": Json(json.dumps(weather))}
            ]
            Psql.insert_rows(
                connection=conn,
                schema_table_name="data_lake.weather_data",
                data_to_insert=weather_to_insert,
                sql_constraint="weather_data_un"
            )
        return "Current Weather fetched and stored in postgres-dw"


    printContext = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
        op_kwargs={"context": "FETCHING DATA"},
    )

    fetchTweets = PythonOperator(
        task_id="fetch_tweets",
        python_callable=fetch_tweets,
    )

    fetchWeather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    syncTwitter = PostgresOperator(
        task_id="sync_twitter",
        postgres_conn_id="postgres_warehouse",
        sql="sql/sync_twitter_dl2dw.sql",
    )

    syncWeather = PostgresOperator(
        task_id="sync_weather",
        postgres_conn_id="postgres_warehouse",
        sql="sql/sync_weather_dl2dw.sql",
    )

    #  Mixes data from Twitter and Weather tables into a better format aimed at data analytics
    generateAnalytics = PostgresOperator(  # TODO: did setup connection manually in Airflow UI. Do it programatically.
        task_id="generate_analytics",
        postgres_conn_id="postgres_warehouse",
        sql="sql/generate_analytics_data.sql",
    )

    generateAnalyticsSentimentAnalysisScore = PostgresOperator(
        task_id="generate_analytics_sentiment_analysis_score",
        postgres_conn_id="postgres_warehouse",
        sql="sql/generate_sentiment_analysis_score.sql",
    )

    printContext >> [fetchTweets, fetchWeather]
    fetchTweets >> syncTwitter
    fetchWeather >> syncWeather
    syncTwitter >> generateAnalytics
    syncWeather >> generateAnalytics
    generateAnalytics >> generateAnalyticsSentimentAnalysisScore
