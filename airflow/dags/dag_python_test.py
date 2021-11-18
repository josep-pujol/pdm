import json
from datetime import date, datetime, timedelta

from deps.psql_api import Psql
from deps.twitter_api import TwitterClient
from deps.weather_api import get_weather

from airflow import DAG
from airflow.operators.python import PythonOperator


# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

with DAG(
    'dag_python_test',
    default_args=default_args,
    description='First version of DAG to fetch Tweets and store them in Postgres',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    def print_context(context):
        """Just to print information about the following task to execute"""
        print(context)
        return "Context printed"

    def fetch_tweets():
        """calls the Twitter API to fetch tweets and then store results in postgres-dw"""
        yesterday = str(date.today() - timedelta(days=-1))
        api = TwitterClient()
        # geo parameter: center of Barcelona plus 5km radius  41.3850639, 2.1734035, 5km
        tweets = api.get_tweets(query="", count=10000, geo="41.3850639,2.1734035,5km", lang="en", until=yesterday)
        with Psql(db_name="data_lake")  as conn:
            for t in tweets:
                print(t.id)
                tweet_id = t.id
                tweet_json = json.dumps(t._json)
                Psql.insert_json_tweet(conn, tweet_id, tweet_json)
        return "Tweets fetched and stored in postgres-dw"

    def fetch_weather():
        w = get_weather(owm_location="Barcelona,ES").to_dict()
        with Psql(db_name="data_lake")  as conn:
            print(w)
            weather_id = str(date.today())
            weather_json = json.dumps(w)
            Psql.insert_json_weather(conn, weather_id, weather_json)
        return "Current Weather fetched and stored in postgres-dw"

    def sync_weather():
        with Psql(db_name="data_lake")  as conn_dl:
            with Psql(db_name="warehouse")  as conn_dw:
                Psql.sync_weather_dl2dw(conn_dl, conn_dw)
        return "Weather data synchronized"

    def sync_twitter():
        with Psql(db_name="data_lake")  as conn_dl:
            with Psql(db_name="warehouse")  as conn_dw:
                Psql.sync_twitter_dl2dw(conn_dl, conn_dw)
        return "Twitter data synchronized"

    def generate_analytics():
        with Psql(db_name="warehouse")  as conn_dw:
            Psql.generate_analytics_data(conn_dw)
        return "Analytics data generated"

    def generate_analytics_sentiment_analysis_score():
        with Psql(db_name="warehouse")  as conn_dw:
            Psql.generate_sentiment_analysis_score(conn_dw)
        return "Sentiment analysis scores generated"


    t1 = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        op_kwargs={"context": "FETCHING DATA"},
    )

    t2 = PythonOperator(
        task_id='fetch_tweets',
        python_callable=fetch_tweets,
    )

    t3 = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather,
    )

    t4 = PythonOperator(
        task_id='sync_twitter',
        python_callable=sync_twitter
    )

    t5 = PythonOperator(
        task_id='sync_weather',
        python_callable=sync_weather
    )

    t6 = PythonOperator(
        task_id='generate_analytics',
        python_callable=generate_analytics
    )

    t7 = PythonOperator(
        task_id='generate_analytics_sentiment_analysis_score',
        python_callable=generate_analytics_sentiment_analysis_score
    )


    t1 >> [t2, t3]
    t2 >> t4
    t3 >> t5
    t4 >> t6
    t5 >> t6
    t6 >> t7
