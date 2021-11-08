from datetime import datetime, date, timedelta
import json
from deps.psql_api import Psql
from deps.twitter_api import TwitterClient
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
        tweets = api.get_tweets(query="", count=10000, geo="41.3850639,2.1734035,5km", lang="en", until=yesterday)  # center of Barcelona plus 5km radius  41.3850639, 2.1734035, 5km
        with Psql(db_name="data_lake")  as conn:
            for t in tweets:
                print(t.id)
                tweet_id = t.id
                tweet_json = json.dumps(t._json)
                Psql.insert_json_tweet(conn, tweet_id, tweet_json)
        return "Tweets fetched and stored in postgres-dw"

    t1 = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        op_kwargs={"context": "FETCHING TWEETS"},
    )

    t2 = PythonOperator(
        task_id='fetch_tweets',
        python_callable=fetch_tweets,
    )

    t1 >> t2
