from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from deps.psql_api import Psql
from deps.twitter_api import TwitterClient
import json

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}


with DAG(
    'dag_python_test',
    default_args=default_args,
    description='Test Python DAG in Docker container',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    def print_context(context):
        print(context)
        return "context printed"

    t1 = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        op_kwargs={"context": "FETCHING TWEETS"},
    )

    def fetch_tweets():
        api = TwitterClient()
        tweets = api.get_raw_tweets(query="", count=10000, geo="41.3850639,2.1734035,5km")  # center of Barcelona plus 5km radius  41.3850639, 2.1734035, 5km
        with Psql(db_name="data_lake")  as conn:
            for t in tweets:
                print(t.id)
                tweet_id = t.id
                tweet_json = json.dumps(t._json)
                Psql.insert_raw_tweet(conn, tweet_id, tweet_json)

    t2 = PythonOperator(
        task_id='fetch_tweets',
        python_callable=fetch_tweets,
    )

    t1 >> t2



    # def short_task(seconds):
    #     print("Starting short task... ", end="")
    #     time.sleep(seconds)
    #     print("short task completed")

    # t2 = PythonOperator(
    #     task_id='short_task',
    #     python_callable=short_task,
    #     op_kwargs={"seconds": 5},
    # )

    # def long_task(seconds):
    #     print("Starting long task... ", end="")
    #     time.sleep(seconds)
    #     print("long task completed")

    # t3 = PythonOperator(
    #     task_id='long_task',
    #     python_callable=long_task,
    #     op_kwargs={"seconds": 10},
    # )

    # t1 >> [t2, t3]
