import os
import urllib
from typing import Union

from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2.extras import Json
from sqlalchemy import create_engine

from deps.sentiment_analysis import get_sentiment_score  # TODO: how to avoid this

load_dotenv()


class Psql:
    """
    Postgresql Database client
    """

    def __init__(
        self,
        host: str = os.getenv("POSTGRES_HOST"),
        port: Union[str, int] = os.getenv("POSTGRES_PORT"),
        usr: str = os.getenv("POSTGRES_USER"),
        pwd: str = os.getenv("POSTGRES_PASSWORD"),
        db_name: str = os.getenv("POSTGRES_DB"),
    ):
        self.host = host
        self.port = port
        self.usr = usr
        self.pwd = pwd
        self.db_name = db_name
        self.conn_obj = None
        self.__handle_miss_params__()

    def __handle_miss_params__(self):
        if not all((self.host, self.port, self.usr, self.pwd, self.db_name)):
            raise ValueError("Missing value(s) to connect to db")

    def __repr__(self):
        return f"<{self.__class__.__name__} to {self.db_name}>"

    def __enter__(self):
        print(f"Connecting to {self.db_name}... ", end="")

        def build_conn_string():
            encoded_pwd = urllib.parse.quote_plus(self.pwd)
            return (
                f"postgresql://{self.usr}:{encoded_pwd}"
                f"@{self.host}:{self.port}/{self.db_name}"
            )

        def get_conn(connection_string: str):
            try:
                engine = create_engine(connection_string)
                self.conn_obj = engine.connect()
            except OperationalError as err:
                print("Exception while connecting to db: ", err)

            return self.conn_obj

        conn_string = build_conn_string()
        return get_conn(conn_string)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn_obj.close()
        print(f"Connection to {self.db_name} closed")

    @staticmethod
    def insert_json_tweet(connection=None, tweet_id=None, tweet_json=None):
        """insert into postgres-dw a tweet in json format"""
        try:
            connection.execute(
                "insert into data_lake.twitter_data (tweet_id, tweet_json) values ((%s), (%s)) ON CONFLICT ON CONSTRAINT twitter_data_un DO NOTHING;",
                [tweet_id, Json(tweet_json)],
            )
        except Exception as err:
            print("Exception while executing db query: ", err)

    @staticmethod
    def insert_json_weather(connection=None, weather_id=None, weather_json=None):
        """insert into postgres-dw data fetched from weather forecast api in json format"""
        try:
            connection.execute(
                "insert into data_lake.weather_data (weather_id, weather_json) values ((%s), (%s)) ON CONFLICT ON CONSTRAINT weather_data_un DO NOTHING;",
                [weather_id, Json(weather_json)],
            )
        except Exception as err:
            print("Exception while executing db query: ", err)

    @staticmethod
    def query_results_generator(query_results=None):
        """Useful to tackle one query result at a time"""
        for row in query_results:
            yield row

    @staticmethod
    def sync_weather_dl2dw(conn_datalake=None, conn_datawarehouse=None):
        """Gets data from the data lake in tabular format and inserts
        the data in the data warehouse. Duplicate values are ignored"""
        json_to_table = """
        select                                                                             weather_id
            ,(weather_json #>> '{}')::jsonb -> 'location' ->> 'name' 					as weather_location
            ,(weather_json #>> '{}')::jsonb -> 'weather' ->> 'detailed_status' 			as detailed_status
            ,(weather_json #>> '{}')::jsonb -> 'weather' ->> 'humidity' 				as humidity
            ,(weather_json #>> '{}')::jsonb -> 'weather' -> 'pressure' ->> 'press'		as pressure
            ,(weather_json #>> '{}')::jsonb -> 'weather' -> 'temperature' ->> 'temp'	as temperature_kelvins
        from data_lake.weather_data;
        """

        insert_into_weather = """
        insert into data_warehouse.weather (weather_id, weather_location, detailed_status, humidity, pressure, temperature_kelvins)
        values ((%s), (%s), (%s), (%s), (%s), (%s)) ON CONFLICT ON CONSTRAINT weather_un DO NOTHING;
        """

        result = conn_datalake.execute(json_to_table)
        generator = Psql.query_results_generator(result.fetchall())
        for row in generator:
            print(row)
            conn_datawarehouse.execute(insert_into_weather, row)

    @staticmethod
    def sync_twitter_dl2dw(conn_datalake=None, conn_datawarehouse=None):
        """Gets data from the data lake in tabular format and inserts
        the data in the data warehouse. Duplicate values are ignored"""
        json_to_table = """
        select 														       tweet_id
            ,(tweet_json #>>'{}')::jsonb ->> 'created_at' 				as created_at
            ,(tweet_json #>>'{}')::jsonb ->> 'truncated' 				as is_truncated
            ,(tweet_json #>>'{}')::jsonb ->> 'text' 					as tweet_text
            ,(tweet_json #>>'{}')::jsonb -> 'user' ->> 'location' 		as tweet_location
            ,(tweet_json #>>'{}')::jsonb -> 'user' ->> 'description' 	as description
        from data_lake.twitter_data
        """

        insert_into_weather = """
        insert into data_warehouse.twitter (tweet_id, created_at, is_truncated, tweet_text, tweet_location, description)
        values ((%s), (%s), (%s), (%s), (%s), (%s)) ON CONFLICT ON CONSTRAINT twitter_un DO NOTHING;
        """

        results = conn_datalake.execute(json_to_table)
        generator = Psql.query_results_generator(results.fetchall())
        for row in generator:
            print(row)
            conn_datawarehouse.execute(insert_into_weather, row)

    @staticmethod
    def generate_analytics_data(conn_datawarehouse=None):
        """Mixes data from Twitter and Weather tables into a better
        format aimed at data analytics"""
        generate_analytics = """
        select 					   t.tweet_id 
	        ,w.weather_id   	as tweet_day
	        ,w.detailed_status  as weather_status
	        ,t.tweet_text 		as tweet_text 
        from data_warehouse.twitter t
        inner join data_warehouse.weather w on to_char(to_date(created_at, 'Dy Mon DD HH24:MI:SS TZHTZM YYYY'), 'YYYY-MM-DD') = w.weather_id ;
        """
        insert_analytics = """
        insert into data_warehouse.sentiment_analysis (tweet_id, tweet_day, weather_status, tweet_text)
        values ((%s), (%s), (%s), (%s)) ON CONFLICT ON CONSTRAINT sentiment_analysis_un DO NOTHING
        """

        results = conn_datawarehouse.execute(generate_analytics)
        generator = Psql.query_results_generator(results.fetchall())
        for row in generator:
            print(row)
            conn_datawarehouse.execute(insert_analytics, row)

    @staticmethod
    def generate_sentiment_analysis_score(conn_datawarehouse=None):
        """Check for missing sentiment analysis scores, generates them and
        stores them in the data_warehouse.sentiment_analysis table"""

        rows_to_analyse = """
        select tweet_id 
            ,tweet_text 
        from data_warehouse.sentiment_analysis sa 
        where tweet_text is not null and tweet_sentiment is null;
        """
        update_analytics = """
        update data_warehouse.sentiment_analysis 
        set tweet_sentiment = (%s)
        where tweet_id = (%s); 
        """
        results = conn_datawarehouse.execute(rows_to_analyse)
        generator = Psql.query_results_generator(results.fetchall())
        for tweet_id, text in generator:
            try:
                analysis_score = get_sentiment_score(text)
                conn_datawarehouse.execute(update_analytics, [analysis_score, tweet_id])
            except Exception as err:
                print(
                    f"Exception while generating sentiment analysis score for tweet_id: {tweet_id}\n",
                    err,
                )


def main():
    """For Testing purposes"""
    with Psql(db_name="data_lake", host="0.0.0.0", port=5433) as conn:
        res = conn.execute("SELECT version();")
        print(res.rowcount)


if __name__ == "__main__":
    main()
