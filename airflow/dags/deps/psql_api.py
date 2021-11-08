import os
from typing import Union, ValuesView
import urllib
from sqlalchemy import create_engine
from psycopg2.extras import Json
from psycopg2 import OperationalError

from dotenv import load_dotenv

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
        if not(all((self.host, self.port, self.usr, self.pwd, self.db_name))):
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

def main():
    """For Testing purposes"""
    with Psql() as conn:
        res = conn.execute("SELECT version();")
        print(res.rowcount)


if __name__ == "__main__":
    main()
