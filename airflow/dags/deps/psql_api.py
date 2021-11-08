import os
import urllib
from sqlalchemy import create_engine
from psycopg2.extras import Json

from dotenv import load_dotenv

load_dotenv()


class Psql:
    """
    Postgresql Database client
    """

    def __init__(
        self,
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        usr=os.getenv("POSTGRES_USER"),
        pwd=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
    ):
        self.host = host
        self.port = port
        self.usr = usr
        self.pwd = pwd
        self.db_name = db_name
        self.conn_obj = None

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

        def get_conn(connection_string):
            engine = create_engine(connection_string)
            self.conn_obj = engine.connect()
            return self.conn_obj

        conn_string = build_conn_string()
        return get_conn(conn_string)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn_obj.close()
        print(f"Connection to {self.db_name} closed")

    @staticmethod
    def insert_raw_tweet(connection=None, tweet_id=None, tweet_json=None):
        connection.execute(
            "insert into data_lake.twitter_data (tweet_id, tweet_json) values ((%s), (%s)) ON CONFLICT ON CONSTRAINT twitter_data_un DO NOTHING;",
            [tweet_id, Json(tweet_json)],
        )


def main():
    with Psql() as conn:
        res = conn.execute("SELECT version();")
        print(res.rowcount)


if __name__ == "__main__":
    main()
