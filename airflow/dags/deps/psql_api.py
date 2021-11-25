import json
import os
import urllib
from typing import Iterable, List, Union

from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2.extras import Json
from sqlalchemy import create_engine

 # TODO: how to avoid having this function in this module

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
        if not all((self.host, self.port, self.usr, self.pwd)):
            raise ValueError("Missing value(s) to connect to db")

    def __repr__(self):
        if self.db_name:
            return f"<{self.__class__.__name__} to {self.db_name}>"
        return f"<{self.__class__.__name__} no database defined>"

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

    # Parameters "exc_type, exc_value, exc_traceback" needed when tearing down class
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn_obj.close()
        print(f"Connection to {self.db_name} closed")

    @staticmethod
    def insert_json(connection, schema_table_name, json_data, sql_constraint=None):  # TODO: refractor
        """insert data in json format into a table"""
        cols = list(json_data.keys())
        insert_sql = "INSERT INTO %s (%s) " % (schema_table_name, ", ".join(cols))
        values_sql = "VALUES ((%s), (%s)) "
        sql = insert_sql + values_sql
        if sql_constraint:
            sql = f"{sql} ON CONFLICT ON CONSTRAINT {sql_constraint} DO NOTHING"
        sql = sql + ";"

        try:
            connection.execute(
                sql, [json_data[cols[0]], Json(json.dumps(json_data[cols[1]]))]
            )
        except Exception as err:
            print("Exception while executing db query: ", err)

    @staticmethod
    def insert_rows(connection, schema_table_name: str, data_to_insert: List[dict]) -> None:
        column_names = ", ".join(data_to_insert[0].keys())
        values = "%(" + ")s, %(".join(data_to_insert[0].keys()) + ")s"
        insert_sql = f"INSERT INTO {schema_table_name} ({column_names}) "
        values_sql = f"VALUES ({values});"

        # https://wiki.postgresql.org/wiki/Psycopg2_Tutorial
        # https://www.semicolonworld.com/question/43429/psycopg2-insert-multiple-rows-with-one-query
        try:
            connection.execute(insert_sql + values_sql, data_to_insert)
        except Exception as err:
            print("Exception while executing db query: ", err)
    
    @staticmethod
    def query_results_generator(query_results=None):
        """Useful to tackle one query result at a time"""
        for row in query_results:
            yield row

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
    print("stop")
    # with Psql(db_name="data_lake", host="0.0.0.0", port=5433) as conn:
    #     res = conn.execute("SELECT version();")
    #     print(res.fetchone() )
    #     res = conn.execute("SELECT * FROM data_lake.weather_data;")
    #     print(res.rowcount)

if __name__ == "__main__":
    main()
