import json
import os
import urllib
from typing import List, Union
from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2.extras import Json
from sqlalchemy import create_engine


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
    def insert_rows(
        connection, schema_table_name: str, data_to_insert: List[dict], sql_constraint=None
    ) -> None:
        """Generic method to insert a list of dictionaries into a table"""
        # https://www.datacareer.ch/blog/improve-your-psycopg2-executions-for-postgresql-in-python/
        # https://hakibenita.com/fast-load-data-python-postgresql
        column_names = ", ".join(data_to_insert[0].keys())
        values = "%(" + ")s, %(".join(data_to_insert[0].keys()) + ")s"
        insert_sql = f"INSERT INTO {schema_table_name} ({column_names}) "
        values_sql = f"VALUES ({values})"
        sql = insert_sql + values_sql
        if sql_constraint:
            sql = f"{sql} ON CONFLICT ON CONSTRAINT {sql_constraint} DO NOTHING"
        sql = sql + ";"

        try:
            connection.execute(sql, data_to_insert)
        except Exception as err:
            print("Exception while executing db query: ", err)


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
