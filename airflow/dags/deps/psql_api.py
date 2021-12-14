import os
from typing import List, Union
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError


load_dotenv()


class Psql:
    """
    Postgresql Database client
    """

    def __init__(
        self,
        host: str = None,
        port: Union[str, int] = None,
        usr: str = None,
        pwd: str = None,
        db_name: str = None,
    ):
        self.host = host if host else os.getenv("POSTGRES_HOST")
        self.port = port if port else os.getenv("POSTGRES_PORT")
        self.usr = usr if usr else os.getenv("POSTGRES_USER")
        self.pwd = pwd if pwd else os.getenv("POSTGRES_PASSWORD")
        self.db_name = db_name if db_name else os.getenv("POSTGRES_DB")
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
        self.conn_obj = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.usr,
            password=self.pwd,
            database=self.db_name,
        )
        return self.conn_obj

    # Parameters "exc_type, exc_value, exc_traceback" needed when tearing down class
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn_obj.close()
        print(f"Connection to {self.db_name} closed")

    @staticmethod
    def insert_rows(
        connection, schema_table_name: str, data_to_insert: List[dict], sql_constraint=None
    ) -> None:
        """Generic method to insert a list of dictionaries into a table"""
        column_names = ", ".join(data_to_insert[0].keys())
        values = "%(" + ")s, %(".join(data_to_insert[0].keys()) + ")s"
        insert_sql = f"INSERT INTO {schema_table_name} ({column_names}) "
        values_sql = f"VALUES ({values})"
        sql = insert_sql + values_sql
        if sql_constraint:
            sql = f"{sql} ON CONFLICT ON CONSTRAINT {sql_constraint} DO NOTHING"
        sql = sql + ";"

        try:
            cur = connection.cursor()
            # https://www.datacareer.ch/blog/improve-your-psycopg2-executions-for-postgresql-in-python/
            # https://hakibenita.com/fast-load-data-python-postgresql
            psycopg2.extras.execute_batch(cur, sql, data_to_insert)
            connection.commit()
        except Exception as err:
            print("Exception while executing db query: ", err)
            raise


def main():
    """For Testing purposes"""
    print("stop")
    with Psql(db_name="data_lake", host="0.0.0.0", port=5433) as conn:
        cur = conn.cursor()
        cur.execute("SELECT version();")
        print(cur.fetchone() )
        cur.execute("SELECT * FROM data_lake.weather_data;")
        print(cur.rowcount)


if __name__ == "__main__":
    main()
