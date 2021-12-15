import os
from typing import List, Union
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_batch

load_dotenv()


def __handle_miss_params__(
    host: str, port: Union[str, int], usr: str, pwd: str
) -> None:
    if not all((host, port, usr, pwd)):
        raise ValueError("Missing value(s) to connect to db")


def get_conn(
    host: str = None,
    port: Union[str, int] = None,
    usr: str = None,
    pwd: str = None,
    db_name: str = None,
) -> connection:
    host = host if host else os.getenv("POSTGRES_HOST")
    port = port if port else os.getenv("POSTGRES_PORT")
    usr = usr if usr else os.getenv("POSTGRES_USER")
    pwd = pwd if pwd else os.getenv("POSTGRES_PASSWORD")
    db_name = db_name if db_name else os.getenv("POSTGRES_DB")
    __handle_miss_params__(host, port, usr, pwd)

    return psycopg2.connect(
        host=host,
        port=port,
        user=usr,
        password=pwd,
        database=db_name,
    )


def insert_rows(
    conn: connection,
    schema_table_name: str,
    data_to_insert: List[dict],
    sql_constraint: str = None,
) -> None:
    """Generic method to insert a list of dictionaries into a Postgres table"""
    column_names = ", ".join(data_to_insert[0].keys())
    values = "%(" + ")s, %(".join(data_to_insert[0].keys()) + ")s"
    insert_sql = f"INSERT INTO {schema_table_name} ({column_names}) "
    values_sql = f"VALUES ({values})"
    sql = insert_sql + values_sql
    if sql_constraint:
        sql = f"{sql} ON CONFLICT ON CONSTRAINT {sql_constraint} DO NOTHING"
    sql = sql + ";"

    cur: cursor = conn.cursor()
    execute_batch(cur, sql, data_to_insert)
    conn.commit()
    cur.close()


def main():
    """For Testing purposes"""
    print("stop")
    conn = get_conn(db_name="data_lake", host="0.0.0.0", port=5433)
    cur = conn.cursor()
    
    cur.execute("SELECT version();")
    print(cur.fetchone())
    cur.execute("SELECT * FROM data_lake.weather_data;")
    print(cur.rowcount)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
