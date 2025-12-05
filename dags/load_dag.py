from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import settings
from airflow.models.connection import Connection
import json

def create_trino_conn():
    # Создаем объект подключения
    conn = Connection(
        conn_id="trino_def",
        conn_type='trino',
        host="trino",
        login="gigachat_best_llm",
        password="",
        port=8080,
    )

    # Работаем с сессией базы данных Airflow
    session = settings.Session()

    existing_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if existing_conn is None:
        session.add(conn)
        session.commit()
        print(f"Connection trino_def created successfully.")
    else:
        print(f"Connection trino_def already exists.")

    session.close()

with DAG(
    dag_id="example_trino",
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    
    create_connection = PythonOperator(
        task_id="create_conn",
        python_callable=create_trino_conn
    )

    trino_create_schema = SQLExecuteQueryOperator(
        task_id="trino_create_schema",
        conn_id="trino_def",
        sql=f"SELECT 1;",
        handler=list,
    )
    # trino_create_table = SQLExecuteQueryOperator(
    #     task_id="trino_create_table",
    #     conn_id="trino_default",
    #     sql=f"""CREATE TABLE IF NOT EXISTS DDS.cities(
    #     cityid bigint,
    #     cityname varchar
    #     )""",
    #     handler=list,
    # )

    # trino_insert = SQLExecuteQueryOperator(
    #     task_id="trino_insert",
    #     conn_id="trino_default",
    #     sql=f"""INSERT INTO DDS.cities VALUES (1, 'San Francisco');""",
    #     handler=list,
    # )

    # trino_multiple_queries = SQLExecuteQueryOperator(
    #     task_id="trino_multiple_queries",
    #     conn_id="trino_default",
    #     sql=f"""CREATE TABLE IF NOT EXISTS DDS.cities1 (cityid bigint,cityname varchar);
    #     INSERT INTO DDS.cities1 VALUES (2, 'San Jose');
    #     CREATE TABLE IF NOT EXISTS DDS.cities2 (cityid bigint,cityname varchar);
    #     INSERT INTO DDS.cities2 VALUES (3, 'San Diego');""",
    #     handler=list,
    # )

    # trino_templated_query = SQLExecuteQueryOperator(
    #     task_id="trino_templated_query",
    #     conn_id="trino_default",
    #     sql="SELECT * FROM {{ params.SCHEMA }}.{{ params.TABLE }}",
    #     handler=list,
    #     params={'SCHEMA': DDS, 'TABLE': cities1},
    # )
    # trino_parameterized_query = SQLExecuteQueryOperator(
    #     task_id="trino_parameterized_query",
    #     conn_id="trino_default",
    #     sql=f"select * from DDS.cities2 where cityname = ?",
    #     parameters=("San Diego",),
    #     handler=list,
    # )

    (
        create_connection
        >> trino_create_schema
        # >> trino_create_table
        # >> trino_insert
        # >> trino_multiple_queries
        # >> trino_templated_query
        # >> trino_parameterized_query
    )