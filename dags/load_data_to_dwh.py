from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator



POSTGRES_CONN_ID = "postgres_dwh"

TARGET_SCHEMA = "test"



default_args = {"owner": "airflow", "retries": 1}

def load_table(target_table, csv_path):
    TARGET_TABLE = target_table
    CSV_PATH = csv_path


    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    exists = hook.get_first(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(TARGET_SCHEMA, TARGET_TABLE),
    )
    if not exists:
        raise RuntimeError(
            f"Target table does not exist: {TARGET_SCHEMA}.{TARGET_TABLE}"
        )

    hook.run(f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE};")

    conn = hook.get_conn()
    conn.autocommit = True

    copy_sql = f"""
        COPY {TARGET_SCHEMA}.{TARGET_TABLE}
        FROM STDIN
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"'
        )
    """

    with conn.cursor() as cur, open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)

    return {
        "table": f"{TARGET_SCHEMA}.{TARGET_TABLE}",
        "csv_path": CSV_PATH,
        "method": "COPY",
    }
def load_games_list():
    target_table = "games_list"
    csv_path = "/opt/airflow/dags/raw_data/steam/games.csv"
    load_table(target_table, csv_path)

def load_players():
    target_table = "players"
    csv_path = "/opt/airflow/dags/raw_data/steam/players.csv"
    load_table(target_table, csv_path)

def load_game_prices():
    target_table = "game_prices"
    csv_path = "/opt/airflow/dags/raw_data/steam/prices.csv"
    load_table(target_table, csv_path)

def load_purchased_games():
    target_table = "purchased_games"
    csv_path = "/opt/airflow/dags/raw_data/steam/purchased_games.csv"
    load_table(target_table, csv_path)

def load_games_description():
    target_table = "games_description"
    csv_path = "/opt/airflow/dags/raw_data/Steam Games/games_description.csv"
    load_table(target_table, csv_path)

def load_reviews_m():
    target_table = "reviews_m"
    csv_path = "/opt/airflow/dags/raw_data/Steam Games/steam_game_reviews.csv"
    load_table(target_table, csv_path)

def load_reviews_s():
    target_table = "reviews_s"
    csv_path = "/opt/airflow/dags/raw_data/steam/reviews.csv"
    load_table(target_table, csv_path)

def load_reviews_l():
    target_table = "reviews_l"
    csv_path = "/opt/airflow/dags/raw_data/steam_reviews.csv"
    load_table(target_table, csv_path)

with DAG(
    dag_id="load_data_to_oltp",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["csv", "postgres", "raw"],
) as dag:


    t1 = PythonOperator(task_id = 'load_games_list', python_callable=load_games_list)
    t2 = PythonOperator(task_id = 'load_players', python_callable=load_players)
    t3 = PythonOperator(task_id = 'load_game_prices', python_callable=load_game_prices)
    t4 = PythonOperator(task_id = 'load_purchased_games', python_callable=load_purchased_games)
    t5 = PythonOperator(task_id = 'load_games_description', python_callable=load_games_description)
    t6 = PythonOperator(task_id = 'load_reviews_m', python_callable=load_reviews_m)
    t7 = PythonOperator(task_id = 'load_reviews_s', python_callable=load_reviews_s)
    t8 = PythonOperator(task_id = 'load_reviews_l', python_callable=load_reviews_l)
    
    (t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8)
