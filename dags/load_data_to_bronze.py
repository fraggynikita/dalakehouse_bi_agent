from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.standard.operators.bash import BashOperator

SPARK_CONN_ID = "spark_default"   
MASTER_URL = "spark://spark-iceberg:7077"

JDBC_URL = "jdbc:postgresql://postgres-dwh:5432/dwh"
JDBC_USER = "dwh"
JDBC_PASSWORD = "dwh"

ICEBERG_CATALOG = "iceberg"
ICEBERG_NAMESPACE = "datalake"

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.0",
    "org.postgresql:postgresql:42.7.2",
])

SPARK_CONF = {
    "spark.master": MASTER_URL,

    "spark.driver.userClassPathFirst": "true",
    "spark.executor.userClassPathFirst": "true",
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.executor.cores": "1",

    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{ICEBERG_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.type": "rest",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.uri": "http://rest:8181",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse": "s3://datalake/",

    f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint": "http://minio:9000",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access": "true",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.access-key-id": "minioadmin",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.secret-access-key": "minioadmin",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.region": "us-east-1",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.ssl.enabled": "false",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.force-path-style": "true",

    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",

    "spark.driver.extraJavaOptions": "-Daws.region=us-east-1 -Daws.defaultRegion=us-east-1",
    "spark.executor.extraJavaOptions": "-Daws.region=us-east-1 -Daws.defaultRegion=us-east-1",
}


default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="load_data_to_bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    export_games_list_from_pg = SparkSubmitOperator(
        task_id="export_games_list_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.games_list",  
            "games_list"            
        ],
    )

    export_players_from_pg = SparkSubmitOperator(
        task_id="export_players_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.players",
            "players"
        ],
    )

    export_game_prices_from_pg = SparkSubmitOperator(
        task_id="export_game_prices_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.game_prices",
            "game_prices"
        ],
    )

    export_purchased_games_from_pg = SparkSubmitOperator(
        task_id="export_purchased_games_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.purchased_games",
            "purchased_games"
        ],
    )

    export_reviews_s_from_pg = SparkSubmitOperator(
        task_id="export_reviews_s_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.reviews_s",
            "reviews_s"
        ],
    )

    export_games_description_from_pg = SparkSubmitOperator(
        task_id="export_games_description_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.games_description",
            "games_description"
        ],
    )

    export_reviews_m_from_pg = SparkSubmitOperator(
        task_id="export_reviews_m_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.reviews_m",
            "reviews_m"
        ],
    )

    export_reviews_l_from_pg = SparkSubmitOperator(
        task_id="export_reviews_l_from_pg",
        application="/opt/airflow/dags/load_table_massive.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        packages=PACKAGES,
        application_args=[
            JDBC_URL, JDBC_USER, JDBC_PASSWORD,
            ICEBERG_CATALOG, ICEBERG_NAMESPACE,
            "dwh.test.reviews_l",
            "reviews_l",
        ],
    )

    # dqc = BashOperator(task_id= "dqc",bash_command="soda scan -d trino -c opt/airflow/soda/configuration.yml opt/airflow/soda/checks.yml")

    export_games_list_from_pg >> export_players_from_pg >> export_game_prices_from_pg >> export_games_description_from_pg >> export_purchased_games_from_pg >> export_reviews_s_from_pg >> export_reviews_m_from_pg >> export_reviews_l_from_pg 

