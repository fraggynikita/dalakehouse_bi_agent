import sys
from pyspark.sql import SparkSession

jdbc_url, user, password, catalog, namespace, source_table, target = sys.argv[1:]

spark = (
    SparkSession.builder.appName(f"load_{target}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_table)
    .option("user", user)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")  # <-- ВОТ ЭТО
    .load()
)


# удаляем существующую таблицу, если нужна полная перезапись
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{namespace}.{target}")

df.writeTo(f"{catalog}.{namespace}.{target}").createOrReplace()  # либо .overwrite()

spark.stop()
