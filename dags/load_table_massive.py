# load_table.py
import sys
from pyspark.sql import SparkSession

jdbc_url, user, pwd, catalog, namespace, src_table, dst_table, *rest = sys.argv[1:]



spark = SparkSession.builder.appName(f"load_{dst_table}").getOrCreate()


reader = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", user)
    .option("password", pwd)
    .option("dbtable", src_table)
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "10000")
)


df = reader.load()

(df.writeTo(f"{catalog}.{namespace}.{dst_table}")
   .createOrReplace())

spark.stop()
