# load_table.py
import sys
from pyspark.sql import SparkSession

jdbc_url, user, pwd, catalog, namespace, src_table, dst_table, *rest = sys.argv[1:]



spark = SparkSession.builder.appName(f"load_{dst_table}").getOrCreate()

# # ✅ ВАЖНО: делаем числовую колонку row_id_num через CAST
# dbtable = f"""
# (
#   SELECT
#     t.*,
#     (t.row_id)::bigint AS row_id_num
#   FROM {src_table} t
#   WHERE t.row_id ~ '^\\d+$'
# ) AS q
# """

reader = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", user)
    .option("password", pwd)
    .option("dbtable", src_table)
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "10000")
)

# ожидаем: lowerBound upperBound numPartitions
# if len(rest) >= 3:
#     lower, upper, num_partitions = rest[0], rest[1], rest[2]
#     reader = (reader
#         .option("partitionColumn", "row_id_num")
#         .option("lowerBound", lower)
#         .option("upperBound", upper)
#         .option("numPartitions", num_partitions)
#     )

df = reader.load()

# ✅ чтобы запись тоже была параллельной, а не "в 1 партицию"
# df = df.repartition(128)

# (опционально) можно выкинуть служебную колонку, если не нужна в Iceberg
# df = df.drop("row_id_num")

(df.writeTo(f"{catalog}.{namespace}.{dst_table}")
   .createOrReplace())

spark.stop()
