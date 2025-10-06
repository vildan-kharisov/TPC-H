import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.functions as sf

# Описание задачи
# Необходимо построить отчет по данным о заказах (orders),
# содержащий сводную информацию по заказам в разрезе месяца,
# страны клиента, оформляющего заказ, а также приоритета выполняемого заказа.
# Используйте сортировку по названию страны и приоритета заказа на возрастание.


ORDERS_PATH = f"s3a://de-raw/orders"
CUSTOMER_PATH = f"s3a://de-raw/customer"
NATION_PATH = f"s3a://de-raw/nation"

TARGET_PATH = f"s3a://de-project/v-harisov/orders_report/"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob1-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())

def main():
    spark = _spark_session()

    df_orders = spark.read.parquet("s3a://de-raw/orders")
    df_customer = spark.read.parquet("s3a://de-raw/customer")
    df_nation = spark.read.parquet("s3a://de-raw/nation")

    df = df_orders.join(other=df_customer, on=df_customer["C_CUSTKEY"] == df_orders["O_CUSTKEY"], how='left')
    df = df.join(other=df_nation, on=df_nation["N_NATIONKEY"] == df["C_NATIONKEY"], how='left')
    df = df \
        .withColumn('O_MONTH', F.substring(F.col('O_ORDERDATE'), 0, 7)) \
        .groupBy(F.col("O_MONTH"),
                 F.col("N_NAME"),
                 F.col("O_ORDERPRIORITY")) \
        .agg(F.count("O_ORDERKEY").alias("orders_count"),
             F.mean("O_TOTALPRICE").alias("avg_order_price"),
             F.sum("O_TOTALPRICE").alias("sum_order_price"),
             F.min("O_TOTALPRICE").alias("min_order_price"),
             F.max("O_TOTALPRICE").alias("max_order_price"),
             F.sum(F.when(F.col("O_ORDERSTATUS") == "F", 1).otherwise(0)).alias("f_order_status"),
             F.sum(F.when(F.col("O_ORDERSTATUS") == "O", 1).otherwise(0)).alias("o_order_status"),
             F.sum(F.when(F.col("O_ORDERSTATUS") == "P", 1).otherwise(0)).alias("p_order_status")) \
        .orderBy(F.col("N_NAME"),
                 F.col("O_ORDERPRIORITY"))
    df.coalesce(4).write.mode("overwrite").parquet("s3a://de-project/v-harisov/orders_report")
    spark.stop()
print('Success')


if __name__ == "__main__":
    main()
