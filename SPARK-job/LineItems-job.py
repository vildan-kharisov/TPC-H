import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.functions as sf

# Необходимо построить отчет по данным о позициях в заказе (lineitems),
# содержащий сводную информацию по позициям каждого заказа, когда-либо совершенного в системе,
# группировать данные необходимо по идентификатору заказа.
# Используйте сортировку по ключу заказа на возрастание.

# В отчете должны присутствовать следующие колонки:
# L_ORDERKEY
# count(L_PARTKEY) count
# sum(L_EXTENDEDPRICE) as sum_extendprice
# median(L_DISCOUNT) as mean_discount
# mean(L_TAX) as mean_tax
# (L_SHIPDATE - L_RECEIPTDATE )as delivery_days
# количество заказов с флагом L_RETURNFLAG == 'A' as A_return_flags
# aколичество заказов с флагом L_RETURNFLAG == 'R' as R_return_flags
# количество заказов с флагом L_RETURNFLAG == 'N' as N_return_flags


DATA_PATH = f"s3a://de-raw/lineitem"
TARGET_PATH = f"s3a://de-project/v-harisov/lineitems_report"

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

    df_lineitem = spark.read.parquet("s3a://de-raw/lineitem")

    df = df_lineitem \
        .groupBy(F.col("L_ORDERKEY")) \
        .agg(F.count("L_ORDERKEY").alias("count"),
             F.sum("L_EXTENDEDPRICE").alias("sum_extendprice"),
             F.percentile_approx('L_DISCOUNT', 0.5).alias("mean_discount"),
             F.mean('L_TAX').alias("mean_tax"),
             F.avg(F.datediff(F.col('L_RECEIPTDATE'), F.col('L_SHIPDATE'))).alias("delivery_days"),
             F.sum(F.when(F.col('L_RETURNFLAG') == 'A', 1).otherwise(0)).alias("A_return_flags"),
             F.sum(F.when(F.col('L_RETURNFLAG') == 'R', 1).otherwise(0)).alias("R_return_flags"),
             F.sum(F.when(F.col('L_RETURNFLAG') == 'N', 1).otherwise(0)).alias("N_return_flags")) \
        .select(F.col("L_ORDERKEY"),
                F.col("count"),
                F.col("sum_extendprice"),
                F.col("mean_discount"),
                F.col("mean_tax"),
                F.col("delivery_days"),
                F.col("A_return_flags"),
                F.col("R_return_flags"),
                F.col("N_return_flags")) \
        .orderBy(F.col("L_ORDERKEY"))

    df.coalesce(4).write.mode("overwrite").parquet("s3a://de-project/v-harisov/lineitems_report")
    spark.stop()

    print('Success')


if __name__ == "__main__":
    main()
