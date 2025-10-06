import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.functions as sf

# Необходимо построить отчет по данным о клиентах (customers),
# содержащий сводную информацию по заказам в разрезе страны, откуда был отправлен заказ, а также приоритета выполняемого заказа.
# Используйте сортировку по названию страны (N_NAME) и приоритета заказа (C_MKTSEGMENT) на возрастание.


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

    df_customer = spark.read.parquet("s3a://de-raw/customer")
    df_nation = spark.read.parquet("s3a://de-raw/nation")
    df_region = spark.read.parquet("s3a://de-raw/region")

    df = df_customer.join(other=df_nation, on=df_nation["N_NATIONKEY"] == df_customer["C_NATIONKEY"], how='left')
    df = df.join(other=df_region, on=df_region["R_REGIONKEY"] == df["N_REGIONKEY"], how='left')
    df = df \
        .groupBy(F.col("R_NAME"),
                 F.col("N_NAME"),
                 F.col("C_MKTSEGMENT")) \
        .agg(F.countDistinct("C_CUSTKEY").alias("unique_customers_count"),
             F.mean("C_ACCTBAL").alias("avg_acctbal"),
             F.percentile_approx('C_ACCTBAL', 0.5).alias("mean_acctbal"),
             F.min("C_ACCTBAL").alias("min_acctbal"),
             F.max("C_ACCTBAL").alias("max_acctbal"), ) \
        .orderBy(F.col("N_NAME"),
                 F.col("C_MKTSEGMENT"))

    df.coalesce(4).write.mode("overwrite").parquet("s3a://de-project/v-harisov/customers_report")
    spark.stop()
print('Success')


if __name__ == "__main__":
    main()
