import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.functions as sf

# Необходимо построить отчет по данным о грузоперевозках (part), содержащий сводную информацию в разрезе страны поставки (N_NAME),
# типа поставки (P_TYPE) и типа контейнера (P_CONTAINER).
# Используйте сортировку по названию страны (N_NAME) , типа поставки (P_TYPE) и типа контейнера (P_CONTAINER) на возрастание.


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

    df_part = spark.read.parquet("s3a://de-raw/part")
    df_partsupp = spark.read.parquet("s3a://de-raw/partsupp")
    df_supplier = spark.read.parquet("s3a://de-raw/supplier")
    df_nation = spark.read.parquet("s3a://de-raw/nation")

    df = df_partsupp.join(other=df_part, on=df_part["P_PARTKEY"] == df_partsupp["PS_PARTKEY"], how='left')
    df = df.join(other=df_supplier, on=df_supplier["S_SUPPKEY"] == df["PS_SUPPKEY"], how='left')
    df = df.join(other=df_nation, on=df_nation["N_NATIONKEY"] == df["S_NATIONKEY"], how='left')
    df = df \
        .groupBy(F.col("N_NAME"),
                 F.col("P_TYPE"),
                 F.col("P_CONTAINER")) \
        .agg(F.countDistinct("P_PARTKEY").alias("parts_count"),
             F.mean("P_RETAILPRICE").alias("avg_retailprice"),
             F.sum("P_SIZE").alias("size"),
             F.percentile_approx("P_RETAILPRICE", 0.5).alias("mean_retailprice"),
             F.min("P_RETAILPRICE").alias("min_retailprice"),
             F.max("P_RETAILPRICE").alias("max_retailprice"),
             F.mean("PS_SUPPLYCOST").alias("avg_supplycost"),
             F.percentile_approx("PS_SUPPLYCOST", 0.5).alias("mean_supplycost"),
             F.min("PS_SUPPLYCOST").alias("min_supplycost"),
             F.max("PS_SUPPLYCOST").alias("max_supplycost")) \
        .orderBy(F.col("N_NAME"),
                 F.col("P_TYPE"),
                 F.col("P_CONTAINER"))

    df.coalesce(4).write.mode("overwrite").parquet("s3a://de-project/v-harisov/parts_report")
    spark.stop()

    print('Success')


if __name__ == "__main__":
    main()
