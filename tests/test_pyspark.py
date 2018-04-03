import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import rand
import pandas


def test_rdd(spark_context):
    test_rdd = spark_context.parallelize([1, 2, 3, 4])
    assert test_rdd.count() == 4

def test_spark_session_dataframe(spark_session):
    # test_df = spark_session.createDataFrame([[1,3],[2,4]], "a: int, b: int")
    from pyspark.sql.functions import rand
    df = spark_session.range(1 << 22).toDF("id").withColumn("x", rand())
    df.printSchema()
    pdf = df.toPandas()
    spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")
    pdf = df.toPandas()


def test_that_requires_sc(spark_context):
    import os
    cwd = os.getcwd()
    print(cwd)
    f = open('README')
    df = spark_context.textFile('README')

    assert df.count() == 57
