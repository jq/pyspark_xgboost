from pyspark import SQLContext
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType

@pandas_udf(StringType())
def to_upper(s):
     return s.str.upper()

# can't use "integer"
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def add_one(x):
    print(x)
    return x + 1

# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf
def test_rdd(spark_context, spark_session):
    sql_sc = SQLContext(spark_context)
    spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

    df = sql_sc.createDataFrame([(1, "John Doe", 21)],
                                ("id", "name", "age"))
    # df.printSchema()
    #
    # df.show()

    slen = pandas_udf(lambda s: s.str.len(), IntegerType())
    # below is similar to @
    # upper = pandas_udf(to_upper, StringType())
    # addOne = pandas_udf(add_one, IntegerType(), PandasUDFType.SCALAR)
    # this works
    df.select("name").show()

    # this doesn't work, Caused by: java.io.EOFException
    # 	at java.io.DataInputStream.readInt(DataInputStream.java:392)
    # seems related to slen output int
    #df.select(slen("name").alias("slen(name)")).show()

    df.select(slen("name").alias("slen(name)"),
              to_upper("name"), add_one("age")).count()
