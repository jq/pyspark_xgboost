from pyspark import SQLContext
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType, LongType, DoubleType, StructType, StructField


@pandas_udf(StringType())
def to_upper(s):
    return s.str.upper()


# can't use "integer" or 'integer'
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def add_one(x):
    # TODO this is not print out..
    print("add_one")
    return x + 1


type = StructType([
    StructField("id", IntegerType(), True),
    StructField("v", DoubleType(), True),
])

@pandas_udf(type, PandasUDFType.GROUPED_MAP)
def normalize(pdf):
    v = pdf.v
    print("mean: ")
    print(v.mean())
    return pdf.assign(v=(v - v.mean()) / v.std())


def group_by(spark_context, spark_session):
    sql_sc = SQLContext(spark_context)
    spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

    df = sql_sc.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
                                ("id", "v"))
    # TODO count return Py4JJavaError: An error occurred while calling o71.count.
    df.groupby("id").apply(normalize).count()


# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf
def test_udf(spark_context, spark_session):
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
    # df.select(slen("name").alias("slen(name)")).show()
    # TODO this hit same error
    # df.select(to_upper("name")).show()

    print(df.select(slen("name").alias("slen(name)"),
              to_upper("name"), add_one("age")).count())
