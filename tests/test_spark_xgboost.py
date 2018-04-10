# First XGBoost model for Pima Indians dataset
from numpy import loadtxt
from pyspark import SQLContext, SparkContext
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd

from pyspark_xgboost.xgboost import train_with_dataframe


# https://github.com/malexer/pytest-spark
def get_dataframe_from_csv(spark_context, csv, fields):
    pandas_df = pd.read_csv(csv,
                            names=fields)
    sql_sc = SQLContext(spark_context)
    df = sql_sc.createDataFrame(pandas_df)
    df.printSchema()
    # spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")
    return df

def test_para(spark_session):
    search = spark_session.sparkContext.parallelize(range(5))
    data = get_data(spark_session)
    r = search.map(lambda index: run(index))
    #search.map(xgboost(data))

def run(index):
    print('load ' + index)

def get_data(spark_session):
    df = get_dataframe_from_csv(spark_session,
                                  'tests/data/pima-indians-diabetes.data.csv',
                                  ['column {0}'.format(s) for s in range(0, 9)])
    spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")
    pdf = df.toPandas()

    return pdf.values

# test single node xgboost
def xgboost(dataset):
    # load data
    #dataset = loadtxt('tests/data/pima-indians-diabetes.data.csv', delimiter=",")
    print('load1')
    # split data into X and y
    X = dataset[:,0:8]
    # last one is label
    Y = dataset[:,8]
    # split data into train and test sets
    seed = 7
    test_size = 0.33
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=test_size, random_state=seed)
    print(len(X_train))
    # fit model no training data
    model = XGBClassifier()
    model.fit(X_train, y_train)
    # make predictions for test data
    y_pred = model.predict(X_test)
    predictions = [round(value) for value in y_pred]
    # evaluate predictions
    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))
