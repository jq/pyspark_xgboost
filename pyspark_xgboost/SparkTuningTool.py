from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col, avg
from pyspark.sql.types import IntegerType, FloatType


from collections import namedtuple
import sys
import math
from xgboostestimator import  XGBoostEstimator

SalesRecord = namedtuple("SalesRecord", ["storeId", "daysOfWeek", "date", "sales", "customers",
                                         "open", "promo", "stateHoliday", "schoolHoliday"])
TestRecord = namedtuple("TestRecord", ["Id","storeId", "daysOfWeek", "date",
                                         "open", "promo", "stateHoliday", "schoolHoliday"])

Store = namedtuple("Store", ["storeId", "storeType", "assortment", "competitionDistance",
                 "competitionOpenSinceMonth", "competitionOpenSinceYear", "promo2",
                 "promo2SinceWeek", "promo2SinceYear", "promoInterval"])

def parseTestFile(testPath):
    isHeader = True
    records = []
    f = open(testPath, "r")
    for line in f.readlines():
        if isHeader:
            isHeader = False
        else:
            idStr, storeIdStr, daysOfWeekStr, dateStr, openStr, promoStr,\
            stateHolidayStr, schoolHolidayStr = line.split(",")
            salesRecord = TestRecord(int(idStr),int(storeIdStr), int(daysOfWeekStr), dateStr, \
                                     0 if (len(openStr) == 0) else int(openStr), \
                                     0 if (len(promoStr) == 0) else int(promoStr), stateHolidayStr,\
                                     schoolHolidayStr)
            records.append(salesRecord)
    f.close()
    return records
def parseStoreFile(storeFilePath):
    isHeader = True
    storeInstances = []
    f = open(storeFilePath, "r")
    for line in f.readlines():
        if isHeader:
            isHeader = False
        else:
            try:
                strArray = line.split(",")
                if len(strArray) == 10:
                    storeIdStr, storeTypeStr, assortmentStr, competitionDistanceStr,competitionOpenSinceMonthStr,\
                    competitionOpenSinceYearStr, promo2Str,promo2SinceWeekStr, promo2SinceYearStr, promoIntervalStr = line.split(",")
                    store = Store(int(storeIdStr), storeTypeStr, assortmentStr,
                                  -1 if (competitionDistanceStr == "") else int(competitionDistanceStr),
                                  -1 if (competitionOpenSinceMonthStr == "") else int(competitionOpenSinceMonthStr),
                                  -1 if (competitionOpenSinceYearStr == "") else int(competitionOpenSinceYearStr),
                                  int(promo2Str),
                                  -1 if (promo2Str == "0") else int(promo2SinceWeekStr),
                                  -1 if (promo2Str == "0") else int(promo2SinceYearStr),
                                  promoIntervalStr.replace("\"", ""))
                    storeInstances.append(store)
                else:
                    storeIdStr, storeTypeStr, assortmentStr, competitionDistanceStr,\
                    competitionOpenSinceMonthStr, competitionOpenSinceYearStr, promo2Str,\
                    promo2SinceWeekStr, promo2SinceYearStr, firstMonth, secondMonth, thirdMonth,\
                    forthMonth = line.split(",")
                    store = Store(int(storeIdStr), storeTypeStr, assortmentStr,
                                  -1 if (competitionDistanceStr == "") else int(competitionDistanceStr),
                                  -1 if (competitionOpenSinceMonthStr == "") else int(competitionOpenSinceMonthStr),
                                  -1 if (competitionOpenSinceYearStr == "") else int(competitionOpenSinceYearStr),
                                  int(promo2Str),
                                  -1 if (promo2Str == "0") else int(promo2SinceWeekStr),
                                  -1 if (promo2Str == "0") else int(promo2SinceYearStr),
                                  firstMonth.replace("\"", "") + "," + secondMonth + "," + thirdMonth + "," + \
                                  forthMonth.replace("\"", ""))
                    storeInstances.append(store)

            except Exception as e:
                print(e)
                sys.exit(1)

    f.close()
    return storeInstances


def parseTrainingFile(trainingPath):
    isHeader = True
    records = []
    f = open(trainingPath, "r")
    for line in f.readlines():
        if isHeader:
            isHeader = False
        else:
            storeIdStr, daysOfWeekStr, dateStr, salesStr, customerStr, openStr, promoStr,\
            stateHolidayStr, schoolHolidayStr = line.split(",")
            salesRecord = SalesRecord(int(storeIdStr), int(daysOfWeekStr), dateStr,
                                      int(salesStr), int(customerStr), int(openStr), int(promoStr), stateHolidayStr,
                                      schoolHolidayStr)
            records.append(salesRecord)
    f.close()
    return records


def featureEngineering(ds):

    stateHolidayIndexer = StringIndexer()\
        .setInputCol("stateHoliday")\
        .setOutputCol("stateHolidayIndex")
    schoolHolidayIndexer = StringIndexer()\
        .setInputCol("schoolHoliday")\
        .setOutputCol("schoolHolidayIndex")
    storeTypeIndexer = StringIndexer()\
        .setInputCol("storeType")\
        .setOutputCol("storeTypeIndex")
    assortmentIndexer = StringIndexer()\
        .setInputCol("assortment")\
        .setOutputCol("assortmentIndex")
    promoInterval = StringIndexer()\
        .setInputCol("promoInterval")\
        .setOutputCol("promoIntervalIndex")
    filteredDS = ds.filter(ds.sales > 0).filter(ds.open > 0)

    # parse date
    dateUdf = udf(lambda dateStr: int(dateStr.split("-")[2]), IntegerType())
    dsWithDayCol = filteredDS.withColumn("day", dateUdf(col("date")))

    monthUdf = udf(lambda dateStr: int(dateStr.split("-")[1]), IntegerType())
    dsWithMonthCol = dsWithDayCol.withColumn("month", monthUdf(col("date")))

    yearUdf = udf(lambda dateStr: int(dateStr.split("-")[0]), IntegerType())
    dsWithYearCol = dsWithMonthCol.withColumn("year", yearUdf(col("date")))

    salesUdf = udf(lambda sales: math.log(sales), FloatType())
    dsWithLogSales = dsWithYearCol.withColumn("logSales", salesUdf(col("sales")))

    # fill with mean values
    meanCompetitionDistance = float(dsWithLogSales.select(avg("competitionDistance")).first()[0])
    print("===={}".format(meanCompetitionDistance))

    distanceUdf = udf(lambda distance: float(distance) if (distance > 0) else meanCompetitionDistance, FloatType())
    finalDS = dsWithLogSales.withColumn("transformedCompetitionDistance",
                                        distanceUdf(col("competitionDistance")))

    vectorAssembler = VectorAssembler()\
        .setInputCols(["storeId", "daysOfWeek", "promo", "competitionDistance", "promo2", "day",
        "month", "year", "transformedCompetitionDistance", "stateHolidayIndex",
        "schoolHolidayIndex", "storeTypeIndex", "assortmentIndex", "promoIntervalIndex"])\
        .setOutputCol("features")

    pipeline = Pipeline(stages=[stateHolidayIndexer, schoolHolidayIndexer,
                                storeTypeIndexer, assortmentIndexer,promoInterval, vectorAssembler])

    return pipeline.fit(finalDS)\
        .transform(finalDS).\
        drop("stateHoliday", "schoolHoliday", "storeType", "assortment", "promoInterval", "sales",
        "promo2SinceWeek", "customers", "promoInterval", "competitionOpenSinceYear",
        "competitionOpenSinceMonth", "promo2SinceYear", "competitionDistance", "date")


def crossValidation(xgboostParam, trainingData):
    xgbEstimator = XGBoostEstimator(xgboostParam)\
        .setFeaturesCol("features")\
        .setLabelCol("logSales")

    paramGrid = ParamGridBuilder() \
        .addGrid(xgbEstimator.num_round, [20, 50])\
        .addGrid(xgbEstimator.eta, [0.1, 0.4])\
        .build()

    tv = TrainValidationSplit()\
        .setEstimator(xgbEstimator)\
        .setEvaluator(RegressionEvaluator().setLabelCol("logSales"))\
        .setEstimatorParamMaps(paramGrid)\
        .setTrainRatio(0.8)
    return tv.fit(trainingData)

def main(trainingPath, storeFilePath):
    sparkSession = SparkSession.builder.appName("rosseman").getOrCreate()
    allSalesRecords = parseTrainingFile(trainingPath)
    salesRecordsDF = sparkSession.createDataFrame(allSalesRecords)

    allStores = parseStoreFile(storeFilePath)
    storesDS = sparkSession.createDataFrame(allStores)
    fullDataset = salesRecordsDF.join(storesDS, "storeId")
    featureEngineeredDF = featureEngineering(fullDataset)


    params = {}
    params["eta"] = 0.1
    params["max_depth"] = 6
    params["silent"] = 1
    params["ntreelimit"] = 1000
    params["objective"] = "reg:linear"
    params["subsample"] = 0.8
    params["num_round"] = 100

    bestModel = crossValidation(params, featureEngineeredDF)
    return bestModel

if __name__ == "__main__":
    bestModel = main(sys.argv[1], sys.argv[2])
