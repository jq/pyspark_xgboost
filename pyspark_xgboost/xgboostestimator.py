from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.context import SparkContext


class XGBoostEstimator(JavaEstimator):

    def __init__(self, xgb_param_map={}):
        super(XGBoostEstimator, self).__init__()
        sc = SparkContext._active_spark_context
        scala_map = sc._jvm.PythonUtils.toScalaMap(xgb_param_map)
        self._java_obj = self._new_java_obj(
            "ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator", self.uid, scala_map)

    def _create_model(self, java_model):
        return JavaModel(java_model)