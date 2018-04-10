

# each executor runs one set of param to train.
def train_with_dataframe(df, params, rounds, workers, use_external_memory):
    assert rounds >= 0
    # assume spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")
    pdf = df.toPandas()

    return 0
