
from pyspark.ml.feature import (
    Normalizer, StandardScaler, OneHotEncoderEstimator,
    VectorAssembler, VectorIndexer
)
from pyspark.ml.regression import (
    LinearRegression, DecisionTreeRegressor
)
from pyspark.ml.evaluation import RegressionEvaluator



train, test = df_no_nulls.randomSplit([0.8, 0.2], seed=19)


ohe = OneHotEncoderEstimator(
    inputCols=categorial_features,
    outputCols=['dept_dow_enc']
)
# ohe_model = ohe.fit(train)
# df_enc = ohe_model.transform(train)



def calc_abs_pct_err(df, pred_col="prediction"):
    df = (df
           .withColumn("pred_err", F.col("fare") - F.col(pred_col))
           .withColumn("abs_pred_err", F.abs(F.col("pred_err")))
           .withColumn("abs_pct_err", F.col("abs_pred_err") / F.col("fare"))
          )
    # show these as well?
    print(df.select(F.mean("abs_pct_err").alias("mape")).show())
    print(df.groupBy("market").agg(F.mean("abs_pct_err").alias("mape")).orderBy("mape").show())
    return df



df.groupBy("market").agg(F.mean("abs_pct_err").alias("mape")).orderBy("mape")

# FEATURE IMPORTANCE
best_pipeline = cv_model.bestModel
va = best_pipeline.stages[-2]
gb_model = best_pipeline.stages[-1]

feat_imps = list(zip(va.getInputCols(), gb_model.featureImportances))

feat_imps.sort(key=lambda x: x[1], reverse=True)
# for x in feat_imps:
#     print(x)

file_name = 'gb-feat-impt'
xs = range(len(feat_imps))
plt.bar(xs, [x[1] for x in feat_imps])
plt.xticks(xs, [x[0] for x in feat_imps], rotation=90)
plt.ylabel("feature importance")
plt.title("Gradient Boosted Regression")
plt.tight_layout()
plt.savefig(f"/tmp/{file_name}.png")