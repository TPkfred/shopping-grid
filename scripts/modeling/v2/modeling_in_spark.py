
import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

from pyspark.ml.feature import (
    MinMaxScaler, StandardScaler, OneHotEncoderEstimator,
    VectorAssembler, VectorIndexer
)
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline


# =========================
# DEFINE FEATURES
# =========================

target_col = 'fare'

# scale: mean = 0, std = 1
features_to_scale = [
    # fare-based features
    'fare_prev_shop_day',
    'fr2_fare_prev_shop_day', 
    'avg_fare_dtd',
    'fare_prev_dept_day', 'fare_next_dept_day',
    'est_fare_from_prev_dept_day', 'est_fare_from_next_dept_day',
    'trailing_avg_fare', 'trailing_std_fare',
                    ]
# norm: [0,1]
features_to_norm = [
    'days_til_dept', # encoded in 'avg_fare_dtd', so exclude?
    'num_itin_prev_shop_day',
    # count-based features - expect some skew in these, so norm instead of scale (?)
    'shop_counts_prev_shop_day', 
    'trailing_avg_shop_counts',
    'solution_counts_prev_shop_day',
    'trailing_avg_solution_counts',
]
categorial_features = [
    'dept_dt_dow_int' 
]
as_is_features = [
    # avail-based features are already norm'd
    'avg_out_avail_max_prev_shop_day',
    'avg_out_avail_low_prev_shop_day',
    # boolean feature(s) - new in version d
    'fr1_fr2_out_cxrs_same',
    'fr1_fr2_out_cxrs_overlap',
    'is_holiday'
]
all_features = features_to_scale + features_to_norm + categorial_features + as_is_features
all_cols_for_modeling = all_features + [target_col]

# =========================
# HELPER FUNCTIONS
# =========================

def calc_abs_pct_err(df, pred_col="prediction"):
    df = (df
           .withColumn("pred_err", F.col("fare") - F.col(pred_col))
           .withColumn("abs_pred_err", F.abs(F.col("pred_err")))
           .withColumn("abs_pct_err", F.col("abs_pred_err") / F.col("fare"))
          )
    # show these as well?
    print(df.select(F.mean("abs_pct_err").alias("mape")).show())
    # print(df.groupBy("market").agg(F.mean("abs_pct_err").alias("mape")).orderBy("mape").show())
    return df




def get_feature_importances(pipeline_model, is_cross_val_model, print=True):
    """
    pipeline_model: fitted pipeline. Assumes last stage is an ML model
        that has `.featureImportances` attribue, and penultimate stage 
        is a VectorAssembler whose `inputCols` are feature names
    is_cross_val_model (bool): whether `pipeline_model` is a 
        CrossValidatorModel
    print (bool): whether to print out the sorted list of feature
        importances
    """
    # FEATURE IMPORTANCE
    if is_cross_val_model:
        best_pipeline = pipeline_model.bestModel
        # va = vector assembler; assumed to be stage just before ML algo,
        # which has feature names
        va = best_pipeline.stages[-2]
        algo = best_pipeline.stages[-1]
    else:
        va = pipeline_model.stages[-2]
        algo = pipeline_model.stages[-1]

    feat_imps = list(zip(va.getInputCols(), algo.featureImportances))


    feat_imps.sort(key=lambda x: x[1], reverse=True)
    if print:
        for x in feat_imps:
            print(x)
    return feat_imps

def plot_feature_impts(feat_imps, file_name, plot_title):
    """
    feat_imps: list of tuples of (feature name, feature importance)
    """
    xs = range(len(feat_imps))
    plt.figure(figsize=(8,6))
    plt.bar(xs, [x[1] for x in feat_imps])
    plt.xticks(xs, [x[0] for x in feat_imps], rotation=90)
    plt.ylabel("feature importance")
    plt.title(plot_title)
    plt.tight_layout()
    plt.savefig(f"/tmp/{file_name}.png")


# =========================
# DATA PREP
# =========================

# clip off first 2 shop days, 
    # we take a 3-day trailing average, so first 2 days won't be same
first_shop_date = datetime.date(2022,10,15)
window_size = 3
shop_cutoff = first_shop_date + datetime.timedelta(days=window_size)
df_clip_shop = df.filter(F.col('shopDate_dt') >= shop_cutoff)

# drop null for now
df_no_nulls = df_clip_shop.dropna(subset=all_cols_for_modeling)

# train / test split
train, test = df_no_nulls.randomSplit([0.8, 0.2], seed=19)

# opt: remove holiday data
df_no_holidays = df_no_nulls.filter(F.col("is_holiday") == 0)
train_no_hol, test_no_hol = df_no_holidays.randomSplit([0.8, 0.2], seed=19)


# DEFINE PIPELINE

ohe = OneHotEncoderEstimator(
    inputCols=categorial_features,
    outputCols=['dept_dow_enc']
)

scale_assembler = VectorAssembler(
    inputCols=features_to_scale,
    outputCol="features_to_scale"
)

scaler = StandardScaler(
    inputCol=scale_assembler.getOutputCol(),
    outputCol="scaled_features"
)

norm_assembler = VectorAssembler(
    inputCols=features_to_norm,
    outputCol="features_to_norm"
)

normer = MinMaxScaler(
    inputCol=norm_assembler.getOutputCol(),
    outputCol="normed_features"
)

lr_assembler = VectorAssembler(
    inputCols=[*ohe.getOutputCols(), 
               scaler.getOutputCol(),
               normer.getOutputCol()
              ] + as_is_features,
    outputCol='features'
)

raw_feature_assembler = VectorAssembler(
    inputCols=all_features,
    outputCol='features'
)

# LINEAR REGRESSION
lr_regression_model = LinearRegression(
    #  elasticNetParam corresponds to α (ratio) and regParam corresponds to λ (weight).
    labelCol="fare",
    maxIter=20,
    # from tuning
    regParam=1.0,
    elasticNetParam=0.01
)

lr_pipeline = Pipeline(stages=[
    scale_assembler,
    norm_assembler,
    ohe, 
    scaler,
    normer,
    lr_assembler,
    lr_regression_model
])

lin_model = lr_pipeline.fit(train_shuffled)
test_tx = lin_model.transform(test)


# RANDOM FOREST

# params from tuning
rf_regression_model = RandomForestRegressor(
    labelCol="fare",
    numTrees=100,
    maxDepth=7,
)

rf_pipeline = Pipeline(stages=[
    raw_feature_assembler,
    rf_regression_model
])


# GRADIENT BOOSTING
# params fro tuning
gb_regression_model = GBTRegressor(
    labelCol="fare",
    maxIter=50,
    maxDepth=5,
    lossType='squared',
    stepSize=0.1,
)

gb_pipeline = Pipeline(stages=[
    raw_feature_assembler,
    gb_regression_model
])


df.groupBy("market").agg(F.mean("abs_pct_err").alias("mape")).orderBy("mape")
