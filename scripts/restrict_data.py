import datetime

# import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# for notebook
# import matplotlib.pyplot as plt
# import matplotlib.cm as cm
# import seaborn as sns
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", 100)

"""
Restrict, or filter, data by various empirical considerations
in order to keep the accuracy of the estimations reasonable

"""
# TODO: define restriction thresholds in a config file & read in
APP_NAME = "KF-eval-grid-restrictions"
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# LOAD DATA
df = spark.read.parquet("/user/kendra.frederick/shop_vol/v7/US-pos_extra-days/")


# filter out anomalies
# ----------------
## An anomaly is defined as being higher thatn X % of 
## and has low shop counts for that day of shopping
w = Window.partitionBy("market")
df = df.withColumn(
    "median", F.expr("percentile_approx(min_fare, 0.5)").over(w)
)
df = df.withColumn(
    "ratio_min_fare_median", 
    F.col("min_fare") / F.col("median")
)
df_filt_anom = df.filter(F.col("ratio_min_fare_median") < 10)


# filter on markets that have all DOW combo's of travel
# ---------------
# Note: these operations can be done on either df or df_filt_anom (I think)

## add in DOW indicators
df = (df
      .withColumn("dept_dt_dow_int", (F.date_format("outDeptDt_dt", "u") - 1).cast(T.IntegerType()))
      .withColumn("dept_dt_dow", F.date_format("outDeptDt_dt", "E"))
      .withColumn("ret_dt_dow_int", (F.date_format("inDeptDt_dt", "u") - 1).cast(T.IntegerType()))
      .withColumn("ret_dt_dow", F.date_format("inDeptDt_dt", "E"))
     )

mrkt_dow_cnts = df.groupBy("market", "dept_dt_dow_int", "ret_dt_dow_int").count()
mrkt_dow_cnts2 = mrkt_dow_cnts.groupBy("market").count()

markets_all_dow = mrkt_dow_cnts2[mrkt_dow_cnts2['count'] == 49]
df_filt_dow = df.join(markets_all_dow.select("market"), on="market", how='inner')

# filter on RSD
# -------------------
## RSD = relative standard deviation
## here, calculated by market. So, if a market's std dev is > 150% 
## of its mean,  we exclude it

# Note: these calcs must be done on `df_filt_anom`
market_fare_stats = (df_filt_anom
                    .groupBy("market")
                     .agg(
                         F.mean("min_fare").alias("avg_min_fare"),
                         F.stddev("min_fare").alias("std_min_fare"),
                         F.min("min_fare").alias("min_min_fare"),
                         F.mean("min_fare").alias("max_min_fare"),
                     )
                .withColumn("rsd", F.col("std_min_fare")/F.col("avg_min_fare"))
                .dropna()
                    )


rsd_markets_to_keep = market_fare_stats.filter(F.col('rsd') < 1.5).select('market')
df_filt_rsd = df.join(rsd_markets_to_keep, on='market', how='inner')


# filter on "top" markets
# ----------------
# TODO: tune this value?
top_rank = 15000

market_cnts = (df
                .groupBy("market")
                .agg(F.sum('shop_counts').alias("sum_shop_counts"))
)

w_ord = Window.orderBy(F.desc("sum_shop_counts"))
market_cnts = market_cnts.withColumn("rank", F.row_number().over(w_ord))

markets_filt = market_cnts[market_cnts['rank'] <= top_rank]
df_filt_top = df.join(markets_filt.select("market", "rank"), on="market")

# combine "filters"
# --------------
df_final = 

# CALCULATE FEATURE
def calc_shifted_min_fare(df):
    w = (Window
        .partitionBy('market', 'outDeptDt', 'inDeptDt')
        .orderBy("searchDt")
        )

    df = (df
            .withColumn("min_fare_prev_shop_day", F.lag("min_fare").over(w))
            .withColumn("prev_shop_day", F.lag("searchDt_dt").over(w))
            .withColumn("prev_shop_date_diff", F.datediff(F.col("searchDt_dt"), F.col("prev_shop_day")))
        )
    return df

# ESTIMATE ERROR

def calc_error_stats(df, label):
    # TODO: 
        # convert this to use spark df
        # beef up stats (i.e. count nulls / data dropped)
        # return results as a dict
    """
    df: dataframe containing average error stats
    label (str): Figure title
    """
    pdf = pdf.dropna()
    data = pdf['mean_abs_pct_error']
#     print(data.describe)
    print("Median error:", np.median(data))
    print("Max error:", np.max(data))
    for err in [0.01, 0.05, 0.1, 1]:
        ptile = stats.percentileofscore(data, err)
        print(f"A MAPE of {err} is of percentile {ptile:.1f}%")  