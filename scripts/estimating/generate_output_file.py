"""
Generates output file for upload to AWS, for use in Calendar
API. Saves as csv.

Applied restricts or filters to data based on various empirical
considerations in order to keep the accuracy of the estimations
reasonable.

Note that this script is designed to run on 
"""


import datetime
import argparse
import os
import sys
import json

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# import numpy as np
import pandas as pd

# project-specific configs & utils
from config import *


pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", 100)

script_start_time = datetime.datetime.now()
print("{} - Starting script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))


parser = argparse.ArgumentParser(description="Apply filters and "
    "restrictions to data, and evaluate feature(s). Note that the "
    "arguments below control whether a restriction/fitler is applied "
    "but their threshold values are defined in a config yaml file.")
parser.add_argument(
    "--run-mode",
    help="Run-mode",
    # TODO: flesh out manual option
    choices=("config", "manual"),
    default="config"
)
args = parser.parse_args()

run_mode = args.run_mode

if run_mode == "manual":
    print("Sorry, can't do that yet")
    sys.exit(0)


APP_NAME = "generate-calendar-predictions"
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# ===========================
# HELPER FUNCS
# ===========================

def add_dow_ind(df):
    ## add in DOW indicators
    df = (df
        .withColumn("dept_dt_dow_int", (F.date_format("outDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("dept_dt_dow", F.date_format("outDeptDt_dt", "E"))
        .withColumn("ret_dt_dow_int", (F.date_format("inDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("ret_dt_dow", F.date_format("inDeptDt_dt", "E"))
        )
    return df


def filter_anoms_high(df, ratio_threshold, shop_counts_threshold):
    # filter out anomalies
    # ----------------
    ## An anomaly is defined as being higher than X-fold of the median
    ## value for that market's min fares *and* having shop counts below
    ## a threshold. We exclude that datapoint from further calculations.

    w = Window.partitionBy("market")
    df = df.withColumn(
        "median", F.expr("percentile_approx(min_fare, 0.5)").over(w)
    )
    df = df.withColumn(
        "z_score", 
        F.col("min_fare") / F.col("median")
    )
    if shop_counts_threshold > 0:
        df_filt_anom = df.filter(
                ~((F.col("z_score") >= ratio_threshold)
                & (F.col("shop_counts") < shop_counts_threshold))
                )
    else:
         df_filt_anom = df.filter(
                (F.col("z_score") < ratio_threshold)
         )
                  
    return df_filt_anom



def filter_rsd(df, rsd_threshold):
    # filter on RSD
    # -------------------
    ## RSD = relative standard deviation
    ## here, calculated by market. So, if a market's std dev is > 150% 
    ## of its mean,  we exclude that market

    # Note: these calcs should be done after any anomaly filtering
    market_fare_stats = (df
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
    rsd_markets_to_keep = market_fare_stats.filter(F.col('rsd') < rsd_threshold).select('market')
    df_filt_rsd = df.join(rsd_markets_to_keep, on='market', how='inner')
    return df_filt_rsd


def filter_dow_combo(df, combo_count=49):
    # filter on markets that have all DOW combo's of travel
    # ---------------
    # Note: these operations can be done on either df or df_filt_anom (I think)

    mrkt_dow_cnts = df.groupBy("market", "dept_dt_dow_int", "ret_dt_dow_int").count()
    mrkt_dow_cnts2 = mrkt_dow_cnts.groupBy("market").count()

    markets_all_dow = mrkt_dow_cnts2[mrkt_dow_cnts2['count'] == combo_count]
    df_filt_dow = df.join(markets_all_dow.select("market"), on="market", how='inner')
    return df_filt_dow


def filter_rank(df, rank):
    # filter on "top" markets
    # ----------------
    top_rank = rank

    market_cnts = (df
                    .groupBy("market")
                    .agg(F.sum('shop_counts').alias("sum_shop_counts"))
    )

    w_ord = Window.orderBy(F.desc("sum_shop_counts"))
    market_cnts = market_cnts.withColumn("rank", F.row_number().over(w_ord))

    markets_filt = market_cnts[market_cnts['rank'] <= top_rank]
    df_filt_top = df.join(markets_filt.select("market", "rank"), on="market")
    return df_filt_top


def calc_shifted_min_fare(df):
    w = (Window
        .partitionBy('market', 'outDeptDt', 'inDeptDt')
        .orderBy("searchDt")
        )

    df = (df
            # TODO: make this column name a magic variable / constant
            .withColumn("min_fare_last_shop_day", F.lag("min_fare").over(w))
            .withColumn("prev_shop_day", F.lag("searchDt_dt").over(w))
            .withColumn("prev_shop_date_diff", F.datediff(F.col("searchDt_dt"), F.col("prev_shop_day")))
        )
    return df

def calc_error(df, col):
    # % error = actual - pred / actual
    df = (df.withColumn("error", F.col("min_fare") - F.col(col))
            .withColumn("abs_pct_error", F.abs(F.col("error")) / F.col("min_fare"))
         )
    return df

def calc_error_stats(df):
    """
    df: dataframe continaing errors
    """
    err_stats = df.describe(['abs_pct_error']).collect()
    median_error = df.approxQuantile("abs_pct_error", [0.5], 0.01)[0]
    err_dict = {r['summary']: r['abs_pct_error'] for r in err_stats}
    err_dict['median'] = median_error
    err_dict['num_markets'] = df.select("market").distinct().count()
    return err_dict
    

# ===========================
# MAIN
# ===========================

# LOAD DATA
print("Reading data")
df = spark.read.parquet(hdfs_input_dir)
df = add_dow_ind(df)
df.cache()

print("Applying filters & restrctions")
df_mod = df.select("*")
if dow_filter:
    df_mod = filter_dow_combo(df_mod)

if zscore_filter:
    df_mod = filter_anoms_high(
        df_mod, zscore_threshold, shop_counts_threshold
    )

if rsd_filter:
    df_mod = filter_rsd(df_mod, rsd_threshold)

if rank_filter:
    df_mod = filter_rank(df_mod, rank_threshold)

# select most recent
w = (Window
        .partitionBy('market', 'outDeptDt', 'inDeptDt')
        .orderBy(F.desc("searchDt"))
        )
df_with_recency = (df_mod
                  .withColumn("recency_rank", F.row_number().over(w))
                 )

df_most_recent = df_with_recency.filter(F.col("recency_rank") == 1)


# write to HDFS csv
print("Writing data to file")
(df_most_recent
 .select(cols_to_write)
 .coalesce(1)
 .write.mode("overwrite")
 .csv(hdfs_output_dir + "/US-USD", header=True)
)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time python script: {:.02f} minutes".format(elapsed_time))