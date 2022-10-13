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

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# import numpy as np

cols_to_write = [
 'pos',
 'currency',
 'origin_city',
 'destination_city',
 'outDeptDt',
 'inDeptDt',
 'min_fare'
]
output_dir = "/user/kendra.frederick/tmp/calendar_data"


script_start_time = datetime.datetime.now()
print("{} - Starting script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))


parser = argparse.ArgumentParser(
    description="Generate lookup / 'prediction' file. Optionally applies "
    " filters restrictions to data."
)
parser.add_argument(
    "--pos",
    help="Point of Sale two-letter code",
    default="US",
)
parser.add_argument(
    "--currency",
    help="Currency (3-letter code)",
    default="USD",
)
parser.add_argument(
    "--stale-after",
    help="Exclude data older than this many days old. "
    "If this param is set to 0, restriction will not be applied. Default = 0",
    type=int,
    default=0,
)
parser.add_argument(
    "--prev-val-ratio",
    help="Ratio of current value to previous. If data is > than this "
    "filter it will be excluded. If this param is set to 0, filter "
    "will not be applied. Default = 0",
    type=int,
    default=0,
)
parser.add_argument(
    "--ratio-median",
    help="Ratio of current value to median for market. If data is > than "
    "this value, it will be excluded. If this param is set to 0, filter "
    "will not be applied. Default = 0",
    type=int,
    default=0,
)
parser.add_argument(
    "--ratio-mean",
    help="Ratio of current value to mean for market. If data is > than  "
    "this value, it will be excluded. If this param is set to 0, filter "
    "will not be applied. Default = 0",
    type=int,
    default=0,
)
parser.add_argument(
    "--shop-counts",
    help="Minimum threshold applied to shop counts in conjuction with mean "
    "and/or median ratios. If this param is set to 0, "
    "filter will not be applied. Default = 0",
    type=int,
    default=0,
)
parser.add_argument(
    "--dow-filter",
    help="Wheter to apply day of week (DOW) combination requirement to "
    "a market. If market has not been shopped for all combinations of "
    "DOW of travel in data, it will be excluded.",
    action="store_true",
    default=False,
)
parser.add_argument(
    "--rsd",
    help="Relative standard deviation (RSD) reqirement by market. "
    "If market's RSD data is > than this value, it will be excluded. "
    "If this param is set to 0, filter will not be applied. Default = 0",
    type=int,
    default=0,
)
parser.add_argument(
    "--rank",
    help="Filter out market outside the top rank by volume. "
    "If this param is set to 0, filter will not be applied. Default = 0",
    type=int,
    default=0,
)

# parser.add_argument(
#     "--input-dir", "-i",
#     help="Directory (in HDFS) containing input data",
#     default="/user/kendra.frederick/shop_grid"
# )
args = parser.parse_args()

prev_val_ratio = args.prev_val_ratio
ratio_median = args.ratio_median
ratio_mean = args.ratio_mean
shop_counts_threshold = args.shop_counts
dow_filter = args.dow_filter
rsd = args.rsd
rank = args.rank
stale_after = args.stale_after

pos = args.pos
currency = args.currency

base_input_dir = "/user/kendra.frederick/shop_grid"
input_dir = "{}/{}-{}".format(base_input_dir, pos, currency)

no_filters = (
    (prev_val_ratio == 0)
    & (ratio_median == 0)
    & (ratio_mean == 0)
    & (dow_filter is False)
    & (rsd == 0)
    & (rank == 0)
)

# derive shopping date cutoff
# assumes input data is current through today / yesterday
shop_date_cutoff = datetime.date.today() - datetime.timedelta(days=stale_after)
shop_date_cutoff_int = int(shop_date_cutoff.strftime("%Y%m%d"))


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


def filter_prev_val_ratio(df, prev_val_ratio):
    """Filter out data based on its ratio to the previous value."""
    pass


def filter_ratio_median(df, ratio_median, shop_counts_threshold=0):
    """Filter out data based on ratio to its markets median lowest fare.

    An anomaly is defined as being higher than X-fold of the median
    value for that market's min fares *and* having shop counts below
    a threshold. We exclude that datapoint from further calculations.
    """
    w = Window.partitionBy("market")
    df = df.withColumn(
        "median", F.expr("percentile_approx(min_fare, 0.5)").over(w)
    )
    df = df.withColumn(
        "ratio_median", 
        F.col("min_fare") / F.col("median")
    )
    if shop_counts_threshold > 0:
        df_filt_anom = df.filter(
                ~((F.col("ratio_median") >= ratio_median)
                & (F.col("shop_counts") < shop_counts_threshold))
                )
    else:
         df_filt_anom = df.filter(
                (F.col("ratio_median") < ratio_median)
         )
                  
    return df_filt_anom


def filter_ratio_mean(df, ratio_mean):
    """Filter out data based on its ratio to its market's mean lowest fare.

    Only filters out high-fare anomalies.
    """
    w = Window.partitionBy("market")
    df = df.withColumn(
        "mean", F.mean("min_fare").over(w)
    )
    df = df.withColumn(
        "ratio_mean", 
        F.col("min_fare") / F.col("mean")
    )
    if shop_counts_threshold > 0:
        df_filt_anom = df.filter(
                ~((F.col("ratio_mean") >= ratio_mean)
                & (F.col("shop_counts") < shop_counts_threshold))
                )
    else:
         df_filt_anom = df.filter(
                (F.col("ratio_mean") < ratio_mean)
         )
    return df_filt_anom


def filter_rsd(df, rsd_threshold):
    """Filter out market based on its RSD.

    RSD = relative standard deviation = stddev / mean

    Note: these calcs should typically be done after any anomaly filtering
    """
    market_fare_stats = (df
                        .groupBy("market")
                        .agg(
                            F.mean("min_fare").alias("avg_min_fare"),
                            F.stddev("min_fare").alias("std_min_fare"),
                            # F.min("min_fare").alias("min_min_fare"),
                            # F.mean("min_fare").alias("max_min_fare"),
                        )
                    .withColumn("rsd", F.col("std_min_fare")/F.col("avg_min_fare"))
                    .dropna()
                        )
    rsd_markets_to_keep = market_fare_stats.filter(F.col('rsd') < rsd_threshold).select('market')
    df_filt_rsd = df.join(rsd_markets_to_keep, on='market', how='inner')
    return df_filt_rsd


def filter_dow_combo(df, combo_count=49):
    """Filter out markets that do not have all DOW combo's of travel

    DOW = days of week. All combinations refers to departure date and return
    dates of travel.
    """
    mrkt_dow_cnts = df.groupBy("market", "dept_dt_dow_int", "ret_dt_dow_int").count()
    mrkt_dow_cnts2 = mrkt_dow_cnts.groupBy("market").count()

    markets_all_dow = mrkt_dow_cnts2[mrkt_dow_cnts2['count'] == combo_count]
    df_filt_dow = df.join(markets_all_dow.select("market"), on="market", how='inner')
    return df_filt_dow


def filter_rank(df, rank):
    """Filter markets based on volume rank."""
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
    
# ===========================
# MAIN
# ===========================

# LOAD DATA
print("Reading data")
df = spark.read.parquet(input_dir)

if stale_after > 0:
    df = df.filter(F.col("searchDt") >= shop_date_cutoff_int)

# modify "market"
    # prior: defined by airport
    # now: defined by city

df = df.withColumn("market", 
        F.concat_ws("-", F.col("origin_city"), F.col("destination_city"))
)
df.cache()

print("Applying filters & restrctions")
df_mod = df.select("*")

if dow_filter:
    df = add_dow_ind(df)
    df_mod = filter_dow_combo(df_mod)

if ratio_mean != 0:
    df_mod = filter_ratio_mean(
        df_mod, ratio_mean, shop_counts_threshold
    )

if ratio_median != 0:
    df_mod = filter_ratio_median(
        df_mod, ratio_median, shop_counts_threshold
    )

if rsd != 0:
    df_mod = filter_rsd(df_mod, rsd)

if rank != 0:
    df_mod = filter_rank(df_mod, rank)


# select most recent
w = (Window
        .partitionBy('market', 'outDeptDt', 'inDeptDt')
        .orderBy(F.desc("searchDt"))
        )
df_with_recency = (df_mod
                  .withColumn("recency_rank", F.row_number().over(w))
                 )

df_most_recent = df_with_recency.filter(F.col("recency_rank") == 1)

df_most_recent.select(cols_to_write).show(5)
print(df_most_recent.count())

num_part = 10 if no_filters else 5
# num_part = 5 # good enough for both cases

# write to HDFS csv
print("Writing data to file")
(df_most_recent
 .select(cols_to_write)
 .coalesce(num_part) # haven't seen a need to convert this to repartition yet...
 .write.mode("overwrite")
 .csv(output_dir + "/US-USD", header=True)
)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time python script: {:.02f} minutes".format(elapsed_time))