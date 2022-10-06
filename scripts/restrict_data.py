"""
Restrict, or filter, data by various empirical considerations
in order to keep the accuracy of the estimations reasonable

"""


import datetime
import argparse
# import yaml
import os
import json

# import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

import numpy as np
import pandas as pd


pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", 100)

script_start_time = datetime.datetime.now()
print(script_start_time, " - Started script")

output_dir = "/home/kendra.frederick/shopping_grid/output/eval_features/restrictions"
# Python2 doesn't recognize `exist_ok` arg. Make folder manually. But beware
# if you change output_dir.
# os.makedirs(output_dir, exist_ok=True)

parser = argparse.ArgumentParser(description="Apply filters and "
    "restrictions to data, and evaluate feature(s). Note that the "
    "arguments below control whether a restriction/fitler is applied "
    "but their threshold values are defined in a config yaml file.")
parser.add_argument(
    "--run-mode",
    help="Run-mode",
    choices=("default", "ranges"),
    default="ranges"
)
parser.add_argument(
    "--anoms-high",
    help="Filter out high fare anomalies based on ratio vs median",
    action='store_true',
)
parser.add_argument(
    "--rsd",
    help="Add restriction that market have RSD below threshold",
    action='store_true',
)
parser.add_argument(
    "--dow",
    help="Add restriction that market have full DOW combo",
    action='store_true',
)
parser.add_argument(
    "--rank",
    help="Filter on market rank, based on volume",
    action='store_true',
)
parser.add_argument(
    "--all",
    help="Apply all functions.",
    action='store_true',
)
args = parser.parse_args()

# run_mode = args.run_mode
run_mode = "default"
anoms_high = args.anoms_high
rsd_filter = args.rsd
dow_filter = args.dow
rank_filter = args.rank
all_filters = args.all

# def load_config_file(self, filename):
#     with open(filename, "rb") as f:
#         config = yaml.safe_load(f)
#     return config


# configs = load_config_file("./restrict-configs.yaml")
# if run_mode == "ranges":
#     val_configs = configs["ranges"]
#     ratio_val = val_configs["ratio_min_fare_median"]
#     rsd_val = val_configs["rsd"]
#     rank_val = val_configs["rank"]
# elif run_mode == "default":
#     val_configs = configs["default"]
#     ratio_val = val_configs["ratio_min_fare_median"]
#     rsd_val = val_configs["rsd"]
#     rank_val = val_configs["rank"]

# configs = load_config_file("./restrict-configs.yaml")
if run_mode == "ranges":
    # ratio_val = 
    # rsd_val = 
    # rank_val = 
    pass
elif run_mode == "default":
    ratio_val = 5
    rsd_val = 1.5
    rank_val = 15000


APP_NAME = "KF-eval-grid-restrictions"
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# ===========================
# HELPER FUNCS
# ===========================

def write_text_file_json(dict_obj, filename):
    for k, v in dict_obj.items():
        if isinstance(v, np.float64):
            dict_obj[k] = float(v)
        elif isinstance(v, np.int64):
            dict_obj[k] = int(v)

    with open(filename, "w") as f:
        json.dump(dict_obj, f)

def add_dow_ind(df):
    ## add in DOW indicators
    df = (df
        .withColumn("dept_dt_dow_int", (F.date_format("outDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("dept_dt_dow", F.date_format("outDeptDt_dt", "E"))
        .withColumn("ret_dt_dow_int", (F.date_format("inDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("ret_dt_dow", F.date_format("inDeptDt_dt", "E"))
        )
    return df


def filter_anoms_high(df, threshold):
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
    df_filt_anom = df.filter(F.col("ratio_min_fare_median") < threshold)
    # filt_df_list.append(df_filt_anom)
    return df_filt_anom


def filter_rsd(df, threshold):
    # filter on RSD
    # -------------------
    ## RSD = relative standard deviation
    ## here, calculated by market. So, if a market's std dev is > 150% 
    ## of its mean,  we exclude it

    # Note: these calcs must be done on `df_filt_anom`
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


    rsd_markets_to_keep = market_fare_stats.filter(F.col('rsd') < threshold).select('market')
    df_filt_rsd = df.join(rsd_markets_to_keep, on='market', how='inner')
    # filt_df_list.append(df_filt_rsd)
    return df_filt_rsd

def filter_dow_combo(df, combo_count=49):
    # filter on markets that have all DOW combo's of travel
    # ---------------
    # Note: these operations can be done on either df or df_filt_anom (I think)

    mrkt_dow_cnts = df.groupBy("market", "dept_dt_dow_int", "ret_dt_dow_int").count()
    mrkt_dow_cnts2 = mrkt_dow_cnts.groupBy("market").count()

    markets_all_dow = mrkt_dow_cnts2[mrkt_dow_cnts2['count'] == combo_count]
    df_filt_dow = df.join(markets_all_dow.select("market"), on="market", how='inner')
    # filt_df_list.append(df_filt_dow)
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
    # filt_df_list.append(df_filt_top)
    return df_filt_top

# combine "filters"
# --------------

# df_final = reduce(lambda df1, df2: df1.join(df2, on="market", how="inner"))


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
    df: dataframe errors
    """
    err_stats = df.describe(['abs_pct_error']).collect()
    median_error = df.approxQuantile("abs_pct_error", [0.5], 0.01)[0]
    err_dict = {r['summary']: r['abs_pct_error'] for r in err_stats}
    err_dict['median'] = median_error
    return err_dict
    

# ===========================
# MAIN
# ===========================

# LOAD DATA
df = spark.read.parquet("/user/kendra.frederick/shop_vol/v7/US-pos_extra-days/")

params = {}

# if run_mode == "ranges":
#     # iterate over ranges of filter/threshold values
#     pass
# elif run_mode == "default":
    

if dow_filter or all_filters:
    df = add_dow_ind(df)
    df = filter_dow_combo(df)
    params['dow_filter'] = True

if anoms_high or all_filters:
    df = filter_anoms_high(df, ratio_val)
    params["ratio_min_fare_median"] = ratio_val

if rsd_filter or all_filters:
    df = filter_rsd(df, rsd_val)
    params["rsd"] = rsd_val

if rank_filter or all_filters:
    df = filter_rank(df, rank_val)
    params["rank"] = rank_val

df_with_feature = calc_shifted_min_fare(df)
df_with_error = calc_error(df_with_feature, "min_fare_last_shop_day")
err_dict = calc_error_stats(df_with_error)

params.update(err_dict)
write_text_file_json(params, os.path.join(output_dir, "test.json"))]

print("Final output:")
print(params)
print()

print("High-error data:")
df_with_error.orderBy("abs_pct_error", ascending=False).show()
print()

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))