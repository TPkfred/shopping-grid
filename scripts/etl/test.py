

"""
Pre-processes aggregated eStreaming data in support of downstream
processing related to modeling, caching, and analysis.

- filters on POS and currency
    - O or D in POS country
- converts data types
- creates of 'days until departure' (dtd) and 'stay duration' 
    (or length of stay, LOS) features and filters on them


Notes / thoughts:
- In the future, we may want to *not* filter on dtd and LOS here, 
    but rather keep data less restricted
"""

import datetime
import argparse
from functools import reduce
import sys
import os
# import numpy as np
# import pandas as pd




APP_NAME = "KF-ProcessAggdShopData"

# This is the current input directory.
# We load hard-coded older ones below.
input_dir = "/user/kendra.frederick/shop_vol/v8/raw"
output_dir = "/user/kendra.frederick/shop_grid/"

DATE_FORMAT = "%Y-%m-%d"


# ========================
# PARSING ARGUMENTS
# ========================
parser = argparse.ArgumentParser()
parser.add_argument(
    "--shop-start",
    help="Date of shopping data to begin processing at. "
    "Format YYYY-MM-DD.",
    type=str,
    required=True
)
parser.add_argument(
    "--shop-end",
    help="Date of shopping data to end processing at (inclusive). "
    "Format YYYY-MM-DD.",
    type=str,
    required=True
)
# parser.add_argument(
#     "--trip-type",
#     choices=("round-trip", "one-way"),
#     help="Whether to process round-trips or one-ways",
#     default="round-trip",
# )
parser.add_argument(
    "--min-stay-duration",
    help="Minimum length of stay / stay duration",
    type=int,
    default=1,
)
parser.add_argument(
    "--max-stay-duration",
    help="Maximum length of stay (LOS)/ stay duration. Note "
    "this should take into account the grid size. e.g., if the "
    "grid is 7x7 (departure and return date each +/- 3 days) and the max "
    "LOS allowed (difference between departure date & return data) is 7 "
    "days, then the max stay duration = 7 + 3 + 3 = 13",
    default=13,
)
parser.add_argument(
    "--min-days-til-dept",
    help="Minimum days til departure",
    type=int,
    default=1,
)
parser.add_argument(
    "--max-days-til-dept",
    help="Maximum days til departure",
    type=int,
    default=60,
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
    "--test", "-t",
    help="Run in test mode on 'old' data only",
    default=False,
    action="store_true"
)

args = parser.parse_args()
run_mode = "spark-submit"
# trip_type = args.trip_type
# round_trip = args.trip_type == "round-trip"

min_stay_duration = args.min_stay_duration
# add one to allow for shifted feature
max_stay_duration = args.max_stay_duration + 1 
min_days_til_dept = args.min_days_til_dept
# add one to allow for shifted feature
max_days_til_dept = args.max_days_til_dept + 1


shop_start_str = args.shop_start
shop_end_str = args.shop_end
shop_start_dt = datetime.datetime.strptime(shop_start_str, DATE_FORMAT).date()
shop_end_dt = datetime.datetime.strptime(shop_end_str, DATE_FORMAT).date()
num_shop_days = (shop_end_dt - shop_start_dt).days
shop_date_range = {shop_start_dt + datetime.timedelta(days=d) for d in range(num_shop_days + 1)}
shop_str_range = [d.strftime("%Y%m%d") for d in shop_date_range]
shop_int_range = [int(s) for s in shop_str_range]

pos = args.pos
currency = args.currency

test = args.test

# ========================
# START UP
# ========================

script_start_time = datetime.datetime.now()
print("------------------")
print("{} - Starting script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
print("Processing shop data from {} to {} (inclusive)".format
    (shop_start_str, shop_end_str)
)
print("Saving processed data to: {}".format(output_dir))


# # SET UP SPARK
# # -----------------------
# spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# ========================
# HELPER FUNCTIONS
# ========================

# ========================
# LOAD DATA
# ========================

print("Loading data")


# AGGREGATED ESTREAMING DATA
# ----------------------------

# TODO: make date loading moar smarter?
# for now we just load in chunks by source & filter

# "v7" 
## thru 09/29/2022.
## has been decoded

# v7 original format
## in this format, all aggregated data was loaded at once, decoded,
## and partitioned by search date when saving. Ex:
## /user/kendra.frederick/shop_vol/v7/decoded/searchDt=20220901
## Shopping data from 08/30/2022 - 09/20/2022 are saved in this format
cov_df1 = None
cov_df2 = None
cov_df3 = None
cov_df_list = []

# could also generate a set of date covered by this data source, and
# check the intersection with `shop_date_range`, but this works as well
if shop_start_dt <= datetime.date(2022,9,20):
    print("loading df1")
    input_dir1 = "/user/kendra.frederick/shop_vol/v7/decoded/"
    # cov_df1 = spark.read.parquet(input_dir1).filter(F.col("searchDt").isin(shop_int_range))
    cov_df1 = "x"

# v7 new format
## in this format, we only process aggregated data for a range of dates, one
## day at a time. The decoded data is saved to a corresponding folder 
## based on the date being processed. Ex:
## /user/kendra.frederick/shop_vol/v7/decoded_new_format/20220921
## Shopping data from 09/21/2022 - 09/29/2022 are saved in this format

# if (shop_start_dt >= datetime.date(2022,9,21)) | (shop_start_dt <= datetime.date(2022,9,29)):
v7_new_date_range = {datetime.date(2022,9,21) + datetime.timedelta(days=d) for d in range(9)}

if len(shop_date_range.intersection(v7_new_date_range)) > 0:
    print("loading df2")
    input_dir2 = "/user/kendra.frederick/shop_vol/v7/decoded_new_format/*"
    # cov_df2 = spark.read.parquet(input_dir2).filter(F.col("searchDt").isin(shop_str_range))
    # cov_df2 = cov_df2.withColumn("searchDt", F.col("searchDt").cast(T.IntegerType()))
    cov_df2 = "y"
    cov_df_list.append(cov_df2)


# current ("v8")
## Shopping data from 09/30/2022 onvwards are saved in this format / location.
## They have not been decoded.

if shop_end_dt >= datetime.date(2022,9,30):
    print("loading df3")
    cov_df3 = "z"
    cov_df_list.append(cov_df3)

# column order must be the same in order to `union` df's
if (cov_df1 is not None) & ((cov_df2 is not None) | (cov_df3 is not None)):
    if cov_df2 is not None:
        # cols_in_order = cov_df2.columns
        print("using df2's cols")
    elif cov_df3 is not None:
        # cols_in_order = cov_df3.columns
        print("using df3's cols")
    # cov_df1 = cov_df1.select(cols_in_order)
    cov_df_list.append(cov_df1)
elif cov_df1 is not None:
    cov_df_list.append(cov_df1)

# combine
# cov_df = reduce(lambda df1, df2: df1.union(df2), cov_df_list)
print(cov_df_list)

