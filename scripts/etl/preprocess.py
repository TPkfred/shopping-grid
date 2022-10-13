"""
**COME UP WITH A BETTER NAME FOR THIS FILE**

Does 

"""
import datetime
import argparse
from functools import reduce
import sys
import os
# import numpy as np
# import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.column import Column, _to_java_column, _to_seq


APP_NAME = "KF-ProcessAggdShopData"

# This is the current input directory.
# We load hard-coded older ones below.
input_dir = "/user/kendra.frederick/shop_vol/v8/raw"
output_dir = "/user/kendra.frederick/shop_vol/processed_agg_data"

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
parser.add_argument(
    "--trip-type",
    choices=("round-trip", "one-way"),
    help="Whether to process round-trips or one-ways",
    default="round-trip",
)
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
round_trip = args.trip_type == "round-trip"

min_stay_duration = args.min_stay_duration
# add one to allow for shifted feature
max_stay_duration = args.max_stay_duration + 1 
min_days_til_dept = args.min_days_til_dept
# add one to allow for shifted feature
max_days_til_dept = args.max_days_til_dept + 1

# shop_date_str = args.shop_date
# stale_cutoff = args.stale_after
shop_start_str = args.shop_start
shop_end_str = args.shop_end
shop_start_dt = datetime.datetime.strptime(shop_start_str, DATE_FORMAT)
shop_end_dt = datetime.datetime.strptime(shop_end_str, DATE_FORMAT)
num_shop_days = (shop_end_dt - shop_start_dt).days
shop_date_range = {shop_start_dt + datetime.timedelta(days=d) for d in range(num_shop_days)}
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


# SET UP SPARK
# -----------------------
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# # not sure we need these
# jvm = spark._jvm
# jsc = spark._jsc
# fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

# ========================
# HELPER FUNCTIONS
# ========================

# TODO: add in other arguments
def preprocess_data(df, round_trip=True):
    """Preprocesses data
    
    - Computes stay duration and days until departure
    - Filter data on:
        - stay duration between min/max
        - days_til_dept between min/max
        - round trip or one-way intineraries
    """
    # updated column names
    df = df.withColumn("market", 
        F.concat_ws("-", F.col("origin"), F.col("destination")))

    # convert dates (which are ints) to datetime
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(outDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(inDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(searchDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS searchDt_dt
        FROM data
    """)

    # **MOVE THIS ELSEWHERE IF WE NEED TO SUPPORT ONE-WAYS**
    # filter on round-trip
    df_filt = (df
                .filter(F.col("round_trip") == round_trip)
            )
    
    # Add days_til_dept column & filter on it
    df_filt = (df_filt.withColumn('days_til_dept',
                    F.datediff(
                        F.col('outDeptDt_dt'), F.col('searchDt_dt')))
                   .filter(F.col('days_til_dept')
                       .between(min_days_til_dept, max_days_til_dept))
                  )

    # **MOVE THIS ELSEWHERE IF WE NEED TO SUPPORT ONE-WAYS**
    if round_trip:
        # add stay_duration column & filter on it
        df_filt = (df_filt.withColumn('stay_duration', F.datediff(
                        F.col('inDeptDt_dt'), F.col('outDeptDt_dt')))
                # Note this effectively filters out null stay durations, 
                # which are one-way trips   
                    .filter(F.col("stay_duration").between(
                        min_stay_duration, max_stay_duration
                    )))
    
    # add shop indicator for individual market heatmaps
    df_filt = df_filt.withColumn("shop_ind", F.lit(1))
    
    return df_filt


def filter_pos(df, pos, currency):
    pos_df = df.filter(
        (F.col("pos") == pos) & (F.col("currency") == currency)
    )

    # filter on org or dest in country of POS
    pos_df = (pos_df
                    .join(
                        airport_df.select('airport_code', 'country_code'),
                        on=[pos_df['origin'] == airport_df['airport_code']],
                    ).withColumnRenamed("country_code", "origin_country")
                    .drop("airport_code")
                    )
    pos_df = (pos_df
                    .join(
                        airport_df.select('airport_code', 'country_code'),
                        on=[pos_df['destination'] == airport_df['airport_code']],
                    ).withColumnRenamed("country_code", "destination_country")
                    .drop("airport_code")
                    )

    pos_df = pos_df.filter(
        (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
    )
    return pos_df

# ========================
# LOAD DATA
# ========================

print("Loading data")

# lookup
airport_df = spark.read.csv("/user/contentoptimization/reference/content_manager/AIRPORT_FULL.CSV", header=True)


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
cov_df_list = []

# could also generate a set of date covered by this data source, and
# check the intersection with `shop_date_range`, but this works as well
if shop_start_dt <= datetime.date(2022,9,20):
    input_dir1 = "/user/kendra.frederick/shop_vol/v7/decoded/"
    cov_df1 = spark.read.parquet(input_dir1).filter(F.col("searchDt").isin(shop_int_range))

# v7 new format
## in this format, we only process aggregated data for a range of dates, one
## day at a time. The decoded data is saved to a corresponding folder 
## based on the date being processed. Ex:
## /user/kendra.frederick/shop_vol/v7/decoded_new_format/20220921
## Shopping data from 09/21/2022 - 09/29/2022 are saved in this format

# if (shop_start_dt >= datetime.date(2022,9,21)) | (shop_start_dt <= datetime.date(2022,9,29)):
v7_new_date_range = {datetime.date(2022,9,21) + datetime.timedelta(days=d) for d in range(9)}

if len(shop_date_range.intersection(v7_new_date_range)) > 0:
    input_dir2 = "/user/kendra.frederick/shop_vol/v7/decoded_new_format/*"
    cov_df2 = spark.read.parquet(input_dir2).filter(F.col("searchDt").isin(shop_str_range))
    cov_df2 = cov_df2.withColumn("searchDt", F.col("searchDt").cast(T.IntegerType()))
    cov_df_list.append(cov_df2)

if (cov_df1 is not None) & (cov_df2 is not None):
    cols_in_order = cov_df2.columns
    cov_df1 = cov_df1.select(cols_in_order)
    # cov_df = cov_df1.union(cov_df2)
    cov_df_list.append(cov_df1)


# current ("v8")
## Shopping data from 09/30/2022 onvwards are saved in this format / location.
## They have not been decoded.

if shop_end_dt >= datetime.date(2022,9,30):
    cov_df3 = spark.read.parquet(input_dir + "/*")
    cov_df3 = cov_df3.withColumn("searchDt", F.col("searchDt").cast(T.IntegerType()))

    # decode
    sc = spark.sparkContext
    numToStringUDF = sc._jvm.com.tvlp.cco.util.Utils.__getattr__('UDFs$').__getattr__('MODULE$').numToStringUDF
    
    cov_df3 = (cov_df3
        .withColumn("origin", 
            Column(numToStringUDF().apply(_to_seq(sc, ["outOriginAirport"], _to_java_column))))
        .withColumn("destination",
            Column(numToStringUDF().apply(_to_seq(sc, ["outDestinationAirport"], _to_java_column))))
        .withColumn("pos_decoded",
            Column(numToStringUDF().apply(_to_seq(sc, ["pos"], _to_java_column))))
        .withColumn("currency_decoded",
            Column(numToStringUDF().apply(_to_seq(sc, ["currency"], _to_java_column))))
    )

    cov_df3 = (cov_df3
        .drop("outOriginAirport", "outDestinationAirport", "pos", "currency")
        .withColumn("market", F.concat_ws("-", F.col("origin"), F.col("destination")))
        .withColumnRenamed("pos_decoded", "pos")
        .withColumnRenamed("currency_decoded", "currency")
        # reorder so it looks nice
        .select("market",  "origin", "destination", "round_trip",
            "pos", "currency", 
            "outDeptDt", "inDeptDt", "searchDt", 
            "shop_counts", "min_fare"
        )
    )
    # cov_df = cov_df.union(cov_df3)
    cov_df_list.append(cov_df3)

# combine
cov_df = reduce(lambda df1, df2: df1.union(df2), cov_df_list)


# ========================
# PROCESS DATA
# ========================

proc_df = preprocess_data(cov_df)
pos_df = filter_pos(proc_df, pos, currency)

print("Writing data")
trip_type = "rt" if round_trip else "ow"
output_path = "{}/{}-{}-{}".format(output_dir, pos, currency, trip_type)
if test:
    output_path += "-test"
pos_df.repartition(1).write.partitionBy("searchDt").mode("overwrite").parquet(output_path)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))