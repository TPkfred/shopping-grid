"""
Aggregate eStreaming (air shopping) data for Lowest Fare application.

Estreaming data source = datalake format in AWS
(tvlp-ds-air-shopping-pn in ml-data-platform-pn)

Version notes:
poc (proof of concept, i.e. is accurate modeling feasible?)
- filter on:
    - POS = 'US' & currency = 'USD'
    - "top" markets
- do *not* filter on constricted searches
- add features (not aggregated on, but carried forward)
    - constricted search
    - carrier
    - gds, pcc
    - availability (boolean == 9 or not)
- TBD: don't agg on city? (can add in later)

orig
- filter out constricted searches
"""

import os
import datetime
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.window import Window

# ========================
# VARIABLE DEFINITIONS

APP_NAME = "KF-ShoppingGrid"
data_dir = "s3://tvlp-ds-air-shopping-pn/v1_5"
out_dir = "s3://tvlp-ds-users/kendra-frederick/shopping-grid/agg-raw-data_poc"

TOP_MARKETS = [
    'LHR-JFK',
    'LHR-EWR',
    'JFK-LHR',
    'EWR-LHR',
    'EWR-CDG',
    'LHR-LAX',
    'LAX-JFK',
    'LAX-EWR',
    'JFK-LAX',
    'SFO-LAX',
    'LAX-SFO',
    'LGA-MIA',
    'ATL-EWR',
    'OAK-LAS',
]
groupby_cols = [
    'out_origin_airport',
    'out_destination_airport',
    # we now filter on these
    # 'point_of_sale', 
    # 'currency',
    'out_departure_date',
    'in_departure_date',
    # not in source data; gets created/added below
]
info_cols_raw = [
    'pcc', 'gds',
    'point_of_sale', 'currency',
    'out_origin_city', 'out_destination_city',
    'out_num_stops', 'in_num_stops',
    # added this after running on 11/05/2022
    'constricted_search'
]
info_cols_derived = [
    'out_cxr', 'in_cxr',
    'round_trip',
    'out_avail_all_009', 'out_avail_any_000', 'out_avail_any_001', 'out_avail_any_002',
    'in_avail_all_009', 'in_avail_any_000', 'in_avail_any_001', 'in_avail_any_002',
]
all_cols_to_write = groupby_cols + info_cols_raw + info_cols_derived
cache_cols = info_cols_raw + [
    "id",
    'out_origin_airport','out_destination_airport',
    'in_origin_airport', 'in_destination_airport', 
    'out_departure_date', 'in_departure_date',
    'out_marketing_cxr', 'in_marketing_cxr',
    'response_PTC', 'fare_break_down_by_PTC',
    'out_remaining_seats', 'in_remaining_seats'
]

# ========================
# HELPER FUNCTIONS & CLAUSES

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--shop-start",
        help="Shopping start date, of format YYYY-MM-DD",
        type=str,
        required=True
    )
    parser.add_argument(
        "--shop-end",
        help="Shopping end date (inclusive), of format YYYY-MM-DD",
        type=str,
        # required=True
    )
    parser.add_argument(
        "--test", "-t",
        help="Run in test mode: only process one hour's worth of data",
        default=False,
        action="store_true"
    )
    args = parser.parse_args()
    return args


def add_market_col(df):
    df_mod = (df
        .withColumn("market",
        F.concat_ws("-",
            F.col("out_origin_airport"), 
            F.col("out_destination_airport"))
    ))
    return df_mod


def filter_top_markets(df):    
    df_filt = df.filter(F.col("market").isin(TOP_MARKETS))
    df_filt = df_filt.repartition(200)
    return df_filt


def filter_other(df):

    # for round-trip: outOrigin = inDest and outDest = inOrigin 
    cond_rt = (((F.col("out_origin_airport") == F.col("in_destination_airport")) &
            (F.col("out_destination_airport") == F.col("in_origin_airport"))))
    cond_ow = ((F.col("in_origin_airport").isNull()) & (F.col("in_destination_airport").isNull()))
    
    # filter on trip type (tt) and add round-trip indicator
    df_filt = df.filter(cond_rt | cond_ow)
    df_filt = df_filt.withColumn("round_trip", F.when(cond_rt, 1).otherwise(0))
    
    # filter on POS and currency
    df_filt = df_filt.filter(
        (F.col("point_of_sale") == 'US') & (F.col("currency") == 'USD')
    )

    # filter out constricted searches (new to datalake format)
    # df_filt = df_filt.filter(~F.col("constricted_search"))

    return df_filt


def explode_fare_filter_adt(df):
    # explode PTC and fare & match their position
        # note: we want *response* PTC and not *request* 
        # (they are not always equivalent)
    df_expl = (df
        .select("*",  F.posexplode("response_PTC").alias("ptc_pos", "PTC"))
        .select("*", F.posexplode("fare_break_down_by_PTC").alias("fare_pos", "fare_PTC"))
        .filter(F.col("ptc_pos") == F.col("fare_pos"))
        .drop("ptc_pos", "fare_pos")
        )
    # filter on ADT
    df_adt = df_expl.filter(F.col("PTC") == "ADT")
    return df_adt



def extract_features(df):
    # add in carrier
    df_mod = df.withColumn("out_cxr", F.col("out_marketing_cxr")[0])
    df_mod = df_mod.withColumn("in_cxr", F.col("in_marketing_cxr")[0])

    df_avail_feat = (df_mod
        .withColumn("out_avail_all_009", F.when(
            F.size(F.array_remove(F.col("out_remaining_seats"), "009")) == 0,
            True).otherwise(False))
        .withColumn("out_avail_any_000", F.array_contains(F.col("out_remaining_seats"), "000"))
        .withColumn("out_avail_any_001", F.array_contains(F.col("out_remaining_seats"), "001"))            
        .withColumn("out_avail_any_002", F.array_contains(F.col("out_remaining_seats"), "002"))
        .withColumn("in_avail_all_009", F.when(
            F.size(F.array_remove(F.col("in_remaining_seats"), "009")) == 0,
            True).otherwise(False))
        .withColumn("in_avail_any_000", F.array_contains(F.col("in_remaining_seats"), "000"))
        .withColumn("in_avail_any_001", F.array_contains(F.col("in_remaining_seats"), "001"))            
        .withColumn("in_avail_any_002", F.array_contains(F.col("in_remaining_seats"), "002"))
    )

    return df_avail_feat


def preprocess_data(df):
    """Combine all desired preprocessing into a single function call."""
    df_mod = add_market_col(df)
    # df gets repartitioned here:
    df_top = filter_top_markets(df_mod)
    df_top_filt = filter_other(df_top)
    df_top_mod = explode_fare_filter_adt(df_top_filt)
    df_extr = extract_features(df_top_mod)
    return df_extr


def data_agg(df_preproc, date_str):
    func_start = datetime.datetime.now()
    print("Starting data aggregation")
    
    w = Window.partitionBy(groupby_cols).orderBy("fare_PTC")
    df_mf_full = (df_preproc
        .withColumn("fare_rank", F.dense_rank().over(w))
        .withColumn("solution_counts", F.count("id").over(w))
        # not 100% accurate, but OK for this use case
        .withColumn("shop_counts", F.approx_count_distinct("id").over(w))
    )
    df_min_fares = df_mf_full.filter(F.col("fare_rank") <= 3)
    df_min_fare_final = df_min_fares.select(
        ['market'] + all_cols_to_write
        + ['fare_PTC', 'fare_rank', 'solution_counts', 'shop_counts']
    ).drop_duplicates()

    day_df = df_min_fare_final.withColumn("search_date", F.lit(date_str))
    # day_df.show(5)
    print(day_df.columns)

    save_path = os.path.join(out_dir, date_str)
    num_partitions = 1 if test else 5

    print("Writing data")
    day_df.coalesce(num_partitions).write.mode("overwrite").parquet(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with aggregation - Elasped time: {}".format(elapsed_time))
    print("")


def daily_analysis(spark, date, test):
    """Load data for `date`, perform analysis, and save results.
    
    params:
    -------
    date (datetime obj)

    returns:
    -------
    None
    """
    loop_start = datetime.datetime.now()
    date_str = date.strftime("%Y%m%d")
    
    print("{} - starting to process data for {}".format(
        loop_start.strftime("%Y-%m-%d %H:%M"), date_str))

    # define input path
    if test:
        print("Running in test mode -- only loading 1 hour of data")
        hdfs_path = "{}/year={}/month={}/day={}/hour=19/".format(data_dir, date.year, date.month, date_str)
    else:
        hdfs_path = "{}/year={}/month={}/day={}".format(data_dir, date.year, date.month, date_str)

    # load data
    try:
        df_raw = spark.read.parquet(hdfs_path)
        check_time = datetime.datetime.now()
        elapsed_time = (check_time - loop_start).total_seconds() / 60
        print("Done reading raw data - Elapsed time: {:.02f} minutes".format(elapsed_time))
        # df_cached = df_raw.select(cache_cols).cache()
    except:
        print("COULD NOT LOAD/FIND {}. skipping.".format(hdfs_path))
        return None

    df_raw_sel = df_raw.select(cache_cols)
    # process data
    df_preproc = preprocess_data(df_raw_sel)
    # df_cached.unpersist()
    df_pp_cached = df_preproc.cache()
    data_agg(df_pp_cached, date_str)

    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))
    print("***DONE PROCESSING DAY: {}***".format(date_str))
    print("")

# ========================
# MAIN


if __name__ == "__main__":
    args = parse_args()
    test = args.test

    shop_start_str = args.shop_start
    shop_end_str = args.shop_end

    start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

    script_start_time = datetime.datetime.now()
    print("*****************************")
    print("{} - Starting Data Aggregation Script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
    print("Processing shopping days {} to {} (inclusive)".format(shop_start_str, shop_end_str))
    # print("Analyzing these POS's: {}".format(pos_list_str))
    print("Saving coverage & fare analysis to: {}".format(out_dir))

    # LOOP OVER SHOPPING DAYS
    num_days = (end_dt - start_dt).days
    date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

    with SparkSession.builder.appName(APP_NAME).getOrCreate() as spark:
        for date in date_list:
            daily_analysis(spark, date, test)

    script_end_time = datetime.datetime.now()
    elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
    print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
    print("*****************************")
    print("")
