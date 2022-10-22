"""
Aggregate eStreaming (air shopping) data for Lowest Fare application.

Estreaming data source = datalake format in AWS
(tvlp-ds-air-shopping-pn in ml-data-platform-pn)

"""

import os
import datetime
import argparse
# from math import ceil

# import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 



# ========================
# VARIABLE DEFINITIONS
# these will likely because arguments some day

APP_NAME = "KF-ShoppingGrid"
# data_dir = "/data/estreaming/datalake_1_5/"

# TODO: derive full path from date supplied
# for now, hard-code
# data_dir = "s3://tvlp-ds-air-shopping-pn/v1_5/year=2022/month=10/day=20221019/hour=19/"
data_dir = "s3://tvlp-ds-air-shopping-pn/v1_5"


# grid_out_dir = "/user/kendra.frederick/shop_vol/v5_datalake/"
out_dir = "s3://kendra-frederick/shopping-grid/agg-raw-data"

# determined off-line / previously
# opt_num_grid_parts = 3

pos_list_str = ["US", "IN"]


# ========================
# SETUP SPARK

# # so we can run using spark-submit or python 
# if run_mode == "python":
#     conf = pyspark.SparkConf().setAll(
#         [('spark.master','yarn'),
#         ('spark.app.name', APP_NAME),
#         ('spark.driver.memory','30g'),
#         ('spark.executor.memory', '20g'),
#         ('spark.executor.instances', 10),
#         ('spark.executor.cores', '5'),

#         ])
#     spark = SparkSession.builder.config(conf=conf).getOrCreate()
# elif run_mode == "spark-submit":
#     spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
# else:
#     pass

# ========================
# ADDITIONAL VARIABLE DEFINTIONS

# spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

groupby_cols = [
    'out_origin_airport',
    'out_destination_airport',
    'out_origin_city',
    'out_destination_city',
    # NOTE: due to conditions imposed, we don't need in_ fields
    'point_of_sale', 
    'currency',
    'out_departure_date',
    'in_departure_date',
    'round_trip', # not in source data; these get created/added below
]




# ========================
# HELPER FUNCTIONS & CLAUSES

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--shop-start",
        help="Shopping start date, of format YYYY-MM-DD",
        type=str,
        # required=True
    )
    parser.add_argument(
        "--shop-end",
        help="Shopping end date (inclusive), of format YYYY-MM-DD",
        type=str,
        # required=True
    )
    # parser.add_argument(
    #     "--include-pcc", "-pcc",
    #     help="Whether to include PCC in the groupby",
    #     default=False,
    #     action="store_true"
    # )
    args = parser.parse_args()
    return args


# # filters 
# # for round-trip: outOrigin = inDest and outDest = inOrigin 
# cond_rt = (((F.col("out_origin_airport") == F.col("in_destination_airport")) &
#          (F.col("out_destination_airport") == F.col("in_origin_airport"))))
# cond_ow = ((F.col("in_origin_airport").isNull()) & (F.col("in_destination_airport").isNull()))


def data_preprocessing(df_raw):

    # filters 
    # for round-trip: outOrigin = inDest and outDest = inOrigin 
    cond_rt = (((F.col("out_origin_airport") == F.col("in_destination_airport")) &
            (F.col("out_destination_airport") == F.col("in_origin_airport"))))
    cond_ow = ((F.col("in_origin_airport").isNull()) & (F.col("in_destination_airport").isNull()))
    
    # filter on trip type (tt) and add round-trip indicator
    df_filt = df_raw.filter(cond_rt | cond_ow)
    df_filt = df_filt.withColumn("round_trip", 
        F.when(cond_rt, 1).otherwise(0))
    
    # filter on POS
        # move this here to restrict data size before we explode
    df_filt = df_filt.filter(F.col("point_of_sale").isin(pos_list_str))

    # filter out constricted searches (new to datalake format)
    df_filt = df_filt.filter(~F.col("constricted_search"))

    # added 10/12/2022, after switching to processing midt_1_5_pn data
    df_filt = df_filt.coalesce(200)

    # explode PTC and fare & match their position
        # note: we want *response* PTC and not *request* 
        # (they are not always equivalent)
    df_expl = (df_filt
        .select("*",  F.posexplode("response_PTC").alias("ptc_pos", "PTC"))
        .select("*", F.posexplode("fare_break_down_by_PTC").alias("fare_pos", "fare_PTC"))
        .filter(F.col("ptc_pos") == F.col("fare_pos"))
        .drop("ptc_pos", "fare_pos")
        )
    # filter on ADT
    df_adt = df_expl.filter(F.col("PTC") == "ADT")
    return df_adt


def data_agg(df_preproc, date_str):
    func_start = datetime.datetime.now()
    print("Starting data aggregation")
    
    # add a market key to aid in joining
    df_preproc = df_preproc.withColumn(
        "market_key",
        F.concat_ws("-",
            F.col("out_origin_airport"), 
            F.col("out_destination_airport"))
    )

    df_agg = (df_preproc
        .groupBy(groupby_cols)
        .agg(
            F.countDistinct("id").alias("shop_counts"),
            F.min("fare_PTC").alias("min_fare")
        )
    )
    
    day_df = df_agg.withColumn("search_date", F.lit(date_str))
    day_df.show(5)
    # if include_pcc:
    #     # num_partitions = int(ceil(opt_num_grid_parts * 1.5))
    #     # save_path = grid_out_dir + "with_pcc/" + date_str
    #     num_partitions = 6
    #     save_path = os.path.join(out_dir, "with-pcc", date_str)
    # else:
        # num_partitions = opt_num_grid_parts
        # save_path = grid_out_dir + date_str
    num_partitions = 5
    save_path = os.path.join(out_dir, date_str)

    print("Writing data")
    day_df.repartition(num_partitions).write.mode("overwrite").parquet(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with aggregation - Elasped time: {}".format(elapsed_time))
    print("")


# def top_market_analysis(df_raw, date_str):
#     # Note: we use "raw" data, without any filters. We just want to get a 
#     # crude idea of what's being shopped for. This approach includes 
#     # multi-city  trips, where out origin != in destination.
#     func_start = datetime.datetime.now()
#     print("Starting top market counts")
#     save_path = top_markets_out_dir + date_str
    
#     df_agg = (df_raw
#               .groupBy("out_origin_airport", "out_destination_airport")
#               .agg(
#                 F.count("id").alias("solution_counts"),
#                 F.countDistinct("id").alias("num_unique_shops")
#               )
#               .orderBy(F.desc("num_unique_shops"))
#               ).coalesce(1)
#     day_df = df_agg.withColumn("date_str", F.lit(date_str))
#     day_df.show(5)
#     # ignore or overwite here? Could also add logic above to check if .csv
#     # already exists and skip processing if it doesn.
#     day_df.write.mode("overwrite").option("header", True).csv(save_path)
    
#     func_end = datetime.datetime.now()
#     elapsed_time = (func_end - func_start).total_seconds() / 60
#     print("Done with top market counts. Elasped time: {}".format(elapsed_time))
#     print("")


def daily_analysis(spark, date):
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
    
    # hdfs_path = "hdfs://" + data_dir + date_str + "/" + "*"
    # TODO: remove hour from path
    hdfs_path = "{}/year={}/month={}/day={}/hour=19/".format(data_dir, date.year, date.month, date_str)

    # setting so we don't error out on corrupt files
    # spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

    print("{} - starting to process data for {}".format(
        loop_start.strftime("%Y-%m-%d %H:%M"), date_str))
    try:
        df_raw = spark.read.parquet(hdfs_path)
        check_time = datetime.datetime.now()
        elapsed_time = (check_time - loop_start).total_seconds() / 60
        print("Done reading raw data - Elapsed time: {:.02f} minutes".format(elapsed_time))
    except:
        print("COULD NOT LOAD/FIND {}. skipping.".format(hdfs_path))
        return None

    df_preproc = data_preprocessing(df_raw)
    data_agg(df_preproc, date_str)

    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))
    print("***DONE PROCESSING DAY: {}***".format(date_str))
    print("")

# ========================
# MAIN


if __name__ == "__main__":
    args = parse_args()

    shop_start_str = args.shop_start
    shop_end_str = args.shop_end

    start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

    # if include_pcc:
    #     groupby_cols += ['pcc', 'gds']

    script_start_time = datetime.datetime.now()
    print("*****************************")
    print("{} - Starting Data Aggregation Script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
    print("Processing shopping days {} to {} (inclusive)".format(shop_start_str, shop_end_str))
    print("Analyzing these POS's: {}".format(pos_list_str))
    print("Saving coverage & fare analysis to: {}".format(out_dir))

    # LOOP OVER SHOPPING DAYS
    num_days = (end_dt - start_dt).days
    date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

    with SparkSession.builder.appName(APP_NAME).getOrCreate() as spark:
        for date in date_list:
            daily_analysis(spark, date)

    script_end_time = datetime.datetime.now()
    elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
    print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
    print("*****************************")
    print("")
