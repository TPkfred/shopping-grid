# Updated 8/25 for "next steps"

import sys
from functools import reduce
import datetime
from calendar import Calendar
import argparse
from re import search

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


"""
A note on dates, datetime objects, and dates as strings
- outDeptDt, inDeptDt: strings, from estreaming data
    - add conversion to datetime objects (outDeptDt, inDeptDt) so 
        it"s easier to do "math" on them
    - use string format for inDeptDt for final pivot table
        - when dates become columns names, they don"t display
            in human-readable format
- searchDt: string, added during data prep / agg (see other script)
    - must convert to datetime, then numeric (unix timestamp)
        - latter is required by Window function
"""

# =============================
# SETUP
# =============================

# ----------------
# VARIABLES & ARGUMENTS
# ----------------
input_dir = "/user/kendra.frederick/shop_vol/markets/v2_encoded"
output_dir = "/user/kendra.frederick/shop_grid/results/v2"
APP_NAME = "KF-ShopGrid"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run-mode",
    help="Run-mode",
    choices=("spark-submit", "python", "local"),
    required=True
)
parser.add_argument(
    "--month", "-m",
    help="Travel month as integer (e.g. Jan=1, Feb=2, etc)",
    type=int,
    required=True
)
parser.add_argument(
    "--year", "-y",
    help="Travel year (defaults to current year)",
    type=int,
    default=datetime.date.today().year
)
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
    required=True
)
parser.add_argument(
    "--num-top-markets", "-n",
    help="Number of top markets to include in analysis",
    type=int,
    default=10
)
parser.add_argument(
    "--process-mode",
    help="Whether to reprocess raw data or load from file",
    choices=("reprocess", "load", "process-only"),
    default="reprocess"
)
args = parser.parse_args()

month = args.month
year = args.year
shop_start_str = args.shop_start
shop_end_str = args.shop_end
top_n = args.num_top_markets
process_mode = args.process_mode

# ----------------
# SET UP SPARK
# ----------------

run_mode = args.run_mode

if run_mode == "python":
    conf1 = pyspark.SparkConf().setAll(
        [("spark.master","yarn"),
        ("spark.app.name", APP_NAME),
        ("spark.driver.memory","20g"),
        ("spark.executor.memory", "20g"),
        ("spark.executor.instances", 10),
        ("spark.executor.cores", "10"),
        ('spark.sql.crossJoin.enabled', True)
        ])
    spark = SparkSession.builder.config(conf = conf1).getOrCreate()
elif run_mode == "spark-submit":
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
else:
    pass


# =============================
# HELPER FUNCTIONS
# =============================




# =============================
# DATES
# =============================
# TODO: move these to arguments
num_travel_days = 120
max_stay_duration = 30

# SEARCH DATES
search_start = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
search_end = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")
num_search_days = (search_end - search_start).days + 1

travel_start = search_end + datetime.timedelta(days=1)
travel_end = travel_start + datetime.timedelta(days=num_travel_days)

travel_start_str = travel_start.strftime("%Y-%m-%d")
travel_end_str = travel_end.strftime("%Y-%m-%d")

last_return = travel_end + datetime.timedelta(days=max_stay_duration)

travel_dates_list = [travel_start + datetime.timedelta(days=x) for x in range(num_travel_days)]
# travel_date_str_list = [x.strftime("%Y%m%d") for x in travel_dates_list]

travel_date_data = [(x, ) for x in travel_dates_list]
travel_date_df = spark.createDataFrame(
    data=travel_date_data, schema=["outDeptDt_dt"])

stay_duration_data = [(x, ) for x in range(max_stay_duration)]
stay_dur_df = spark.createDataFrame(
    data=stay_duration_data, schema=["stay_duration"])

travel_date_cross_df = (travel_date_df.crossJoin(stay_dur_df))

# =============================
# LOAD & FILTER DATA
# =============================

# ---------------------
# MARKETS TO ANALYZE
# ---------------------
markets_path = "/user/kendra.frederick/shop_grid/markets.csv"
markets_df = spark.read.csv(markets_path, header=True)
print("Analyzing {} markets".format(markets_df.count()))

# column name is wrong
markets_df = markets_df.withColumnRenamed("market_key_encoded", "market_key_decoded")

temp = markets_df.select("market_key_decoded").collect()
market_list = [x["market_key_decoded"] for x in temp]

# ---------------------
# INPUT DATA
# ---------------------
processed_data_dir =  "/user/kendra.frederick/shop_grid/tmp/"
processed_data_filename = "_".join([
    "search-cnts_", "travel", str(month).zfill(2) + '-' + str(year), 
    "shop", shop_start_str.strip("-"), shop_end_str.strip("-")
    ])
processed_data_path = processed_data_dir + processed_data_filename

if process_mode in ["reprocess", "process-only"]:

    print("Loading & processing raw data")
    df = spark.read.parquet(input_dir)
    df = df.withColumn("market_key_decoded", 
        F.concat_ws("-", F.col("origin_decoded"), F.col("dest_decoded")))
    
    df_join = df.join(F.broadcast(markets_df), on="market_key_decoded", how="inner")

    # time zone conversion
    df_join = (df_join
                .withColumn("outDeptDtFmt", 
                            F.regexp_replace(F.col("outDeptDt").cast(T.StringType()), 
                                "(\\d{4})(\\d{2})(\\d{2})",
                                "$1-$2-$3"))
                .withColumn("outDeptTimeFmt", 
                            F.regexp_replace(F.col("outDeptTime").cast(T.StringType()), 
                                "(\\d{1,2})(\\d{2})",
                                "$1:$2"))
                .withColumn("outDeptDtTimeFmt", 
                            F.concat_ws(" ", 
                            F.col("outDeptDtFmt"), F.col("outDeptTimeFmt")))
    )

    # convert Dept dates (which are strings) to datetime
    df_join.registerTempTable("data")
    df_join = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(outDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(inDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(searchDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS searchDt_dt,
            CAST(outDeptDtTimeFmt as TIMESTAMP) AS outDept_dt4
        FROM data
    """)


    # filter on dates & round-trip
    df_filt = (df_join
                .filter(F.col("outDeptDt_dt").between(travel_start, travel_end))
                .filter(F.col("searchDt_dt") >= search_start)
                .filter(F.col("searchDt_dt") <= search_end)
                # Note: when we filter on stay duration below, this also
                # effectively accomplishes filtering on round-trip == 1
                .filter(F.col("round_trip") == 1)
            )



    # group by market and count shops
    cnt_df = (df_filt
                .groupBy(["market_key_decoded", 
                            "searchDt_dt", "outDeptDt_dt", "inDeptDt_dt", "round_trip"
                            # these aren't used any longer
                            # "outDeptDt", "inDeptDt", "searchDt"
                            ])
                .agg(
                    F.sum("solution_counts").alias("sum_solution_counts"),
                    F.sum("shop_counts").alias("sum_shop_counts")
                )
                .withColumn("shop_ind", F.lit(1)) # add an indicator column
                )
    
    cnt_df = cnt_df.withColumn('stay_duration',
                    F.datediff(
                        F.col('inDeptDt_dt'), F.col('outDeptDt_dt'))
                    )
    cnt_df.coalesce(1)
    cnt_df.write.mode("overwrite").parquet(processed_data_path)

    if process_mode == "process-only":
        print("Done processing data. Exiting")
        sys.exit(0)

elif process_mode == "load":
    print("Loading previously-prepared data")
    cnt_df = spark.read.parquet(processed_data_path)

cnt_df.cache()
cnt_df.show(5)
print(cnt_df.rdd.getNumPartitions())
print("")


# =============================
# MARKET ANALYSIS
# =============================

# Filter on stay duration
cnt_df = cnt_df.filter(F.col('stay_duration').between(0, max_stay_duration))

for market in market_list:
    print("Processing market {}...".format(market))
    file_name = (market + "_shop_" + shop_start_str + "_" + shop_end_str 
                     + "_travel_" + str(num_travel_days) + "-days")
    market_df = cnt_df.filter(F.col("market_key_decoded") == market)
    market_df.cache()

    # --------------------------
    # SUMMARY SHOPPING DAYS 
    # --------------------------
    
    # note we use the non-joined market df
    # so we don't collect search date if it's not present
    market_agg_df = (market_df
        .groupby("outDeptDt_dt", "stay_duration")
        .agg(
            F.sum("shop_ind").alias("num_shop_days"),
            # note: we are taking max of a string; confirmed that this works in Python
            F.max("searchDt").alias("last_shop_date"),
            F.sum("sum_shop_counts").alias("total_num_shops")
        )
    )

    # join to travel dates df to fill in the gaps of missing shop days
    market_agg_join_df = (market_agg_df
                            .join(travel_date_cross_df, 
                                on=["outDeptDt_dt", "stay_duration"],
                                how="outer")
                            .fillna(0) #, subset=["num_shop_days", "last_shop_date"])
                        )
                        
    # save the flat version of the agg'd data
    print("...Saving flat data")
    parquet_path = output_dir + "flat/" + file_name
    market_agg_join_df.write.mode("overwrite").parquet(parquet_path)

    # # --------------------------
    # # TABULAR OUTPUT
    # # --------------------------

    # # summarize output as tuple of (date last shop, toal num days w/ shop)
    # print("...Formatting & saving desired output")

    # market_output_df = (market_agg_join_df
    #     .withColumn("output_arr", F.array("last_shop_date", "num_shop_days"))
    #     # must convert to string, as .csv can't accept arrays
    #     .withColumn("output_str", F.concat_ws(",", "output_arr"))
    # )
    # # note we could save flat file here instead of above

    # # pivot on out-date and in-date, and save as .csv
    # pvt = (market_output_df
    #         .groupBy("outDeptDt")
    #         .pivot("inDeptDt")
    #         .agg(F.first("output_str"))
    #         .orderBy("outDeptDt")
    # )

    # csv_file_path = output_dir + "pivot_csv/" + file_name + ".csv"
    # pvt.write.mode("overwrite").csv(csv_file_path, header=True)
    
    print("Finished with market {}!".format(market))
    print("========================")

print("END OF SCRIPT")