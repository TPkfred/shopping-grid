
"""
Volume counts for estreaming data coverage analysis
by specific markets and destinations

Per market / route, count 
- total number of solutions (i.e. records in estreaming)
- number of "shops" (i.e. unique shopID)

Constraints on data:
- one-way or round-trip only
- data only analyzed for a specified list of markets / routes

Fields that are grouped by (which may or may not be used in subsequent analysis)
- out-bound airports (origin, destination)
- out-bound departure date
- in-bound return date
- round-trip

"""
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, BooleanType, FloatType

from functools import reduce
import datetime
from calendar import Calendar
import argparse

# ========================
# VARIABLE DEFINITIONS
# these will likely because arguments some day

APP_NAME = "KF-ShoppingGrid"
data_dir = "/data/estreaming/midt_1_5/"
markets_path = "/user/kendra.frederick/shop_grid/markets.csv"
markets_out_dir = "/user/kendra.frederick/shop_vol/raw/v2/markets"
destinations_path = "/user/kendra.frederick/shop_grid/destinations.csv"
dest_out_dir = "/user/kendra.frederick/shop_vol/raw/v2/destinations"

# TOP_N = 100

# ========================
# SET UP SPARK

# I do this so I can run using spark-submit or python

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run-mode",
    help="Run-mode",
    choices=("spark-submit", "python")
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
args = parser.parse_args()

run_mode = args.run_mode
shop_start_str = args.shop_start
shop_end_str = args.shop_end

start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

script_start_time = datetime.datetime.now()
print("Starting at: {}".format(script_start_time.strftime("%Y-%m-%d %H:%M")))

if run_mode == "python":
    conf = pyspark.SparkConf().setAll(
        [('spark.master','yarn'),
        ('spark.app.name', APP_NAME),
        ('spark.driver.memory','30g'),
        ('spark.executor.memory', '20g'),
        ('spark.executor.instances', 10),
        ('spark.executor.cores', '5'),

        ])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
elif run_mode == "spark-submit":
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
else:
    pass

# ========================
# ADDITIONAL VARIABLE DEFINTIONS

spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

groupby_cols = [
    'outOriginAirport',
    'outDestinationAirport',
    'outDeptDt',
    
    # due to conditions imposed, we don't need these
    # 'inOriginAirport',
    # 'inDestinationAirport',
    
    'inDeptDt',
    
    # May want to analyze by these at some point (e.g. if adding in fare), 
    # but not right now
    # 'pos',
    # 'currency',
    # 'outBookingClass',
    # 'outCabinClass',
    # 'outMrktCxr_single', # not in source data; gets added below
    # 'inMrktCxr_single', # not in source data; gets added below

    'round_trip', # not in source data; gets added below
]

# ========================
# HELPER FUNCTIONS & CLAUSES

# filters for round-trip and one-way flights
    # for round-trip: outOrigin = inDest and outDest = inOrigin 
cond_rt = (((F.col("outOriginAirport") == F.col("inDestinationAirport")) &
         (F.col("outDestinationAirport") == F.col("inOriginAirport"))))
cond_ow = ((F.col("inOriginAirport") == 0) & (F.col("inDestinationAirport") == 0))

# ========================
# MAIN

# load markets
markets_df = spark.read.csv(markets_path, header=True)
markets_df = markets_df.withColumn("market_key",
    F.concat_ws("-",
    F.col("OriginAirport").cast(StringType()), 
    F.col("DestinationAirport").cast(StringType()))
)
print("Analyzing {} markets".format(markets_df.count()))
# markets_df.show()

dest_df = spark.read.csv(destinations_path, header=True)
print("Analyzing {} destinations".format(dest_df.count()))
# dest_df.show()
print("")


def market_counts(df_raw, date_str):
    # add a market key to aid in joining
    df_raw = df_raw.withColumn(
        "market_key",
        F.concat_ws("-",
            F.col("outOriginAirport").cast(StringType()), 
            F.col("outDestinationAirport").cast(StringType())))

    # join with top markets to restrict analysis 
    df_join = df_raw.join(F.broadcast(markets_df), 
        on="market_key", how="inner")
    
    # filter on conditions
    df_filt = df_join.filter(cond_rt | cond_ow)
        # add round-trip indicator

    df_agg = (df_filt
        .groupBy(groupby_cols)
        .agg(
            F.count("transactionID").alias("solution_counts"),
            F.countDistinct("shopID").alias("shop_counts"),
        )
    )
    
    day_df = df_agg.withColumn("searchDt", F.lit(date_str))
    # <1 partitions per hour is optimal
    day_df.repartition(2).write.mode("append").parquet(markets_out_dir)
    print("Done with markets agg + counts")


def destination_counts(df_raw, date_str):
    # join with destinations to restrict analysis 
    df_join = df_raw.join(
        F.broadcast(dest_df), 
        df_raw["outDestinationAirport"] == dest_df["DestinationAirport"],
        how="inner").drop("DestinationAirport")
    
    # filter on conditions
    df_filt = df_join.filter(cond_rt)
    
    # groupby & count
    df_agg = (df_filt
        .groupBy(groupby_cols)
        .agg(
            F.count("transactionID").alias("solution_counts"),
            F.countDistinct("shopID").alias("shop_counts"),
        )
    )
    
    day_df = df_agg.withColumn("searchDt", F.lit(date_str))
    # <1 partitions per hour is optimal
    day_df.repartition(2).write.mode("append").parquet(dest_out_dir)

    print("Done with destination agg + counts")

# this is where the action happens!
def calc_counts_per_day(date):
    """Load data from `data_dir` for ` date` and calculate counts"""
    loop_start = datetime.datetime.now()
    date_str = date.strftime("%Y%m%d")
    hdfs_path = "hdfs://" + data_dir + date_str + "/" + "*"
    # setting so we don't error out on corrupt files
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

    print("Starting to load/process data for {}".format(date_str))
    print("Starting at: {}".format(loop_start.strftime("%Y-%m-%d %H:%M")))
    try:
        df_raw = spark.read.parquet(hdfs_path)
    except:
        print("COULD NOT LOAD/FIND {}. skipping.".format(hdfs_path))
        return None
    print("Done reading raw data")

    # add round-trip indicator
    df_raw = df_raw.withColumn("round_trip", 
        F.when(cond_ow, 0).otherwise(1))

    market_counts(df_raw, date_str)
    destination_counts(df_raw, date_str)

    print("***DONE PROCESSING DAY: {}***".format(date_str))
    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))


num_days = (end_dt - start_dt).days
date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]
for date in date_list:
    calc_counts_per_day(date)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
