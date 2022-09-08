
"""
Script to combine daily analysis of estreaming data
- Lowest Fare Calendar
    - Data agg'd by market
        - Boolean shop + volume counts for data coverage analysis
        - Lowest fare by market
    - Restrictions
        - top 1000 markets (previously determined)
        - paxtype = ADT
        - round-trip or one-way (no multi-city itineraries)
- Top Market counts
    - Counts of:
        - total number of solutions (i.e. records in estreaming)
        - number of "shops" (i.e. unique shopID)
    - save top 10,000
"""

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 

import datetime
import argparse

# ========================
# VARIABLE DEFINITIONS
# these will likely because arguments some day

APP_NAME = "KF-ShoppingGrid"
data_dir = "/data/estreaming/midt_1_5/"

top_markets_path = "/user/kendra.frederick/lookups/top_markets/top_1000_markets_encoded.csv"
grid_out_dir = "/user/kendra.frederick/shop_vol/v4/raw/"


# ========================
# SET UP SPARK

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run-mode",
    help="Run-mode",
    choices=("spark-submit", "python"),
    default="spark-submit"
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
    "--top-n",
    help="The top N markets to save counts for",
    type=int,
    default=10000
)
args = parser.parse_args()

run_mode = args.run_mode
shop_start_str = args.shop_start
shop_end_str = args.shop_end
TOP_N = args.top_n

top_markets_out_dir = "/user/kendra.frederick/lookups/top_markets/top_{}/".format(TOP_N)


start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

script_start_time = datetime.datetime.now()
print("{} - Starting script at".format(script_start_time.strftime("%Y-%m-%d %H:%M")))

# so we can run using spark-submit or python (though usually )
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
    # 'pos', # is this required for fare analysis?
    'currency',
    # 'outBookingClass',
    # 'outCabinClass',

    # 'outMrktCxr_single', # not in source data; gets added below
    # 'inMrktCxr_single', # not in source data; gets added below
    'round_trip', # not in source data; gets added below
]

# ========================
# HELPER FUNCTIONS & CLAUSES

# filters 
# for round-trip: outOrigin = inDest and outDest = inOrigin 
cond_rt = (((F.col("outOriginAirport") == F.col("inDestinationAirport")) &
         (F.col("outDestinationAirport") == F.col("inOriginAirport"))))
cond_ow = ((F.col("inOriginAirport") == 0) & (F.col("inDestinationAirport") == 0))
cond_adt = (F.col("paxtype") == "ADT")

# ========================
# MAIN

# load markets we want to analyze
markets_df = spark.read.csv(top_markets_path, header=True)
# print("Analyzing {} markets".format(markets_df.count()))
print("Top markets df:")
markets_df.show(5)
print("")


def grid_analysis(df_raw, date_str):
    # add a market key to aid in joining
    print("Starting grid analysis")
    df_raw = df_raw.withColumn(
        "market_key",
        F.concat_ws("-",
            F.col("outOriginAirport").cast(T.StringType()), 
            F.col("outDestinationAirport").cast(T.StringType())))

    # join with top markets to restrict analysis 
    df_join = df_raw.join(F.broadcast(markets_df), 
        on="market_key", how="inner")
    
    # filter on conditions
    df_filt = df_join.filter(cond_rt | cond_ow)
    # add round-trip indicator
    df_filt = df_filt.withColumn("round_trip", 
        F.when(cond_rt, 1).otherwise(0))

    df_expl = (df_filt
        .withColumn("PTC", F.explode("responsePTC"))
        .withColumn("farePTC", F.explode("fareBreakDownByPTC"))
        )

    df_adt = df_expl.filter(F.col("PTC") == "ADT")

    df_agg = (df_adt
        .groupBy(groupby_cols)
        .agg(
            F.count("transactionID").alias("solution_counts"),
            F.countDistinct("shopID").alias("shop_counts"),
            F.min("farePTC").alias("min_fare")
        )
    )
    
    day_df = df_agg.withColumn("searchDt", F.lit(date_str))
    day_df.show(5)
    # <1 partitions per hour is optimal
    # ALT: include date in filename, and change mode to overwrite
        # with current approach, long-term will need to partition
        # output data into different folders, eventually (perhaps by month?)
    day_df.repartition(2).write.mode("append").parquet(grid_out_dir)
    print("Done with grid analysis")


def top_market_analysis(df_raw, date_str):
    print("Starting top market counts")
    save_path = top_markets_out_dir + date_str # + ".csv"
    df_agg = (df_raw
              .groupBy("outOriginAirport", "outDestinationAirport")
              .agg(
                F.count("transactionID").alias("solution_counts"),
                F.countDistinct("shopID").alias("num_unique_shops")
              )
              .orderBy(F.desc("num_unique_shops"))
              .limit(TOP_N)
              ).coalesce(1)
    day_df = df_agg.withColumn("date_str", F.lit(date_str))
    day_df.show(5)
    # ignore or overwite here??
    day_df.write.mode("overwrite").option("header", True).csv(save_path)
    print("Done with top market counts")


def daily_analysis(date):
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
    hdfs_path = "hdfs://" + data_dir + date_str + "/" + "*"
    # setting so we don't error out on corrupt files
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

    print("{} - starting to process data for {}".format(
        loop_start.strftime("%Y-%m-%d %H:%M"), date_str))
    try:
        df_raw = spark.read.parquet(hdfs_path)
    except:
        print("COULD NOT LOAD/FIND {}. skipping.".format(hdfs_path))
        return None
    print("Done reading raw data")

    grid_analysis(df_raw, date_str)
    top_market_analysis(df_raw, date_str)

    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))
    print("***DONE PROCESSING DAY: {}***".format(date_str))
    print("")


# LOOP OVER SHOPPING DAYS
num_days = (end_dt - start_dt).days
date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

for date in date_list:
    daily_analysis(date)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
