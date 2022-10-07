"""
Estreaming data source = datalake format

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
from math import ceil


# ========================
# VARIABLE DEFINITIONS
# these will likely because arguments some day

APP_NAME = "KF-ShoppingGrid"
data_dir = "/data/estreaming/datalake_1_5/"
# don't want "encoded" version of top markets, as datalake format contains decoded values
top_markets_path = "/user/kendra.frederick/lookups/top_markets/top_1000_markets.csv"
# don't need 'raw' data/folder designation, because fields are already decoded
grid_out_dir = "/user/kendra.frederick/shop_vol/v5_datalake/"
top_markets_out_dir = "/user/kendra.frederick/lookups/top_markets/daily_counts/datalake_1_5/"
# determined off-line / previously
opt_num_grid_parts = 3

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
# parser.add_argument(
#     "--top-n",
#     help="The top N markets to save counts for",
#     type=int,
#     default=10000
# )
parser.add_argument(
    "--include-pcc", "-pcc",
    help="Whether to include PCC in the groupby",
    default=False,
    action="store_true"
)
args = parser.parse_args()

run_mode = args.run_mode
shop_start_str = args.shop_start
shop_end_str = args.shop_end
# TOP_N = args.top_n
include_pcc = args.include_pcc

# top_markets_out_dir = "/user/kendra.frederick/lookups/top_markets/top_{}_dl/".format(TOP_N)


start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

script_start_time = datetime.datetime.now()
print("{} - starting script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
print("Processing shopping days {} to {} (inclusive)".format(shop_start_str, shop_end_str))

# ========================
# SETUP SPARK

# so we can run using spark-submit or python 
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
    'out_origin_airport',
    'out_destination_airport',
    'out_origin_city',
    'out_destination_city',
    # NOTE: due to conditions imposed, we don't need in_ fields
    'point_of_sale', # should correspond 1:1 w/ currency, but also
                     # useful for filtering by country / customer market
    'currency', # for fare analysis
    # not in source data; these get created/added below
    'round_trip_ind',
    'out_departure_date_gmt',
    'in_departure_date_gmt'
]

if include_pcc:
    groupby_cols += ['pcc', 'gds']

# ========================
# HELPER FUNCTIONS & CLAUSES

# filters 
# for round-trip: outOrigin = inDest and outDest = inOrigin 
cond_rt = (((F.col("out_origin_airport") == F.col("in_destination_airport")) &
         (F.col("out_destination_airport") == F.col("in_origin_airport"))))
cond_ow = ((F.col("in_origin_airport").isNull()) & (F.col("in_destination_airport").isNull()))


def grid_analysis(df_preproc, date_str):
    func_start = datetime.datetime.now()
    print("Starting grid analysis")
    
    # add a market key to aid in joining
    df_preproc = df_preproc.withColumn(
        "market_key",
        F.concat_ws("-",
            F.col("out_origin_airport"), 
            F.col("out_destination_airport"))
    )
    # join with top markets to restrict analysis 
    df_join = df_preproc.join(F.broadcast(markets_df), 
        on="market_key", how="inner")
    
    # add columns 
    # - convert epoch times to dates (in GMT)
    # - round-trip indicator
    df_join = (df_join
                .withColumn("out_departure_date_gmt",
                    F.from_unixtime(F.col("out_departure_epoc"), "yyyy-MM-dd"))
                .withColumn("in_departure_date_gmt",
                    F.from_unixtime(F.col("in_departure_epoc"), "yyyy-MM-dd"))
                .withColumn("round_trip_ind", 
                    F.when(cond_rt, 1).otherwise(0))
    )
    
    # filter on conditions
    df_filt = df_join.filter(cond_rt | cond_ow)
    
    # explode out PTC...
    df_expl = (df_filt
        .withColumn("PTC", F.explode("response_PTC"))
        .withColumn("fare_PTC", F.explode("fare_break_down_by_PTC"))
        )
    # ...so we can filter on ADT
    df_adt = df_expl.filter(F.col("PTC") == "ADT")

    df_agg = (df_adt
        .groupBy(groupby_cols)
        .agg(
            # Solution count is inaccurate, because we've exploded. Besides, 
            # we don't use. So, omit it
            # F.count("id").alias("solution_counts"),
            F.countDistinct("id").alias("shop_counts"),
            F.min("fare_PTC").alias("min_fare")
        )
    )
    
    day_df = df_agg.withColumn("search_date", F.lit(date_str))
    day_df.show(5)
    if include_pcc:
        num_partitions = int(ceil(opt_num_grid_parts * 1.5))
        save_path = grid_out_dir + "with_pcc/" + date_str
    else:
        num_partitions = opt_num_grid_parts
        save_path = grid_out_dir + date_str

    day_df.repartition(num_partitions).write.mode("overwrite").parquet(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with grid analysis. Elasped time: {}".format(elapsed_time))
    print("")


def top_market_analysis(df_raw, date_str):
    # Note: we use "raw" data, without any filters. We just want to get a 
    # crude idea of what's being shopped for. This approach includes 
    # multi-city  trips, where out origin != in destination.
    func_start = datetime.datetime.now()
    print("Starting top market counts")
    save_path = top_markets_out_dir + date_str
    
    df_agg = (df_raw
              .groupBy("out_origin_airport", "out_destination_airport")
              .agg(
                F.count("id").alias("solution_counts"),
                F.countDistinct("id").alias("num_unique_shops")
              )
              .orderBy(F.desc("num_unique_shops"))
              ).coalesce(1)
    day_df = df_agg.withColumn("date_str", F.lit(date_str))
    day_df.show(5)
    # ignore or overwite here? Could also add logic above to check if .csv
    # already exists and skip processing if it doesn.
    day_df.write.mode("overwrite").option("header", True).csv(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with top market counts. Elasped time: {}".format(elapsed_time))
    print("")


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

# ========================
# MAIN

# load markets we want to analyze
markets_df = spark.read.csv(top_markets_path, header=True)
print("Will analyze shopping coverage for {} markets".format(markets_df.count()))

# LOOP OVER SHOPPING DAYS
num_days = (end_dt - start_dt).days
date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

for date in date_list:
    daily_analysis(date)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
