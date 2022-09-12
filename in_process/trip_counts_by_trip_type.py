
"""
Counts "Top" Market from eStreaming datalake data
- Counts:
    - total number of solutions (i.e. records in estreaming)
    - number of "shops" (i.e. unique shopID)
- Broken out by market + trip type (one-way (ow), round-trip (rt), or other (other))
- TOP_N saved to file
"""

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.window import Window

import datetime
import argparse

# ========================
# VARIABLE DEFINITIONS
# these will likely because arguments some day

APP_NAME = "KF-ShoppingGrid"
data_dir = "/data/estreaming/datalake_1_5/"

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
include_pcc = args.include_pcc

top_markets_out_dir = "/user/kendra.frederick/lookups/top_markets/top_{}_v2/".format(TOP_N)


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

# ========================
# HELPER FUNCTIONS & CLAUSES

# filters 
# for round-trip: outOrigin = inDest and outDest = inOrigin 
cond_rt = (((F.col("out_origin_airport") == F.col("in_destination_airport")) &
         (F.col("out_destination_airport") == F.col("in_origin_airport"))))
cond_ow = ((F.col("in_origin_airport").isNull()) & (F.col("in_destination_airport").isNull()))


def top_market_analysis(df_raw, date_str):
    # Note: we use "raw" data, without any filters. We just want to get a 
    # crude idea of what's being shopped for. This approach includes 
    # multi-city  trips, where out origin != in destination, but so be it.
    func_start = datetime.datetime.now()
    print("Starting top market counts")
    save_path = top_markets_out_dir + date_str
    
    # add trip type
    df_mod = df_raw.withColumn("trip_type", 
        F.when(cond_rt, "rt").otherwise(
            F.when(cond_ow, "ow").otherwise("other")
        ))

    df_agg = (df_mod
                .groupBy("out_origin_airport", 
                         "out_destination_airport", 
                         "trip_type")
                .agg(
                    F.count("id").alias("solution_counts"),
                    F.countDistinct("id").alias("num_unique_shops")
                )
            )

    w = (Window
        .partitionBy("out_origin_airport", "out_destination_airport")
        )

    df_agg = (df_agg
            .withColumn("total_shop_counts_market", F.sum("num_unique_shops").over(w))
            .withColumn("pct_shop_count_by_trip_type", 
                        F.col("num_unique_shops")/F.col("total_shop_counts_market"))
            )

    w2 = (Window
        .orderBy(F.desc("total_shop_counts_market"))
        )

    df_agg = df_agg.withColumn("total_shop_counts_rank", F.rank().over(w2))
    df_agg_filt = df_agg.filter(F.col("total_shop_counts_rank") <= TOP_N)

    day_df = df_agg_filt.withColumn("date_str", F.lit(date_str))
    day_df.show(5)
    # ignore or overwite here?
    day_df.coalesce(1).write.mode("overwrite").option("header", True).csv(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with top market counts. Elasped time: {}".format(elapsed_time))


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

    top_market_analysis(df_raw, date_str)

    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))
    print("***DONE PROCESSING DAY: {}***".format(date_str))
    print("")

# ========================
# MAIN


# LOOP OVER SHOPPING DAYS
num_days = (end_dt - start_dt).days
date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

for date in date_list:
    daily_analysis(date)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
