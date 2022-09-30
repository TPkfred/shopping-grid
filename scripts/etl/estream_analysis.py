
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
# NOTE: modified this for testing
grid_out_dir = "/user/kendra.frederick/shop_vol/v5/raw/"


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
TOP_N = args.top_n
include_pcc = args.include_pcc

top_markets_out_dir = "/user/kendra.frederick/lookups/top_markets/top_{}_v2/".format(TOP_N)


start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

script_start_time = datetime.datetime.now()
print("{} - Starting script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))

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

jvm = spark._jvm
jsc = spark._jsc
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

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
    
    # for fare analysis
    'pos', # is this required for fare analysis?
    'currency',

    # not in source data; gets added below
    # 'outMrktCxr_single', 
    # 'inMrktCxr_single', 
    'round_trip', 
]

if include_pcc:
    groupby_cols += ['pcc']

# ========================
# HELPER FUNCTIONS & CLAUSES

# filters 
# for round-trip: outOrigin = inDest and outDest = inOrigin 
cond_rt = (((F.col("outOriginAirport") == F.col("inDestinationAirport")) &
         (F.col("outDestinationAirport") == F.col("inOriginAirport"))))
cond_ow = ((F.col("inOriginAirport") == 0) & (F.col("inDestinationAirport") == 0))

# ========================
# MAIN

def common_data_preprocessing(df_raw):
    # filter on conditions
    df_filt = df_raw.filter(cond_rt | cond_ow)
    # add round-trip indicator
    df_filt = df_filt.withColumn("round_trip", 
        F.when(cond_rt, 1).otherwise(0))

    df_expl = (df_filt
        .withColumn("PTC", F.explode("responsePTC"))
        .withColumn("farePTC", F.explode("fareBreakDownByPTC"))
        )
    df_adt = df_expl.filter(F.col("PTC") == "ADT")
    return df_adt
    


def grid_analysis(df_preproc, date_str):
    func_start = datetime.datetime.now()
    print("Starting grid analysis")
    
    # add a market key to aid in joining
    df_preproc = df_preproc.withColumn(
        "market_key",
        F.concat_ws("-",
            F.col("outOriginAirport").cast(T.StringType()), 
            F.col("outDestinationAirport").cast(T.StringType())))

    # join with top markets to restrict analysis 
    df_join = df_preproc.join(F.broadcast(markets_df), 
        on="market_key", how="inner")
    

    df_agg = (df_join
        .groupBy(groupby_cols)
        .agg(
            F.countDistinct("shopID").alias("shop_counts"),
            F.min("farePTC").alias("min_fare")
        )
    )
    
    day_df = df_agg.withColumn("searchDt", F.lit(date_str))
    day_df.show(5)
    # ALT: include date in filename, and change mode to overwrite
        # with current approach, long-term will need to partition
        # output data into different folders, eventually (perhaps by month?)
    if include_pcc:
        num_partitions = 3
        save_path = grid_out_dir + "with_pcc/" + date_str
    else:
        num_partitions = 2
        save_path = grid_out_dir + date_str

    day_df.repartition(num_partitions).write.mode("append").parquet(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with grid analysis. Elasped time: {}".format(elapsed_time))
    print("")


def top_market_analysis(df_preproc, date_str):
    func_start = datetime.datetime.now()
    print("Starting top market counts")
    save_path = top_markets_out_dir + date_str # + ".csv"

    # First let's check if a .csv already exists. If so, we can
    # skip processing!
    if fs.exists(jvm.org.apache.hadoop.fs.Path(save_path)):
        print(".csv exists. Skipping processing.")
    else:
        df_agg = (df_preproc
                .groupBy("outOriginAirport", "outDestinationAirport")
                .agg(
                    # note: solution counts includes double-counts from exploding
                    # PTC and fare -- so it represents an over-estimate of the solutions
                    # returned. But since we don't use them, I'm not worrying
                    # about correcting it. Keeping in to give an idea of the number
                    # of solutions generated vs num shops.
                    F.count("transactionID").alias("solution_counts"),
                    F.countDistinct("shopID").alias("num_unique_shops")
                )
                .orderBy(F.desc("num_unique_shops"))
                .limit(TOP_N)
                ).coalesce(1)
        day_df = df_agg.withColumn("date_str", F.lit(date_str))
        day_df.show(5)
        # ignore or overwite here?
        day_df.write.mode("overwrite").option("header", True).csv(save_path)
        
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

    df_preproc = common_data_preprocessing(df_raw)
    grid_analysis(df_preproc, date_str)
    top_market_analysis(df_preproc, date_str)

    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))
    print("***DONE PROCESSING DAY: {}***".format(date_str))
    print("")


# load markets we want to analyze
markets_df = spark.read.csv(top_markets_path, header=True)
print("Top markets df:")
markets_df.show(5)
print("")


# LOOP OVER SHOPPING DAYS
num_days = (end_dt - start_dt).days
date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

for date in date_list:
    daily_analysis(date)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
