
"""
Analyze coverage and lowest fare by market & POS in eStreaming data.

- Data grouped by market and other columns and agg'd for:
    - Boolean shop + volume counts for data coverage analysis
    - Lowest fare
- Restrictions
    - passenger type = ADT
    - round-trip or one-way (no multi-city itineraries)
    - POS = US or IN

Version notes:
- 09/21/2022
    - fix index-matching between responsePTC and fareBreakDownByPTC
    - limits POS's to US and IN to further reduce processing load
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
# /raw gets added below
grid_out_dir = "/user/kendra.frederick/shop_vol/v7/"


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
    "--include-pcc", "-pcc",
    help="Whether to include PCC in the groupby",
    default=False,
    action="store_true"
)
args = parser.parse_args()

run_mode = args.run_mode
shop_start_str = args.shop_start
shop_end_str = args.shop_end
include_pcc = args.include_pcc


# # both are minus Russia
# if args.num_pos == "long":
#     # top 20
#     pos_list_str = ["US", "GB", "HK", "IN", "DE", "CA", "AU", "FR", "TW", "IT", "ES", "TH", "JP", "KR", "PT", "NL", "PH", "IL", "AE"]
#     pos_list = [353566720, 117571584, 134938624, 151912448, 67436544, 50397184, 18153472, 101842944, 337051648, 152305664, 85131264, 336068608, 168820736, 185729024, 269746176, 235667456, 268959744, 151781376, 17104896]
# else:
#     # being more selective - top 10 POS = 83% of shopping volume 
#     pos_list_str = ["US", "GB", "HK", "IN", "DE", "CA", "AU", "FR"]
#     pos_list = [353566720, 117571584, 134938624, 151912448, 67436544, 50397184, 18153472, 101842944]

pos_list = [353566720, 151912448]
pos_list_str = ["US", "IN"]

start_dt = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d")
end_dt = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d")

script_start_time = datetime.datetime.now()
print("{} - Starting script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
print("Processing shopping days {} to {} (inclusive)".format(shop_start_str, shop_end_str))
print("Analyzing these POS's: {}".format(pos_list_str))
print("Saving coverage & fare analysis to: {}".format(grid_out_dir))

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

# so we don't error out on corrupt files. 
# Note that these fail silently, so this could be improved
spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

groupby_cols = [
    'outOriginAirport',
    'outDestinationAirport',
    'outDeptDt',
    'inDeptDt',
    'pos',
    # for fare analysis
    'currency',
    # not in source data; gets added below
    'round_trip', 
]

if include_pcc:
    groupby_cols += ['pcc', 'gds'] # 9/16/2022: added GDS

# ========================
# HELPER FUNCTIONS & CLAUSES

# filters 
# for round-trip: outOrigin = inDest and outDest = inOrigin 
cond_rt = (((F.col("outOriginAirport") == F.col("inDestinationAirport")) &
         (F.col("outDestinationAirport") == F.col("inOriginAirport"))))
cond_ow = ((F.col("inOriginAirport") == 0) & (F.col("inDestinationAirport") == 0))

# ========================
# MAIN

def data_preprocessing(df_raw):
    # filter on trip type (tt)
    tt_filt = df_raw.filter(cond_rt | cond_ow)
    # add round-trip indicator
    tt_filt = tt_filt.withColumn("round_trip", 
        F.when(cond_rt, 1).otherwise(0))
    # filter on POS
        # move this here to restrict data size before we explode
    pos_filt = tt_filt.filter(F.col("pos").isin(pos_list))

    # explode PTC and fare & match their position
    df_expl = (pos_filt
        .select("*",  F.posexplode("responsePTC").alias("ptc_pos", "PTC"))
        .select("*", F.posexplode("fareBreakDownByPTC").alias("fare_pos", "farePTC"))
        .filter(F.col("ptc_pos") == F.col("fare_pos"))
        .drop("ptc_pos", "fare_pos")
        )
    # filter on ADT
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

    # groupby & agg
    df_agg = (df_preproc
        .groupBy(groupby_cols)
        .agg(
            F.countDistinct("shopID").alias("shop_counts"),
            F.min("farePTC").alias("min_fare")
        )
    )
    
    day_df = df_agg.withColumn("searchDt", F.lit(date_str))
    day_df.show(5)
    if include_pcc:
        num_partitions = 6
        save_path = grid_out_dir + "raw_with_pcc/" + date_str
    else:
        num_partitions = 3
        save_path = grid_out_dir + "raw/" + date_str

    # with switch to partitioning data by day, write mode can be overwrite (not append)
    day_df.repartition(num_partitions).write.mode("overwrite").parquet(save_path)
    
    func_end = datetime.datetime.now()
    elapsed_time = (func_end - func_start).total_seconds() / 60
    print("Done with grid analysis. Elasped time: {}".format(elapsed_time))
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

    print("{} - starting to process data for {}".format(
        loop_start.strftime("%Y-%m-%d %H:%M"), date_str))
    try:
        df_raw = spark.read.parquet(hdfs_path)
    except:
        print("COULD NOT LOAD/FIND {}. skipping.".format(hdfs_path))
        return None
    print("Done reading raw data")

    df_preproc = data_preprocessing(df_raw)
    grid_analysis(df_preproc, date_str)

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
