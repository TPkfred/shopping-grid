"""
"""

import os
import datetime
import argparse
import numpy as np
import pandas as pd

# import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# import pyspark.sql.types as T
# from pyspark.sql.window import Window


pd.options.mode.chained_assignment = None

# TODO: derive input dir from pos and currency args
INPUT_DIR = "/user/kendra.frederick/shop_grid/US-USD/"
# OUTPUT_DIR = "/data/16/kendra.frederick/shopping_grid/output/market_pkl_files_take2"
# OUTPUT_DIR = "/tmp/kf-cco/"
OUTPUT_DIR = "/home/kendra.frederick/shopping_grid/output/market_pkl_files_take2"


APP_NAME = "generate-market-pkl-files"
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# TODO: allow this to be supplied by arguments
market_list = [
 'JFK-LHR',
     'LHR-JFK',
 'LAX-JFK',
 'JFK-LAX',
 'JFK-LGW',
 'EWR-LHR',
    'LHR-EWR',
    'EWR-CDG',
    'LHR-LAX',
 'LAX-EWR',
     'SFO-LAX',
    'LAX-SFO',
 'EWR-MIA',
 'JFK-MIA',
    'LGA-MIA',
 'EWR-FLL',
 'EWR-LAX',
     'ATL-EWR',
    'OAK-LAS',
 'DFW-ORD',
 'ATL-LAS',
 'AUS-DEN'
]


parser = argparse.ArgumentParser()
parser.add_argument(
    "--shop-start",
    help="Date of shopping data to begin processing at (inclusive). "
    "Format YYYY-MM-DD.",
    type=str,
    # required=True
)
parser.add_argument(
    "--shop-end",
    help="Date of shopping data to end processing at (inclusive). "
    "Format YYYY-MM-DD.",
    type=str,
    # required=True
)
args = parser.parse_args()


script_start_time = datetime.datetime.now()
print("*****************************")
print("{} - Starting Script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
print("Script arguments / params: {}".format(args))
print("Saving data to: {}".format(OUTPUT_DIR))

shop_start = args.shop_start
shop_end = args.shop_end


df = spark.read.parquet(INPUT_DIR)

if shop_start is not None:
    df = df.filter(F.col("searchDt") >= shop_start)
if shop_end is not None:
    df = df.filter(F.col("searchDt") <= shop_end)

for market in market_list:
    print("Working on {}".format(market))
    loop_start = datetime.datetime.now()

    market_df = df.filter(F.col("market") == market)
    market_pdf = market_df.toPandas().drop_duplicates()
    filename = "{}.pkl".format(market)
    market_pdf.to_pickle(os.path.join(OUTPUT_DIR, filename))

    loop_end = datetime.datetime.now()
    elapsed_time = (loop_end - loop_start).total_seconds() / 60
    print("Loop elapsed time: {:.02f} minutes".format(elapsed_time))
    print("")

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))
print("*****************************")
print("")