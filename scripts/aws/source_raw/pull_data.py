

import os
import datetime
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.window import Window


APP_NAME = "KF-ShoppingGrid"
BASE_INPUT_DIR = "s3://tvlp-ds-air-shopping-pn/v1_5"
BASE_OUTPUT_DIR = "s3://tvlp-ds-users/kendra-frederick/shopping-grid/tmp"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test", "-t",
        help="Run in test mode: only process one hour's worth of data",
        default=False,
        action="store_true"
    )
    args = parser.parse_args()
    return args

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


if __name__ == "__main__":

    args = parse_args()
    test = args.test

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    # shop date
    date = datetime.date(2022,10,31)
    dd_int = 20221113
    org = "JFK"
    dst = "LHR"
    
    loop_start = datetime.datetime.now()
    date_str = date.strftime("%Y%m%d")
    
    print("{} - starting to process data for {}".format(
        loop_start.strftime("%Y-%m-%d %H:%M"), date_str))

    # define input path
    if test:
        print("Running in test mode -- only loading 1 hour of data")
        hdfs_path = "{}/year={}/month={}/day={}/hour=19/".format(BASE_INPUT_DIR, date.year, date.month, date_str)
    else:
        hdfs_path = "{}/year={}/month={}/day={}".format(BASE_INPUT_DIR, date.year, date.month, date_str)
    
    df = spark.read.parquet(hdfs_path)

    df_filt = df.filter(
        (F.col("out_origin_airport") == org) & (F.col("out_destination_airport") == dst)
        & (F.col("out_departure_date") == dd_int) & (F.col("in_origin_airport").isNull())
    ).coalesce(2)
    df_expl = explode_fare_filter_adt(df_filt)
    output_path = "{}/{}-{}".format(BASE_OUTPUT_DIR, org, dst)
    df_expl.write.mode("overwrite").parquet(output_path)
    
    spark.close()  
