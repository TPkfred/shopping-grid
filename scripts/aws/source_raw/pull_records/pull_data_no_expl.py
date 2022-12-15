

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
        "--origin", "-o",
        help="Outbound origin airport",
        type=str,
        required=True
    )
    parser.add_argument(
        "--destination", "-d",
        help="Outbound destination airport",
        type=str,
        required=True
    )
    parser.add_argument(
        "--shop-date", "-sd",
        help="Date of shopping data. Of format YYYYMMDD",
        type=str,
        required=True
    )
    parser.add_argument(
        "--departure-date", "-dd",
        help="Outbound departure date. Of format YYYYMMDD",
        type=str,
        required=True
    )
    parser.add_argument(
        "--return-date", "-rd",
        help="Inbound departure date. If not specified, will pull data for one-ways",
        type=str,
        # required=True
    )
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
    script_start_time = datetime.datetime.now()

    args = parse_args()
    print(args)
    test = args.test


    sd_str = args.departure_date
    dd_str = args.departure_date
    rd_str = args.return_date
    org = args.origin
    dst = args.destination

    # shop date
    # date = datetime.date(2022,10,31)
    shop_date = datetime.datetime.strptime(sd_str, "%Y%m%d") # format doesn't matter
    # org = "JFK"
    # dst = "LHR"

    # dd_int = 20221113
    sd_int = int(sd_str)
    dd_int = int(dd_str)

    # date_str = date.strftime("%Y%m%d")
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    # define input path
    if test:
        print("Running in test mode -- only loading 1 hour of data")
        hdfs_path = "{}/year={}/month={}/day={}/hour=19/".format(BASE_INPUT_DIR, shop_date.year, shop_date.month, sd_str)
    else:
        hdfs_path = "{}/year={}/month={}/day={}".format(BASE_INPUT_DIR, shop_date.year, shop_date.month, sd_str)
    
    print("Reading data")
    df = spark.read.parquet(hdfs_path)

    # TODO: add in logic for rd_str (right now, assume it's null)
    df_filt = df.filter(
        (F.col("out_origin_airport") == org) 
        & (F.col("out_destination_airport") == dst)
        & (F.col("out_departure_date") == dd_int) 
        & (F.col("in_origin_airport").isNull())
    )
    
    # df_expl = explode_fare_filter_adt(df_filt)
    output_path = "{}/{}-{}-dept-{}/{}".format(BASE_OUTPUT_DIR, org, dst, dd_str, sd_str)
    print("Writing filtered data")
    # df_expl.coalesce(2).write.mode("overwrite").parquet(output_path)
    df_filt.coalesce(10).write.mode("overwrite").parquet(output_path)
    
    script_end_time = datetime.datetime.now()

    elapsed_time = (script_end_time - script_start_time).total_seconds() / 60
    print("Done! Elasped time: {:.2f}".format(elapsed_time))

    spark.close()  
