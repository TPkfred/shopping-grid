

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

cols_to_write = [
    "id",  'group_id', 'shop_req_timeStamp',
    'pcc', 'gds',
    'point_of_sale', 'currency',
    'constricted_search',
    'robotic_shop_type', 'confidence_level',
    'validating_cxr',
    'fare', 'tax', 'fare_break_down_by_PTC',
    'request_PTC', 'response_PTC',
    'out_origin_airport', 'out_destination_airport',
    'out_departure_date', 'out_departure_time',
    'out_marketing_cxr', 'out_operating_cxr',
    'out_remaining_seats', 
    'out_num_stops', 
    'out_booking_class', 'out_cabin_class',
    'in_origin_airport', 'in_destination_airport', 
    'in_departure_date', 'in_departure_time',
    'in_marketing_cxr', 'in_operating_cxr',
    'in_remaining_seats',
    'in_num_stops',
    'in_booking_class', 'in_cabin_class',
]


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
        "--currency", "-c",
        help="Currency",
        type=str,
        # required=True
        default="USD"
    )
    parser.add_argument(
        "-pos",
        help="Point of Sale",
        type=str,
        # required=True
        default="US"
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


    sd_str = args.shop_date
    dd_str = args.departure_date
    rd_str = args.return_date
    org = args.origin
    dst = args.destination
    currency = args.currency
    pos = args.pos

    shop_date = datetime.datetime.strptime(sd_str, "%Y%m%d")
    dd_int = int(dd_str)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    if test:
        print("Running in test mode -- only loading 1 hour of data")
        hours = [12]
    else:
        hours = range(24)

    for hour in hours:
        loop_start = datetime.datetime.now()
        hdfs_path = "{}/year={}/month={}/day={}/hour={:02d}/".format(
            BASE_INPUT_DIR, shop_date.year, shop_date.month, sd_str, hour
        )
        print("{} - Reading data for hour {}".format(datetime.datetime.now(), hour))
        df = spark.read.parquet(hdfs_path)

        # TODO: add in logic for return info (right now, assume it's null, b/c o-w)
        df_filt = df.filter(
            (F.col("out_origin_airport") == org) 
            & (F.col("out_destination_airport") == dst)
            & (F.col("out_departure_date") == dd_int) 
            & (F.col("in_origin_airport").isNull())
            & (F.col("currency") == currency)
            & (F.col("point_of_sale") == pos)
        ).select(cols_to_write)
        
        print("{} - Writing filtered data".format(datetime.datetime.now()))
        output_path = "{}/{}-{}-dept-{}/{}".format(BASE_OUTPUT_DIR, org, dst, dd_str, sd_str)
        df_filt.coalesce(10).write.mode("append").parquet(output_path)
        
        loop_end = datetime.datetime.now()
        loop_elapsed_time = (loop_end - loop_start).total_seconds() / 60
        print("Done with hour! Loop elasped time: {:.2f}".format(loop_elapsed_time))

    script_end_time = datetime.datetime.now()
    elapsed_time = (script_end_time - script_start_time).total_seconds() / 60
    print("Done with day! Total elasped time: {:.2f}".format(elapsed_time))

    spark.close()  
