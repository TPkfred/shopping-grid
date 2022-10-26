"""
Processes summarized eStreaming / air shopping data and generate
an "output" file of "predictions" to suppor the Lowest Fare Calendar.

- converts data types
- filters on POS, currency, and O or D in POS country
- creates of 'days until departure' (dtd) filters on it
- filters on los (length of stay)

Version notes:
- derived from `aws_preprocess.py` and `aws_generate_output.py`. Combined
    these two scripts into a single one to simplify execution.
"""

import os
import datetime
import argparse
import math

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.window import Window


# ========================
# VARIABLES 
# ========================
APP_NAME = "CalendarPredictions"

BASE_INPUT_DIR = "s3://tvlp-ds-lead-price-archives"
BASE_OUTPUT_DIR = "s3://tvlp-ds-lead-price-predictions"
AIRPORT_LOOKUP_PATH = "s3://tvlp-ds-users/kendra-frederick/reference-data/AIRPORT.CSV"
TEST_DIR = "s3://kendra-frederick/test/calendar-predictions"

DATE_FORMAT = "%Y-%m-%d"
FILE_SIZE_LIMIT_ROWS = 5e5


# ========================
# HELPER FUNCTIONS
# ========================

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pos",
        help="Point of Sale two-letter code",
        default="US",
    )
    parser.add_argument(
        "--currency",
        help="Currency (3-letter code)",
        default="USD",
    )
    parser.add_argument(
        "--stale-after",
        help="Exclude data older than this many days old. "
        "If this param is set to 0, restriction will not be applied. Default = 0",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--min-stay-duration",
        help="Minimum length of stay / stay duration",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--max-stay-duration",
        help="Maximum length of stay (LOS)/ stay duration. Note "
        "this should take into account the grid size. e.g., if the "
        "grid is 7x7 (departure and return date each +/- 3 days) and the max "
        "LOS allowed (difference between departure date & return data) is 7 "
        "days, then the max stay duration = 7 + 3 + 3 = 13",
        type=int,
        default=21,
    )
    parser.add_argument(
        "--min-days-til-dept",
        help="Minimum days til departure",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--max-days-til-dept",
        help="Maximum days til departure",
        type=int,
        default=120,
    )
    # see `aws_generate_output.py` for other data restrictions & filters
    parser.add_argument(
        "--ratio-mean",
        help="Ratio of current value to mean for market. If data is > than  "
        "this value, it will be excluded. If this param is set to 0, filter "
        "will not be applied. Default = 0",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--test", "-t",
        help="Run in test mode",
        default=False,
        action="store_true"
    )

    args = parser.parse_args()
    return args


def load_and_filter(shop_date_first, shop_date_last, spark):
    print("Reading data")
    df = spark.read.parquet(BASE_INPUT_DIR)
    
    df = (df
        .withColumn("shop_date", F.concat_ws("-", F.col("year"), F.col("month"), F.col("day")))
        .withColumn("shop_date_dt", F.to_date("shop_date", "yyyy-MM-dd"))
        .filter(F.col("shop_date").between(shop_date_first, shop_date_last))
    )
    return df


def filter_pos(df, pos, currency):
    """Filters on pos and currency, and also O & D in pos country.
    
    Also enriches data with O & D city.
    """
    pos_df = df.filter(
        (F.col("pos") == pos) & (F.col("currency") == currency)
    )

    # enrich with O, D country
    pos_enr_df = (pos_df
                .join(
                    city_df,
                    on=[pos_df['origin'] == city_df['reference_city_code']],
                    how='left'
                )
                .withColumnRenamed("country_code", "origin_country")
                .drop("reference_city_code")
                )
    pos_enr_df = (pos_enr_df
                .join(
                    city_df,
                    on=[pos_df['destination'] == city_df['reference_city_code']],
                    how='left'
                )
                .withColumnRenamed("country_code", "destination_country")
                .drop("reference_city_code")
                )
    # filter on O or D in country of POS
    pos_enr_filt_df = pos_enr_df.filter(
        (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
    )
    return pos_enr_filt_df


def mod_fields(df):
    """Modifies fields in df
    - creates "market" field (is this needed?)
    - renames 'date' to 'departure_date'
    - convert date strings to datetime objects
    - Computes days until departure
    """
    df_mod = (df
        .withColumn("market", 
            F.concat_ws("-", F.col("origin"), F.col("destination")))
        .withColumnRenamed("date", "departure_date")
        .withColumn("departure_date_dt", F.to_date("departure_date", "yyyy-MM-dd"))
        .withColumn('days_til_dept', F.datediff(
                    F.col('departure_date_dt'), F.col('shop_date_dt'))
                )
    )
    return df_mod
    

def filter_los_dtd(df, 
                  min_days_til_dept, max_days_til_dept,
                  min_stay_duration, max_stay_duration
                  ):
    """
    Filter data on:
        - stay duration between min/max
        - days_til_dept between min/max
    """
    df_filt = (df
                .filter(F.col('days_til_dept').between(
                                min_days_til_dept, max_days_til_dept)
                        )
                .filter((F.col("los").between(
                        min_stay_duration, max_stay_duration))
                        # Summary data does not contain one-way's
                    # | (F.col("los").isNull())
                    )
            )
    
    return df_filt


def filter_ratio_mean(df, ratio_mean):
    """Filter out data based on its ratio to its market's mean lowest fare.

    Only filters out high-fare anomalies.

    Can be used as a stop-gap to eliminate constricted fares.
    """
    w = Window.partitionBy("market")
    df = df.withColumn(
        "mean", F.mean("price").over(w)
    )
    df = df.withColumn(
        "ratio_mean", 
        F.col("price") / F.col("mean")
    )
    df_filt_anom = df.filter(F.col("ratio_mean") < ratio_mean)
    return df_filt_anom


def get_most_recent(df):
    w = (Window
            .partitionBy('market', 'departure_date', 'los')
            .orderBy(F.desc("shop_date_dt"))
            )
    df_with_recency = (df
                    .withColumn("recency_rank", F.row_number().over(w))
                    )
    df_most_recent = df_with_recency.filter(F.col("recency_rank") == 1)

    return df_most_recent


if __name__ == "__main__":
    script_start_time = datetime.datetime.now()

    print("*****************************")
    print("{} - Starting Script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
    
    # PARSE ARGS
    # ------------------------
    args = parse_args()

    pos = args.pos
    currency = args.currency

    stale_after = args.stale_after
    ratio_mean = args.ratio_mean

    min_stay_duration = args.min_stay_duration
    max_stay_duration = args.max_stay_duration
    min_days_til_dept = args.min_days_til_dept
    max_days_til_dept = args.max_days_til_dept

    test = args.test

    print("Script arguments / params: {}".format(args))
    print("Saving processed data to: {}".format(BASE_OUTPUT_DIR))

    # variables derived from args
    if test:
        output_dir = TEST_DIR
    else:
        output_dir = BASE_OUTPUT_DIR
    
    # NOTE: Must only load thru yesterdays' data, otherwise input data
    # will be changing while we try to read & process it
    today = datetime.date.today()
    shop_date_last = today - datetime.timedelta(days=1)
    shop_date_first = today - datetime.timedelta(days=stale_after)

    # partition output dir
    output_dir = "{}/{}-{}_{}".format(output_dir, pos, currency, today.strftime("%Y-%m-%d"))

    # SET UP SPARK
    # ------------------------
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    # LOAD DATA
    # ------------------------
    airport_df = spark.read.option("header", "true").csv(AIRPORT_LOOKUP_PATH)
    city_df = airport_df.select("reference_city_code", "country_code").drop_duplicates()

    df = load_and_filter(shop_date_first, shop_date_last, spark)

    # FILTER & PROCESS DATA
    # ----------------------------
    print("Processing data")
    pos_df = filter_pos(df, pos, currency)
    mod_pos_df = mod_fields(pos_df)
    filt_mod_pos_df = filter_los_dtd(mod_pos_df, min_days_til_dept, 
        max_days_til_dept, min_stay_duration, max_stay_duration)

    df_restr = filt_mod_pos_df.select("*")

    # restrict data, if needed
    if ratio_mean != 0:
        df_restr = filter_ratio_mean(df_restr, ratio_mean)

    # get most recent shop by market
    df_most_recent = get_most_recent(df_restr)
    n = df_most_recent.count()
    print("Output data size: {}".format(n))


    # WRITE OUTPUT
    # ----------------------------
    cols_to_write = [
        'key',
        'pcc',
        'pos',
        'currency',
        'origin',
        'destination',
        'departure_date',
        'los',
        'price'
    ]
    
    # Limit the number of records per file
    num_parts = int(math.ceil(n/FILE_SIZE_LIMIT_ROWS))

    print("Writing data")
    (df_most_recent
        .select(cols_to_write)
        .repartition(num_parts)
        .write
        # .mode("overwrite") # not sure if S3 likes this
        .csv(output_dir, header=True)
    )

    script_end_time = datetime.datetime.now()
    elapsed_time = (script_end_time - script_start_time).total_seconds() / 60
    print("Done with preprocessing - Total elapsed time: {:.02f} minutes".format(elapsed_time))
    print("*****************************")
    print("")
    spark.stop()