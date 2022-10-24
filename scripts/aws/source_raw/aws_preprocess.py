"""
Pre-processes aggregated eStreaming data in support of downstream
processing related to modeling, caching, and analysis.

- filters on POS and currency
    - O or D in POS country
- converts data types
- creates of 'days until departure' (dtd) and 'stay duration' 
    (or length of stay, LOS) features and filters on them


Notes / thoughts:
- In the future, we may want to *not* filter on dtd and LOS here, 
    but rather keep data less restricted
"""

import os
import datetime
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 


APP_NAME = "KF-ProcessAggdShopData"

BASE_INPUT_DIR = "s3://kendra-frederick/shopping-grid/agg-raw-data"
BASE_OUTPUT_DIR = "s3://kendra-frederick/shopping-grid/processed-agg-data"
AIRPORT_LOOKUP_PATH = "s3://kendra-frederick/reference-data/AIRPORT.CSV"

DATE_FORMAT = "%Y-%m-%d"



# ========================
# HELPER FUNCTIONS
# ========================

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--shop-start",
        help="Date of shopping data to begin processing at. "
        "Format YYYY-MM-DD.",
        type=str,
        required=True
    )
    parser.add_argument(
        "--shop-end",
        help="Date of shopping data to end processing at (inclusive). "
        "Format YYYY-MM-DD.",
        type=str,
        required=True
    )
    parser.add_argument(
        "--min-stay-duration",
        help="Minimum length of stay / stay duration",
        type=int,
        default=1,
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
        default=1,
    )
    parser.add_argument(
        "--max-days-til-dept",
        help="Maximum days til departure",
        type=int,
        default=120,
    )
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
        "--test", "-t",
        help="Run in test mode",
        default=False,
        action="store_true"
    )
    parser.add_argument(
        "--write-mode", "-wm",
        help="Write mode (e.g. 'append' or 'overwrite')",
        choices=("append", "overwrite", "ignore"),
        default="append",
    )

    args = parser.parse_args()
    return args


def preprocess_data(df, min_days_til_dept, max_days_til_dept,
    min_stay_duration, max_stay_duration
    ):
    """Preprocesses data
    
    - Computes stay duration and days until departure
    - Filter data on:
        - stay duration between min/max
        - days_til_dept between min/max
        - round trip or one-way intineraries
    """
    df = (df
        .withColumn("market", 
            F.concat_ws("-", F.col("out_origin_airport"), 
                                F.col("out_destination_airport"))
                    )
        .withColumnRenamed("out_origin_airport", "origin")
        .withColumnRenamed("out_destination_airport", "destination")
        .withColumnRenamed("out_origin_city", "origin_city")
        .withColumnRenamed("out_destination_city", "destination_city")
    )
    
    # convert dates (which are ints) to datetime
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(out_departure_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(in_departure_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(search_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS searchDt_dt
        FROM data
    """)
    
    # add days_til_dept column & filter on it
    df_filt = (df
        .withColumn('days_til_dept', F.datediff(
                        F.col('outDeptDt_dt'), F.col('searchDt_dt'))
                    )
        .filter(F.col('days_til_dept').between(
                        min_days_til_dept, max_days_til_dept)
                )
        )

    # add stay_duration column & filter on it
        # note we keep 'Null' values, which correspond to one-ways
    df_filt = (df_filt
        .withColumn('stay_duration', F.datediff(
                        F.col('inDeptDt_dt'), F.col('outDeptDt_dt')))
        .filter(
            (F.col("stay_duration").between(
                min_stay_duration, max_stay_duration))
            | (F.col("stay_duration").isNull()))
    )
    
    # add shop indicator for individual market heatmaps
    df_filt = df_filt.withColumn("shop_ind", F.lit(1))
    
    return df_filt


def filter_pos(df, pos, currency):
    """Filters on pos and currency, and also O & D in pos country.
    
    Also enriches data with O & D city.
    """
    pos_df = df.filter(
        (F.col("point_of_sale") == pos) & (F.col("currency") == currency)
    )

    # filter on org or dest in country of POS
    pos_df = (pos_df
                .join(
                    airport_df.select('airport_code', 'country_code'),
                    on=[pos_df['origin'] == airport_df['airport_code']])
                .withColumnRenamed("country_code", "origin_country")
                .drop("airport_code")
            )
    pos_df = (pos_df
                .join(
                    airport_df.select('airport_code', 'country_code'),
                    on=[pos_df['destination'] == airport_df['airport_code']])
                .withColumnRenamed("country_code", "destination_country")
                .drop("airport_code")
            )

    pos_df = pos_df.filter(
        (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
    )
    return pos_df


def load_process_write_data(date, spark):
    # LOAD AGGREGATED ESTREAMING DATA
    # ----------------------------
    date_str = date.strftime("%Y%m%d")

    # Datalake format is "decoded"
    print("Reading data")
    input_dir = os.path.join(BASE_INPUT_DIR, date_str)
    df = spark.read.parquet(input_dir)

    # PROCESS DATA
    # ----------------------------
    print("Processing data")
    proc_df = preprocess_data(df, min_days_til_dept, max_days_til_dept,
        min_stay_duration, max_stay_duration)
    pos_df = filter_pos(proc_df, pos, currency)

    print("Writing data")
    output_dir = "{}/{}-{}".format(BASE_OUTPUT_DIR, pos, currency)
    if test:
        output_dir += "-test"
    (pos_df
        .repartition(1)
        .write
        .partitionBy("searchDt_dt")
        .mode(write_mode)
        .parquet(output_dir)
    )


if __name__ == "__main__":
    script_start_time = datetime.datetime.now()

    print("*****************************")
    print("{} - Starting Preprocessing Script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
    
    # PARSE ARGS
    # ------------------------
    args = parse_args()
    min_stay_duration = args.min_stay_duration
    # add one to allow for shifted features
    max_stay_duration = args.max_stay_duration + 1 
    min_days_til_dept = args.min_days_til_dept
    # add one to allow for shifted features
    max_days_til_dept = args.max_days_til_dept + 1

    shop_start_str = args.shop_start
    shop_end_str = args.shop_end
    shop_start_dt = datetime.datetime.strptime(shop_start_str, DATE_FORMAT).date()
    shop_end_dt = datetime.datetime.strptime(shop_end_str, DATE_FORMAT).date()
    num_shop_days = (shop_end_dt - shop_start_dt).days
    shop_date_range = {shop_start_dt + datetime.timedelta(days=d) for d in range(num_shop_days)}
    shop_str_range = [d.strftime("%Y%m%d") for d in shop_date_range]
    shop_int_range = [int(s) for s in shop_str_range]

    pos = args.pos
    currency = args.currency

    write_mode = args.write_mode

    test = args.test

    print("Script arguments / params: {}".format(args))
    print("Saving processed data to: {}".format(BASE_OUTPUT_DIR))

    num_days = (shop_end_dt - shop_start_dt).days
    date_list = [shop_start_dt + datetime.timedelta(days=x) for x in range(num_days + 1)]

    # SET UP SPARK
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    # Load lookup table
    airport_df = spark.read.option("header", "true").csv(AIRPORT_LOOKUP_PATH)

    for date in date_list:
        load_process_write_data(date, spark)

    script_end_time = datetime.datetime.now()
    elapsed_time = (script_end_time - script_start_time).total_seconds() / 60
    print("Done with preprocessing - Total elapsed time: {:.02f} minutes".format(elapsed_time))
    print("*****************************")
    print("")
    spark.stop()