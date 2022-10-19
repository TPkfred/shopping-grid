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

import datetime
import argparse
from functools import reduce

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.column import Column, _to_java_column, _to_seq


APP_NAME = "KF-ProcessAggdShopData"

# This is the current input directory.
# We load hard-coded older ones below.
input_dir = "/user/kendra.frederick/shop_vol/v8/raw"
output_dir = "/user/kendra.frederick/shop_grid/"

DATE_FORMAT = "%Y-%m-%d"


# ========================
# PARSING ARGUMENTS
# ========================
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
    default=13,
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
    default=60,
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

# ========================
# START UP
# ========================

script_start_time = datetime.datetime.now()
print("*****************************")
print("{} - Starting Preprocessing Script".format(script_start_time.strftime("%Y-%m-%d %H:%M")))
# print("Processing shop data from {} to {} (inclusive)".format
#     (shop_start_str, shop_end_str)
# )
# TODO: log other params (min/max stay duration, etc)
# print("Processing POS {}, currency {}".format(pos, currency))
print("Script arguments / params: {}".format(args))
print("Saving processed data to: {}".format(output_dir))



# import sys
# sys.exit(0)

# SET UP SPARK
# -----------------------
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# ========================
# HELPER FUNCTIONS
# ========================

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
    # convert dates (which are ints) to datetime
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(outDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(inDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(searchDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS searchDt_dt
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
        (F.col("pos") == pos) & (F.col("currency") == currency)
    )

    # filter on org or dest in country of POS
    pos_df = (pos_df
                    .join(
                        airport_df.select('airport_code', 'country_code', 'reference_city_code'),
                        on=[pos_df['origin'] == airport_df['airport_code']],)
              .withColumnRenamed("country_code", "origin_country")
              .withColumnRenamed("reference_city_code", "origin_city")
                    .drop("airport_code")
                    )
    pos_df = (pos_df
                    .join(
                        airport_df.select('airport_code', 'country_code', 'reference_city_code'),
                        on=[pos_df['destination'] == airport_df['airport_code']],
                    ).withColumnRenamed("country_code", "destination_country")
              .withColumnRenamed("reference_city_code", "destination_city")
                    .drop("airport_code")
                    )

    pos_df = pos_df.filter(
        (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
    )
    return pos_df

# ========================
# LOAD DATA
# ========================

print("Reading data")

# lookup
airport_df = spark.read.csv("/user/contentoptimization/reference/content_manager/AIRPORT_FULL.CSV", header=True)


# AGGREGATED ESTREAMING DATA
# ----------------------------

# TODO: make date loading moar smarter?
# for now we just load in chunks by source & filter

# "v7" 
## thru 09/29/2022.
## has been decoded

# v7 original format
## in this format, all aggregated data was loaded at once, decoded,
## and partitioned by search date when saving. Ex:
## /user/kendra.frederick/shop_vol/v7/decoded/searchDt=20220901
## Shopping data from 08/30/2022 - 09/20/2022 are saved in this format
cov_df1 = None
cov_df2 = None
cov_df_list = []

# could also generate a set of dates covered by this data source, and
# check the intersection with `shop_date_range`, but this works
if shop_start_dt <= datetime.date(2022,9,20):
    input_dir1 = "/user/kendra.frederick/shop_vol/v7/decoded/"
    cov_df1 = spark.read.parquet(input_dir1).filter(F.col("searchDt").isin(shop_int_range))

# v7 new format
## in this format, we decoded aggregated data one day at a time, and saved
## to a corresponding folder based on the date. Ex:
## /user/kendra.frederick/shop_vol/v7/decoded_new_format/20220921
## Shopping data from 09/21/2022 - 09/29/2022 are saved in this format

v7_new_date_range = {datetime.date(2022,9,21) + datetime.timedelta(days=d) for d in range(9)}

if len(shop_date_range.intersection(v7_new_date_range)) > 0:
    input_dir2 = "/user/kendra.frederick/shop_vol/v7/decoded_new_format/*"
    cov_df2 = spark.read.parquet(input_dir2).filter(F.col("searchDt").isin(shop_str_range))
    cov_df2 = cov_df2.withColumn("searchDt", F.col("searchDt").cast(T.IntegerType()))
    cov_df_list.append(cov_df2)


# current ("v8")
## Shopping data from 09/30/2022 onwards are saved in this format / location.
## They have not been decoded, so we do so here.

if shop_end_dt >= datetime.date(2022,9,30):
    cov_df3 = spark.read.parquet(input_dir + "/*")
    cov_df3 = cov_df3.withColumn("searchDt", F.col("searchDt").cast(T.IntegerType()))

    # decode
    sc = spark.sparkContext
    numToStringUDF = sc._jvm.com.tvlp.cco.util.Utils.__getattr__('UDFs$').__getattr__('MODULE$').numToStringUDF
    
    cov_df3 = (cov_df3
        .withColumn("origin", 
            Column(numToStringUDF().apply(_to_seq(sc, ["outOriginAirport"], _to_java_column))))
        .withColumn("destination",
            Column(numToStringUDF().apply(_to_seq(sc, ["outDestinationAirport"], _to_java_column))))
        .withColumn("pos_decoded",
            Column(numToStringUDF().apply(_to_seq(sc, ["pos"], _to_java_column))))
        .withColumn("currency_decoded",
            Column(numToStringUDF().apply(_to_seq(sc, ["currency"], _to_java_column))))
    )

    cov_df3 = (cov_df3
        .drop("outOriginAirport", "outDestinationAirport", "pos", "currency")
        .withColumn("market", F.concat_ws("-", F.col("origin"), F.col("destination")))
        .withColumnRenamed("pos_decoded", "pos")
        .withColumnRenamed("currency_decoded", "currency")
        # reorder so it looks nice
        .select("market",  "origin", "destination", "round_trip",
            "pos", "currency", 
            "outDeptDt", "inDeptDt", "searchDt", 
            "shop_counts", "min_fare"
        )
    )
    cov_df_list.append(cov_df3)

# column order must be the same in order to `union` df's
# Note: we don't need to check on cov_df3, because it's not possible
# for df1 to be not None, df2 to be None, and df3 to be not None
if (cov_df1 is not None) & (cov_df2 is not None):
    cols_in_order = cov_df2.columns
    cov_df1 = cov_df1.select(cols_in_order)
    cov_df_list.append(cov_df1)
elif cov_df1 is not None:
    cov_df_list.append(cov_df1)

# combine
cov_df = reduce(lambda df1, df2: df1.union(df2), cov_df_list)


# ========================
# PROCESS DATA
# ========================
print("Processing data")
pos_df = filter_pos(cov_df, pos, currency)
proc_df = preprocess_data(pos_df, min_days_til_dept, max_days_til_dept,
    min_stay_duration, max_stay_duration)

print("Writing data")
output_path = "{}/{}-{}".format(output_dir, pos, currency)
if test:
    output_path += "-test"
(proc_df.repartition(1)
    .write.partitionBy("searchDt")
    .mode(write_mode).parquet(output_path)
)

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60
print("Done with preprocessing - Total elapsed time: {:.02f} minutes".format(elapsed_time))
print("*****************************")
print("")