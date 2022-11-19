

import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


BASE_INPUT_DIR = "s3://tvlp-ds-users/kendra-frederick/shopping-grid/agg-raw-data_poc_v2/*"
AIRPORT_LOOKUP_PATH = "s3://tvlp-ds-users/kendra-frederick/reference-data/AIRPORT.CSV"

# =============================
# HELPER FUNCTIONS
# =============================

def convert_dates(df, ow=False):
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(out_departure_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(shop_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS shopDate_dt
        FROM data
    """)
    if not ow:
        df = spark.sql("""
            SELECT *,
                TO_DATE(CAST(UNIX_TIMESTAMP(CAST(in_departure_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            FROM data
        """)
    return df


def add_date_features(df, ow=False):
    df_mod = df.withColumn('days_til_dept', F.datediff(
                        F.col('outDeptDt_dt'), F.col('shopDate_dt'))
                    )
    if not ow:
        df_mod = df_mod.withColumn('stay_duration', F.datediff(
                        F.col('inDeptDt_dt'), F.col('outDeptDt_dt'))
                   )
    return df_mod


def add_dow_ind(df, ow=False):
    ## add in DOW indicators
    df_mod = (df
        .withColumn("dept_dt_dow_int", (F.date_format("outDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("dept_dt_dow", F.date_format("outDeptDt_dt", "E"))
    )
    if not ow:
        df_mod = (df_mod
        .withColumn("ret_dt_dow_int", (F.date_format("inDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("ret_dt_dow", F.date_format("inDeptDt_dt", "E"))
        )
    return df_mod


def filter_pos(df, pos, currency):
    airport_df = spark.read.option("header", "true").csv(AIRPORT_LOOKUP_PATH)

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
                    on=[pos_df['out_origin_airport'] == airport_df['airport_code']])
                .withColumnRenamed("country_code", "origin_country")
                .drop("airport_code")
            )
    pos_df = (pos_df
                .join(
                    airport_df.select('airport_code', 'country_code'),
                    on=[pos_df['out_destination_airport'] == airport_df['airport_code']])
                .withColumnRenamed("country_code", "destination_country")
                .drop("airport_code")
            )

    pos_df = pos_df.filter(
        (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
    )
    return pos_df

# =============================
# LOAD AND PREP DATA
# =============================

df = spark.read.parquet(BASE_INPUT_DIR)
df_pos_filt = filter_pos(df, 'US', 'USD')
# do these later?
# df_pos_filt = convert_dates(df_pos_filt)
# df_pos_filt = add_date_features(df_pos_filt)
# df_mod = add_dow_ind(df_pos_filt)

df_ow = df_pos_filt.filter(F.col("round_trip") == 0).cache()

# =============================
# FEATURE ENGINEERING
# =============================
def eng_avail_features_and_agg(df_ow):
    # for now, collapse the 'any' features into one
    df_ow = df_ow.withColumn("out_avail_any_low", 
                             F.col("out_avail_any_000").cast('int').bitwiseOR(F.col("out_avail_any_001").cast('int'))
                             .bitwiseOR(F.col("out_avail_any_002").cast('int'))
                            )

    # convert 009 avail feature to an int
    df_ow = df_ow.withColumn("out_avail_all_max", F.col("out_avail_all_009").cast('int'))

    # for now, take average of avail features. 
        # Considered taking `any` and `bit_and` equivalents. Out-of-the-box functions not
        # available in this version of spark, however.
        # The averages do capture this info, though:
            # if any out_avail_max == 1, average will be non-zero
            # if all out_avail_min == 1, average will be 1
    df_ow_agg = (df_ow
                 .groupBy(['market', 'out_departure_date', 'shop_date', 'fare_rank'])
                 .agg(
                     F.count("*").alias("num_itin"),
                     F.first("fare").alias("fare"),
                     F.collect_set("out_cxr").alias("out_cxrs"),
                     F.first("total_solution_counts_day").alias("total_solution_counts_day"),
                     F.first("total_shop_counts_day").alias("total_shop_counts_day"),
                     # F.any("out_avail_all_max").alias("out_avail_all_max"),
                     F.mean("out_avail_all_max").alias("avg_out_avail_max"),
                     # F.bit_and("out_avail_any_low").alias("out_avail_all_low"),
                     F.mean("out_avail_any_low").alias("avg_out_avail_low"),
                 )
                )

    # alt: do this on raw data & then either agg on these above
    df_ow_agg = convert_dates(df_ow_agg, ow=True)
    df_ow_agg = add_date_features(df_ow_agg, ow=True)
    df_ow_agg = add_dow_ind(df_ow_agg, ow=True)
    return df_ow_agg


def shift_fare_rank_features(df):
    w = (Window.partitionBy("market", "out_departure_date", "shop_date").orderBy("fare_rank"))
    df_mod = (df
                .withColumn("fr2_fare", F.lead("fare").over(w))
                .withColumn("fr2_out_cxrs", F.lead("out_cxrs").over(w))
                .withColumn("fr2_avg_out_avail_max", F.lead("avg_out_avail_max").over(w))
                .withColumn("fr2_avg_out_avail_low", F.lead("avg_out_avail_low").over(w))
                .filter(F.col("fare_rank") == 1)
            )

    df_mod = (df_mod
                .withColumn("fr1_fr2_out_cxrs_same", F.col("out_cxrs") == F.col("fr2_out_cxrs"))
                .withColumn("fr1_fr2_out_cxrs_overlap", F.size(F.array_intersect(F.col("out_cxrs"), F.col("fr2_out_cxrs"))) > 0)
            )
    return df_mod


def target_encode(df):
    # target-encode by dtd
    w3 = (Window
        .partitionBy("market", "days_til_dept")
        .orderBy("shop_date")
        .rowsBetween(Window.unboundedPreceding, -1)
        )
    df_mod = (df
                    .withColumn("avg_fare_dtd", F.mean("fare").over(w3))
                    )
    return df_mod


def eng_shifted_day_features(df):
    shifted_features_dict = {
        # shift shopping day
        "shop_day": {
            'sort_col': 'shop_date',
            'groupby_cols': ['market', 'out_departure_date', ], #'inDeptDt'],
            'target_cols': ['fare', 'fr2_fare', 
                'avg_out_avail_max', 'avg_out_avail_low',
                'num_itin']
        },
        
        # shift departure day
        "dept_day": {
            'sort_col': 'out_departure_date',
            'groupby_cols': ['market', 'shop_date'],
            'target_cols': ['fare']
        },
    }
    df_mod = df.select("*")
    for feature, kwargs in shifted_features_dict.items():
        w = (Window
            .orderBy(kwargs['sort_col'])
            .partitionBy(kwargs['groupby_cols'])
            )

        for target_col in kwargs['target_cols']:
            df_mod = (df_mod
                .withColumn("{}_prev_{}".format(target_col, feature), F.lag(target_col).over(w))
                )
            if feature != 'shop_day':
                df_mod = (df_mod
                .withColumn("{}_next_{}".format(target_col, feature), F.lead(target_col).over(w))
                )
    return df_mod


df_ow = (df_ow
        .withColumnRenamed("total_shop_counts_day", "shop_counts")
        .withColumnRenamed("total_solution_counts_day", "solution_counts")
        )
df_ow_agg = eng_avail_features_and_agg(df_ow)
df_ow_mf = shift_fare_rank_features(df_ow_agg)
df_ow_mf = target_encode(df_ow_mf)
df_ow_shift = eng_shifted_day_features(df_ow_mf)
