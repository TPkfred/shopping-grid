import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


BASE_INPUT_DIR = "s3://tvlp-ds-users/kendra-frederick/shopping-grid/agg-raw-data_poc_v2/*"
BASE_OUTPUT_DIR = f"s3://tvlp-ds-users/kendra-frederick/shopping-grid/engineered-features/parquet/"
AIRPORT_LOOKUP_PATH = "s3://tvlp-ds-users/kendra-frederick/reference-data/AIRPORT.CSV"

# to make args

last_shop_day = "20221117" # used to name output file; could derive from data

# =========================
# HELPER FUNCTIONS
# =========================

# DATA PREP
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


def filter_dtd(df, dtd_cutoff):
    df_filt = df.filter(F.col("days_til_dept") > dtd_cutoff)
    return df_filt


def get_date_range(date_start, date_stop):
    """Return a list of dates between `date_start` and `date_stop`, inclusive."""
    num_days = (date_stop - date_start).days
    date_list = [date_start + datetime.timedelta(days=x) for x in range(num_days + 1)]
    return date_list


def add_holiday_ind(df, holiday_list):
    """
    df: spark DataFrame
    holiday_list: list of date objects
    """
    df_mod = df.withColumn("is_holiday", F.when(F.col("outDeptDt_dt").isin(holiday_list), 1).otherwise(0))
    return df_mod



# FEATURE ENGINEERING
# -----------------------
def eng_avail_features_out(df):
    # for now, collapse the 'any' features into one
    df_mod = (df
              .withColumn("out_avail_any_low", 
                          F.col("out_avail_any_000").cast('int')
                          .bitwiseOR(F.col("out_avail_any_001").cast('int'))
                          .bitwiseOR(F.col("out_avail_any_002").cast('int'))
                         )
              .withColumn("out_avail_all_max", F.col("out_avail_all_009").cast('int'))
             )
    return df_mod


def agg_by_fare_rank(df):
    """Aggregate by fare rank, so there is one row per fare rank.
    Also re-computes day/date-based features.
    Note: Availability features should have already been engineered.

    """
    # for now, take average of avail features. 
        # Considered taking `any` and `bit_and` equivalents. Out-of-the-box functions not
        # available in this version of spark, however.
        # The averages do capture this info, though:
            # if any out_avail_max == 1, average will be non-zero
            # if all out_avail_min == 1, average will be 1
    df_agg = (df
                 .groupBy(['market', 'out_departure_date', 'shop_date', 'fare_rank'])
                 .agg(
                     F.count("*").alias("num_itin"),
                     F.first("fare").alias("fare"),
                     F.collect_set("out_cxr").alias("out_cxrs"),
                     F.first("solution_counts").alias("solution_counts"),
                     F.first("shop_counts").alias("shop_counts"),
                     # F.any("out_avail_all_max").alias("out_avail_all_max"),
                     F.mean("out_avail_all_max").alias("avg_out_avail_max"),
                     # F.bit_and("out_avail_any_low").alias("out_avail_all_low"),
                     F.mean("out_avail_any_low").alias("avg_out_avail_low"),
                 )
                )

    # alt: do this on raw data & then either agg on these above
    df_agg = convert_dates(df_agg, ow=True)
    df_agg = add_date_features(df_agg, ow=True)
    df_agg = add_dow_ind(df_agg, ow=True)
    df_agg = add_holiday_ind(df_agg)
    return df_agg


def collapse_fare_rank_features(df):
    """Shift fare rank features, so we are left with a single row per search criteria.
    
    Collapse 2nd-lowest fare info into lowest-fare row.
    """
    w = (Window.partitionBy("market", "out_departure_date", "shop_date").orderBy("fare_rank"))
    df_mod = (df
                  # note: this temporarily pulls in 3rd-lowest fare to 2nd-lowest row
                  # but when filter on fare_rank == 1 below, these column names become
                  # accurate (could name them "next-lowest" instead of "fr2")
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
    """Target-encode appropriate features.
    Features:
        - days until departure 
    Note: Data should contain a single row per search criteria -- i.e. 'fare'
        column should correspond to the lowest fare (e.g. the target).
    """
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
    """Engineer 'shifted' features based on shop and travel days.
    """
    shifted_features_dict = {
        "shop_day": {
            'sort_col': 'shop_date',
            'groupby_cols': ['market', 'out_departure_date'], #, 'inDeptDt'],
            'target_cols': ['fare', 'fr2_fare', 
                            'avg_out_avail_max', 'avg_out_avail_low',
                            # new in version e:
                            'num_itin', 'shop_counts', 'solution_counts']
        },        
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
    # calc diff for dept day fares
    df_mod = (df_mod
              .withColumn('fare_diff_prev_dept_day', F.col('fare') - F.col('fare_prev_dept_day'))
              .withColumn('fare_diff_next_dept_day', F.col('fare') - F.col('fare_next_dept_day'))
    )
    return df_mod


# new in version e:
def eng_trailing_avg_features(df):
    # 3-day trailing avg of counts and min fare
    w = (Window
         .orderBy("shop_date")
         .partitionBy("market", "out_departure_date")
         .rowsBetween(-4, -1)
        )
    df_mod = (df
              .withColumn("trailing_avg_shop_counts", F.mean("shop_counts").over(w))
              .withColumn("trailing_avg_solution_counts", F.mean("solution_counts").over(w))
              .withColumn("trailing_avg_fare", F.mean("fare").over(w))
              .withColumn("trailing_std_fare", F.stddev("fare").over(w))
             )
    
    # Trailing average (unbounded) of fare diff between prev/next dept day
    days_to_sec = lambda i: i * 24 * 60 *60

    w2 = (Window
         .orderBy(F.col("shopDate_dt").cast("timestamp").cast("long"))
         .partitionBy("market", "dept_dt_dow")
         .rangeBetween(Window.unboundedPreceding, -days_to_sec(1))
        )
    df_mod = (df_mod
              .withColumn("avg_diff_prev_dept_day", F.mean("fare_diff_prev_dept_day").over(w2))
              .withColumn("avg_diff_next_dept_day", F.mean("fare_diff_next_dept_day").over(w2))
             )
    
    # Calc estimated fare from fare & TA of fare diff
    df_mod = (df_mod
              .withColumn("est_fare_from_prev_dept_day",
                          F.col("fare_prev_dept_day") + F.col("avg_diff_prev_dept_day")
                         )
              .withColumn("est_fare_from_next_dept_day",
                          F.col("fare_next_dept_day") + F.col("avg_diff_next_dept_day")
                         )
             )
          
    return df_mod


def engineer_features(df):
    """One function to perform all desired feature engineering"""
    df_mod = eng_avail_features_out(df)
    df_agg = agg_by_fare_rank(df_mod)
    df_min_fares = collapse_fare_rank_features(df_agg)
    df_min_fares = target_encode(df_min_fares)
    df_shift = eng_shifted_day_features(df_min_fares)
    df_trail = eng_trailing_avg_features(df_shift)
    return df_trail

# ========================
# LOAD & PREP DATA
# ========================

df = spark.read.parquet(BASE_INPUT_DIR)

# Note: previous POS + currency filter plus limited markets must essentially do geo-filtering for us. But keep here for best practice
df_pos_filt = filter_pos(df, 'US', 'USD')

df_with_day_feats = convert_dates(df_pos_filt, ow=True)
df_with_day_feats = add_date_features(df_with_day_feats, ow=True)

TDAY_HOL = get_date_range(datetime.date(2022,11,23), datetime.date(2022,11,27))
XMAS_HOL = get_date_range(datetime.date(2022,12,16), datetime.date(2023,1,2))
HOLIDAYS = TDAY_HOL + XMAS_HOL

df_with_hol = add_holiday_ind(df_with_day_feats, HOLIDAYS)

df_dtd_filt = filter_dtd(df_with_hol, 3)

# focus on one-ways for now
df_ow = df_dtd_filt.filter(F.col("round_trip") == 0).cache()

# rename things to be less verbose
df_ow = (df_ow
         .withColumnRenamed("total_shop_counts_day", "shop_counts")
         .withColumnRenamed("total_solution_counts_day", "solution_counts")
        )   

df_ow_fe = engineer_features(df_ow)

out_path = f"{BASE_OUTPUT_DIR}/{last_shop_day}/"
df_ow_fe.coalesce(1).write.mode("overwrite").parquet(out_path) 