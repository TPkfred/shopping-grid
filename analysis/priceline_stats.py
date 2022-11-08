
import argparse
import datetime
import random

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.window import Window
from pyspark.sql import Row


APP_NAME = "KF-EDA"

BASE_DATA_DIR = "s3://tvlp-ds-air-shopping-pn/v1_5"
BASE_OUTPUT_DIR = "s3://tvlp-ds-users/kendra-frederick/tmp/priceline-eda/"
AIRPORT_LOOKUP_PATH = "s3://tvlp-ds-users/kendra-frederick/reference-data/AIRPORT.CSV"

uniq_key_cols = ["id", "originalRequest", "shop_req_timeStamp", "gds", "pcc"]

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--shop-start",
        help="Shopping start date, of format YYYY-MM-DD",
        type=str,
        required=True
    )
    parser.add_argument(
        "--num-days", "-n",
        help="Number of days of shopping data to analyze",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--test", "-t",
        help="Run in test (sample) mode",
        default=False,
        action="store_true"
    )
    args = parser.parse_args()
    return args


def load_ref_data(spark):
    airport_df = spark.read.option("header", "true").csv(AIRPORT_LOOKUP_PATH)
    pl_pccs_df = spark.read.csv("s3://tvlp-ds-users/kendra-frederick/reference-data/priceline_PCCs_for_kendra.csv", header=True)
    return airport_df, pl_pccs_df


def count_searches(df):
    return df.select(uniq_key_cols).distinct().count()


def convert_dates_to_features(df, date, spark):
    # convert dates (which are ints) to datetime
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(out_departure_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(in_departure_date AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt
        FROM data
    """)

    df = df.withColumn("searchDt_dt", F.lit(date))

    # do date math
    df = (df
        .withColumn('days_til_dept', F.datediff(
                        F.col('outDeptDt_dt'), F.col('searchDt_dt')))
        .withColumn('stay_duration', F.datediff(
                        F.col('inDeptDt_dt'), F.col('outDeptDt_dt')))
    )
    return df


def calc_market_stats(df, save_path):
    market_stats = (df
                    .groupBy("market")
                    .agg(
                        # note id alone does not identify a unique search
                        F.countDistinct(*uniq_key_cols).alias("num_searches"),
                        F.count("id").alias("num_solutions"), # could count any column, really
                    )
                    )#.cache()

    market_stats = (market_stats
                    .withColumn("search_rank", F.row_number().over(Window.partitionBy().orderBy(F.desc("num_searches"))))
                    .withColumn("solution_rank", F.row_number().over(Window.partitionBy().orderBy(F.desc("num_solutions"))))
                    )

    w = Window.partitionBy()
    w2 = Window.partitionBy().orderBy("search_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    w3 = Window.partitionBy().orderBy("solution_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    market_stats = (market_stats
                    .withColumn("pct_searches", F.col("num_searches") / F.sum("num_searches").over(w))
                    .withColumn("pct_solutions", F.col("num_solutions") / F.sum("num_solutions").over(w))
                    .withColumn("cum_pct_searches", F.sum("pct_searches").over(w2))
                    .withColumn("cum_pct_solutions", F.sum("pct_solutions").over(w3))
                    )
    print("Saving market stats")
    market_stats.coalesce(1).write.mode("overwrite").csv(save_path, header=True)
    return market_stats.count()


def calc_time_stats(df, time_col, save_path):
    time_stats = df.groupBy(time_col).agg(
        # F.countDistinct("market").alias("num_markets"),
        F.countDistinct(*uniq_key_cols).alias("num_searches"),
        F.count("id").alias("num_solutions"), # could count any column, really
    )
    time_stats = time_stats.orderBy(time_col)

    w = Window.partitionBy()
    w2 = Window.partitionBy().orderBy(time_col).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    time_stats = (time_stats
                .withColumn("pct_searches", F.col("num_searches") / F.sum("num_searches").over(w))
                .withColumn("pct_solutions", F.col("num_solutions") / F.sum("num_solutions").over(w))
                .withColumn("cum_pct_searches", F.sum("pct_searches").over(w2))
                .withColumn("cum_pct_solutions", F.sum("pct_solutions").over(w2))
                )
    print("Saving {} stats".format(time_col))
    time_stats.coalesce(1).write.mode("overwrite").csv(save_path, header=True)


def load_analyze_data(spark, date, test):

    date_str = date.strftime("%Y%m%d")
    day_output_dir = "{}/{}".format(BASE_OUTPUT_DIR, date_str)
    print("Saving to {}".format(day_output_dir))

    # job 0, 1 (csv, csv)
    airport_df, pl_pccs_df = load_ref_data(spark)

    if test:
        hour = 12
        print("Working on {}, hour = {}".format(date_str, hour))
        input_dir = "{}/year={}/month={}/day={}/hour={:02d}".format(BASE_DATA_DIR, date.year, date.month, date_str, hour)
    else:
        hour = -1
        print("Working on {}".format(date_str, hour))
        input_dir = "{}/year={}/month={}/day={}/".format(BASE_DATA_DIR, date.year, date.month, date_str)

    # job 2 (parquet)
    df = spark.read.parquet(input_dir)

    # restrict to US for now
    pos = 'US'
    currency = 'USD'
    df_us = df.filter(
            (F.col("point_of_sale") == pos) & (F.col("currency") == currency)
        )
    # geo-enrich
    df_us = (df_us
                    .join(
                        airport_df.select('airport_code', 'country_code'),
                        on=[df_us['out_origin_city'] == airport_df['airport_code']])
                    .withColumnRenamed("country_code", "origin_country")
                    .drop("airport_code")
                )
    df_us = (df_us
                .join(
                    airport_df.select('airport_code', 'country_code'),
                    on=[df_us['out_destination_city'] == airport_df['airport_code']])
                .withColumnRenamed("country_code", "destination_country")
                .drop("airport_code")
            )

    # filter on O or D in POS
    df_us = df_us.filter(
            (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
        )
    
    ## job 4 (count)
    c_all_us = df_us.count() 
    print("Total num US solutions: ", c_all_us)

    # add columns to assist with joining and grouping
    df_us = (df_us
        .withColumn("market", F.concat_ws("-", F.col("out_origin_airport"), F.col("out_destination_airport")))
        .withColumn("gds_pcc", F.concat_ws("_", F.col("gds"), F.col("pcc")))
        )

    df_us = df_us.repartition(200)

    # convert dates
    df_us = convert_dates_to_features(df_us, date, spark)

    # filter on Priceline PCCs
    df_pl = df_us.join(pl_pccs_df.select('CRS_PCC'), 
                       on=[df_us['gds_pcc'] == pl_pccs_df['CRS_PCC']])
    ## job 5 (csv)
    c_pl = df_pl.count()
    print("Total num Priceline-only solutions: ", c_pl)

    # compute stats
    pl_only_save_dir = "{}/priceline-only-data".format(day_output_dir)
    # job 9? 10? 11? 12 (csv)
    print("Calculating market stats")
    m_pl = calc_market_stats(df_pl, 
        "{}/market-stats/".format(pl_only_save_dir)
    )
    print("Total number of Priceline markets: ", m_pl)
    ## job 13? (csv)
    print("Calculating DTD stats")
    calc_time_stats(df_pl, "days_til_dept",
        "{}/dtd-stats/".format(pl_only_save_dir)
    )
    print("Calculating LOS stats")
    calc_time_stats(df_pl, "stay_duration",
        "{}/los-stats/".format(pl_only_save_dir)
    )

    # all data for Priceline markets
    pl_markets = df_pl.select("market").distinct()
    df_pl_markets = df_us.join(pl_markets, on="market")
    c_plm = df_pl_markets.count()
    print("Total num Priceline market solutions: ", c_plm)

    pl_markets_save_dir = "{}/priceline-markets-all-data".format(day_output_dir)
    print("Calculating market stats")
    _ = calc_market_stats(df_pl_markets, 
        "{}/market-stats/".format(pl_markets_save_dir)
    )
    print("Calculating DTD stats")
    calc_time_stats(df_pl_markets, "days_til_dept",
        "{}/dtd-stats/".format(pl_markets_save_dir)
    )
    print("Calculating LOS stats")
    calc_time_stats(df_pl_markets, "stay_duration",
        "{}/los-stats/".format(pl_markets_save_dir)
    )

    # summary data

    # row = Row(date=date, hour=hour, 
    #     us_markets= m_us_all, 
    #     us_searches=count_searches(df_us),
    #     us_solutions=c_all_us, 
    #     pl_markets=m_pl,
    #     pl_only_searches=count_searches(df_pl), 
    #     pl_only_solutions=c_pl,
    #     plm_all_data_searches=count_searches(df_pl_markets), 
    #     plm_all_data_solutions=c_plm
    #     )
    m_us_all = df_us.select("market").distinct().count()
    print("Number of US markets: ", m_us_all)
    s_us = count_searches(df_us)
    print("Number of US searches: ", s_us)

    s_pl = count_searches(df_pl)
    print("Number of Priceline searches: ", s_pl)

    s_plm = count_searches(df_pl_markets)
    print("Number of Priceline market searches: ", s_plm)

    row_us = Row(date=date, hour=hour,
        subset="US",
        num_markets= m_us_all, 
        num_searches=s_us,
        num_solutions=c_all_us
    )
    row_pl = Row(date=date, hour=hour,
        subset="Priceline-only",
        num_markets= m_pl, 
        num_searches=s_pl,
        num_solutions=c_pl
    )
    row_plm = Row(date=date, hour=hour,
        subset="Priceline markets - all data",
        num_markets= m_pl, 
        num_searches=s_plm,
        num_solutions=c_plm
    )

    res_df = spark.createDataFrame([row_us, row_pl, row_plm])
    (res_df
        .write
        .mode("overwrite")
        .csv("{}/summary/".format(day_output_dir), header=True)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    args = parse_args()
    start_dt_str = args.shop_start
    start_dt = datetime.datetime.strptime(start_dt_str, "%Y-%m-%d")
    num_days = args.num_days
    date_list = [start_dt + datetime.timedelta(days=x) for x in range(num_days)]

    for date in date_list:
        load_analyze_data(spark, date, args.test)

    spark.stop()