"""
To run:

spark-submit create_date_df.py --shop-start 2022-08-01 --shop-end 2022-08-31 -dtd 120 --missing-dates 2022-08-15 2022-08-16 2022-08-17 2022-08-18 -o /user/kendra.frederick/shop_grid/dates_cross_shop_aug_120dtd

spark-submit create_date_df.py --shop-start 2022-08-25 --shop-end 2022-09-16 -dtd 120 -o /user/kendra.frederick/shop_grid/date_enums/date_cross_2022-09-19


"""
import datetime
import argparse

# =====================
# PARSE ARGS & DEFINE VARS
# =====================

parser = argparse.ArgumentParser()
parser.add_argument(
    "--shop-start",
    help="Shopping start date, of format YYYY-MM-DD",
    type=str,
    required=True
)
parser.add_argument(
    "--shop-end",
    help="Shopping end date (inclusive), of format YYYY-MM-DD",
    type=str,
    required=True
)
parser.add_argument(
    "-dtd", "--days-til-dept",
    help="Max days until departure",
    type=int,
    required=True,
    default=60
)
parser.add_argument(
    "-los", "--length-of-stay",
    help="Max length of stay (stay duration)",
    type=int,
    required=False,
    default=30
)
parser.add_argument(
    "--missing-dates",
    help="Space-delimited list of missing dates, in format YYYY-MM-DD",
    type=str,
    nargs='*',
    required=False
)
parser.add_argument(
    "-o", "--output",
    help="Output path to save date dataframe to",
    type=str,
    required=False,
)
args = parser.parse_args()


# assign args to vars
max_days_til_dept = args.days_til_dept
max_stay_duration = args.length_of_stay

# shop_start_str = "2022-08-01" 
# shop_end_str = "2022-08-25" 
shop_start_str = args.shop_start
shop_end_str = args.shop_end

# missing_search_days_str = ["2022-08-15", "2022-08-16", "2022-08-17", "2022-08-18"]
missing_search_days_str = args.missing_dates or []

# vars that could be params
if args.output is None:
    out_dir = "/user/kendra.frederick/shop_grid/"
    file_name = "date-cross_{}".format(max_days_til_dept)
    out_path = out_dir + file_name
else:
    out_path = args.output

# =====================
# SET UP SPARK
# =====================
APP_NAME = "KF-date-enum"
# conf = pyspark.SparkConf().setAll(
#     [('spark.master','yarn'),
#     ('spark.app.name', APP_NAME),
#     ('spark.driver.memory','30g'),
#     ('spark.executor.memory', '20g'),
#     ('spark.executor.instances', 10),
#     ('spark.executor.cores', '5'),

#     ])
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

# must run in spark-submit mode
# import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# =====================
# DO THE DATES
# =====================

# SEARCH DATES
search_start = datetime.datetime.strptime(shop_start_str, "%Y-%m-%d").date()
search_end = datetime.datetime.strptime(shop_end_str, "%Y-%m-%d").date()
num_search_days = (search_end - search_start).days + 1

missing_search_days_dt = [datetime.datetime.strptime(x, "%Y-%m-%d").date() 
                          for x in missing_search_days_str]
search_date_list = [search_start + datetime.timedelta(days=x) for x in range(num_search_days)]
search_date_list_filt = [x for x in search_date_list if x not in missing_search_days_dt]

search_date_data = [(x, ) for x in search_date_list_filt]
search_date_df = spark.createDataFrame(
    data=search_date_data, schema=["searchDt_dt"]
)
search_date_df = search_date_df.coalesce(1)

# STAY DURATION
stay_duration_data = [(x, ) for x in range(max_stay_duration)]
stay_dur_df = spark.createDataFrame(
    data=stay_duration_data, schema=["stay_duration"])

# OUTBOUND DEPARTURE DATES
min_dept_dt = search_start #+ datetime.timedelta(days=1) # filters out same-day searches; include them?
max_dept_dt = search_end + datetime.timedelta(days=max_days_til_dept)
max_return_dt = max_dept_dt + datetime.timedelta(days=max_stay_duration)

num_days = (max_dept_dt - min_dept_dt).days
dept_date_list = [min_dept_dt + datetime.timedelta(days=x) for x in range(num_days)]
dept_date_data = [(x, ) for x in dept_date_list]
dept_date_df = spark.createDataFrame(
    data=dept_date_data, schema=["outDeptDt_dt"])
dept_date_df = dept_date_df.coalesce(1)

# CROSS-JOIN
cross_df1 = (dept_date_df.crossJoin(stay_dur_df))
date_cross_df = (cross_df1.crossJoin(search_date_df))
date_cross_df = date_cross_df.repartition(6, "searchDt_dt")

print("Saving to: {}".format(out_path))
date_cross_df.write.mode("overwrite").parquet(out_path)