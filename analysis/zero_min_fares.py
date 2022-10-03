
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 

import datetime
import argparse

APP_NAME = "KF-InvestigateZeroFares"
data_dir = "/data/estreaming/midt_1_5/"
out_dir = "/user/kendra.frederick/tmp/lax-ewr_0915"

date_fmt = "%Y-%m-%d %H:%M"


def get_now_time():
    return datetime.datetime.now().strftime(date_fmt)

script_start_time = datetime.datetime.now()
print("{} - Starting script".format(script_start_time.strftime(date_fmt)))

# ========================
# SETUP SPARK

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# ========================

# val us_enc = stringToNum("US")
us_enc = 353566720
# val usd_enc = stringToNum("USD")
usd_enc = 353567744
# val lax_enc = stringToNum("LAX")
lax_enc= 201398272
# val ewr_enc = stringToNum("EWR")
ewr_enc = 85398016

print("{} - Reading data".format(get_now_time))
df = spark.read.parquet("/data/estreaming/midt_1_5/20220915/*")

print("{} - Filtering data".format(get_now_time))
us_df = df.filter((F.col("pos") == us_enc) & (F.col("currency") == usd_enc))

market_df = us_df.filter(
    (F.col("outOriginAirport") == lax_enc) 
    & (F.col("outDestinationAirport") == ewr_enc)
    & (F.col("inOriginAirport") == ewr_enc) 
    & (F.col("inDestinationAirport") == lax_enc)
)

example_df = market_df.filter(
    (F.col("outDeptDt") == 20221108) & (F.col("inDeptDt") == 20221111)
)
# print("Repartitioning")
# example_df = example_df.repartition(120)
print("Number of examples: {}".format(example_df.count()))


print("{} - Exploding".format(get_now_time))
df_expl = (example_df
    .withColumn("PTC", F.explode("responsePTC"))
    .withColumn("farePTC", F.explode("fareBreakDownByPTC"))
    )
df_adt = df_expl.filter(F.col("PTC") == "ADT")
# df_adt = df_adt.repartition(20).cache()

print("{} - Saving".format(get_now_time))
df_adt.coalesce(1).write.mode("overwrite").parquet(out_dir)
print("{} - Done Saving".format(get_now_time))

print("Zero fares:")
df_adt.filter(F.col("farePTC") == 0).count()
df_adt.filter(F.col("farePTC") == 0).show()

script_end_time = datetime.datetime.now()
elapsed_time = (script_end_time - script_start_time).total_seconds() / 60   
print("Total elapsed time: {:.02f} minutes".format(elapsed_time))