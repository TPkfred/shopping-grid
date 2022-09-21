"""
Utility functions for analyzing market volume in a large spark dataframe.

We must generate the summary data (i.e. histogram, CDF) in Spark before
sending to Pandas for plotting
"""

from functools import reduce
import datetime
from calendar import Calendar
import argparse

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# for notebook
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", 100)


# load data into df
# define `num_shop_days`
# shopping volume column = "num_shops"


market_cnts = df.groupBy("market").agg(F.sum('num_shops').alias("sum_shop_counts"))
market_cnts = market_cnts.withColumn("avg_counts_per_day", F.col("sum_shop_counts") / num_shop_days)


# ============================
# HISTOGRAM
# ============================
# count of markets with a given volume

w = Window.orderBy("avg_counts_per_day")
w2 = Window.orderBy("rounded_log_counts")

output = (market_cnts
          .withColumn("cume_proba", F.percent_rank().over(w))
          .withColumn("log_avg_counts", F.log10(F.col("avg_counts_per_day")))
          .withColumn("rounded_log_counts", F.round(F.col("log_avg_counts"), 1))
          .groupBy("rounded_log_counts")
          .agg(
              F.max("cume_proba").alias("cume_proba"),
              F.count('*').alias("data_count")
          )
          .withColumn("cume_proba", F.lead(F.col("cume_proba")).over(w2))
          .fillna(1, subset=["cume_proba"])
          .withColumn("avg_counts", F.pow(F.col("rounded_log_counts"), F.lit(10)))
         )

pdf_cdf = output.toPandas()

plt.bar(pdf_cdf['rounded_log_counts'], pdf_cdf['data_count']);
plt.title("Histogram")
plt.xlabel("log10(avg shops / day)");
plt.ylabel("num markets");

# ============================
# CDF
# ============================
# "reverse" cumulative percentage of volume
# markets sorted by volume in descending order

market_cnts = df.groupBy("market").agg(F.sum('num_shops').alias("sum_shop_counts"))
market_cnts = market_cnts.withColumn("avg_counts_per_day", F.col("sum_shop_counts") / num_shop_days)

w = Window.partitionBy()

market_cnts = market_cnts.withColumn("pct_total_shops",
                                    F.col("sum_shop_counts") / F.sum("sum_shop_counts").over(w))

w2 = (Window
      .orderBy("avg_counts_per_day")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
     )
market_cnts = market_cnts.withColumn("cume_pct",
                                    F.sum("pct_total_shops").over(w2))

w3 = (Window
      .orderBy(F.desc("avg_counts_per_day"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
     )
market_cnts = market_cnts.withColumn("rev_cume_pct",
                                    F.sum("pct_total_shops").over(w3))

w_ord = Window.orderBy(F.desc("sum_shop_counts"))
market_cnts = market_cnts.withColumn("rank", F.rank().over(w_ord))

pdf_out2 = market_cnts.toPandas()

# plot
plt.plot(pdf_out2['rank'], pdf_out2['rev_cume_pct'])
plt.xlabel('market')
plt.ylabel("cumulative pct of total shops");
plt.title("CDF by market rank");

# print-outs
vol_pct = 0.8
rank = pdf_out2[pdf_out2['rev_cume_pct'] > vol_pct].head(1)['rank'].values[0]
rank_pct = rank / len(pdf_out2)
print(f"The top {rank_pct*100:.1f}% of markets account for {vol_pct*100:.0f}% of the shopping volume")

rank = 1000
vol_pct = pdf_out2[pdf_out2['rank'] == rank]['rev_cume_pct'].values[0]
print(f"The top {rank} markets account for {vol_pct*100:.0f}% of the shopping volume")