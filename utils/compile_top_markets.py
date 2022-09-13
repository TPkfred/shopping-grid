"""
Script to analyze / summarize daily top markets data that has been generated
by the [XXX | `CountTopMarkets` scala | `estream_analysis.py` | OTHER] script

TODO:
- add arguments for:
    - top X (currently hard-coded to 1000)
    - how far back in time to compute top X for

Version history:
- 2022-09-12: created from notebook 08b
"""


from collections import Counter
import pandas as pd

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T



inputPath = "/user/kendra.frederick/lookups/top_markets/top_10000/"
top_markets_path = "/user/kendra.frederick/lookups/top_markets/top_1000_markets.csv"
TOP_N = 1000

if run_mode == "python":
    conf1 = pyspark.SparkConf().setAll([
        ("spark.app.name", APP_NAME),
#         ("spark.master","yarn"),
        ("spark.driver.memory","10g"),
#         ("spark.executor.memory", "10g"),
#         ("spark.executor.instances", 5),
#         ("spark.executor.cores", "5"),
#         ('spark.sql.crossJoin.enabled', True),
#         ('spark.sql.shuffle.partitions', 8) 
        ])
    spark = SparkSession.builder.config(conf = conf1).getOrCreate()
elif run_mode == "spark-submit":
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
else:
    pass


# how to do this in .py file??
!hdfs dfs -ls $inputPath > temp.txt


with open('temp.txt') as file:
    tempList = file.readlines()[1:]

pathList = [x.split(" ")[-1].strip() for x in tempList]
results_dict = {}

for path in pathList:
    date = path.split("/")[-1].rstrip(".csv")
    print(f"Processing {date}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    temp = df.select('market', 'num_unique_shops').collect()
    market_dict = {x['market']: x['num_unique_shops'] for x in temp}
    results_dict[date] = market_dict

marketCounter = Counter()
shopCounter = Counter()
for key, valDict in results_dict.items():
    marketCounter += Counter(valDict.keys())
    shopCounter += Counter(valDict)


dfShop = pd.DataFrame.from_dict(shopCounter, orient='index', columns=['sumShopCounts'])
dfDays = pd.DataFrame.from_dict(marketCounter, orient='index', columns=['numDays'])

dfSummary = dfShop.join(dfDays)
dfSummary.reset_index(inplace=True)
# Chekc this
dfSummary.rename(columns={'index':'market'}, inplace=True)

# SAVE LIST OF TOP N
# Check this - with above
top1000sum = list(dfSummary.sort_values(by='sumShopCounts', ascending=False)[:TOP_N].index)
topData = [(x,) for x in top1000sum]
sparkDfTop = spark.createDataFrame(topData, schema=T.StructType(
    [T.StructField("market_key", T.StringType(), nullable=False)]
                                  ))
sparkDfTop.write.mode("overwrite").csv(top_markets_path, header=True)

# TODO: also save counts with this .csv