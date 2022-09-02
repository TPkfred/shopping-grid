// note: must launch spark-shell with jars:
// 
spark-shell --jars /projects/apps/cco/estreamingTransformerStream/bin/estreammidtmerger_2.11-1.0.jar


// ====================================
// ENCODE
// ====================================

import com.tvlp.cco.util.Utils.UDFs._
val hdfs_dir = "/user/kendra.frederick/lookups/top_markets_2021.csv"
val df = spark.read.option("header", "true").csv(hdfs_dir)


val df2 = (df.withColumn("OriginAirport", (stringToNumUDF(col("origin"))))
    .withColumn("DestinationAirport", (stringToNumUDF(col("dest"))))
    )

df2.write.option("header", "true").csv("/user/kendra.frederick/lookups/top_markets_encoded_2021.csv")


// -----------------------------
// markets text file
import com.tvlp.cco.util.Utils.UDFs._

val hdfs_dir = "/user/kendra.frederick/lookups/markets.txt"
val text_df = spark.read.text(hdfs_dir)

text_df.registerTempTable("data")

val text_df2 = spark.sql("SELECT * FROM data WHERE value NOT LIKE '#%'")

val text_df3 = (text_df2
    .withColumn("org_dst_array", split(col("value"), "-"))
    .withColumn("org", col("org_dst_array").getItem(0))
    .withColumn("dst", col("org_dst_array").getItem(1))
    .withColumnRenamed("value", "market_key_encoded")
)

val text_df4 = (text_df3
    .withColumn("OriginAirport", (stringToNumUDF(col("org"))))
    .withColumn("DestinationAirport", (stringToNumUDF(col("dst"))))
    )

text_df4.drop(col"org_dst_array").write.option("header", "true").csv(
    "/user/kendra.frederick/shop_grid/markets.csv"
    )

text_df4.write.option("header", "true").csv("/user/kendra.frederick/shop_grid/markets.csv")

// -----------------------------
// destinations text file
import com.tvlp.cco.util.Utils.UDFs._

val hdfs_dir = "/user/kendra.frederick/lookups/destinations.txt"
val text_df = spark.read.text(hdfs_dir)

text_df.registerTempTable("data")

val text_df2 = spark.sql("SELECT * FROM data WHERE value NOT LIKE '#%'")

val text_df3 = (text_df2
    .withColumnRenamed("value", "dst")
    .withColumn("DestinationAirport", (stringToNumUDF(col("dst"))))
    )

text_df3.write.option("header", "true").csv("/user/kendra.frederick/shop_grid/destinations.csv")


// ====================================
// DECODE
// ====================================

// MERGED

import com.tvlp.cco.util.Utils.UDFs._
import com.tvlp.cco.util.Utils.QueryHelpers._

val in_dir = "/user/kendra.frederick/min_fare/v5"

val df = spark.read.parquet(in_dir)

val df2 = (df.withColumn("Origin_decoded", (numToStringUDF(col("s_outOriginAirport"))))
    .withColumn("Dest_decoded", (numToStringUDF(col("s_outDestinationAirport"))))
    .withColumn("outMrktCxr_decoded", (numToStringUDF(col("outMrktCxr_single"))))
    .withColumn("inMrktCxr_decoded", (numToStringUDF(col("inMrktCxr_single"))))
    .withColumn("pos_decoded", (numToStringUDF(col("pos"))))
    )

val out_dir = "/user/kendra.frederick/min_fare/v5_encoded/"
df2.write.mode("overwrite").parquet(out_dir)


// ====================================
// ESTREAM
import com.tvlp.cco.util.Utils.UDFs._
import com.tvlp.cco.util.Utils.QueryHelpers._

val in_dir = "/user/kendra.frederick/shop_vol/raw/v2/markets"
val df = spark.read.parquet(in_dir)
// counts: 1,950,243

val df2 = (df.withColumn("origin_decoded", (numToStringUDF(col("outOriginAirport"))))
    .withColumn("dest_decoded", (numToStringUDF(col("outDestinationAirport"))))
    // .withColumn("outMrktCxr_decoded", (numToStringUDF(col("outMrktCxr_single"))))
    // .withColumn("inMrktCxr_decoded", (numToStringUDF(col("inMrktCxr_single"))))
    // .withColumn("pos_decoded", (numToStringUDF(col("pos"))))
    // .withColumn("currency_decoded", (numToStringUDF(col("currency"))))
    )

val out_dir = "/user/kendra.frederick/shop_vol/encoded/markets/v2"
// df2.repartition(75).write.mode("overwrite").parquet(out_dir)
// will eventually want to partition by month
df2.repartition(1).write.mode("overwrite").parquet(out_dir)


// ----------------------
// SHOPPING GRID - VOL COUNTS - MARKETS
import com.tvlp.cco.util.Utils.UDFs._
import com.tvlp.cco.util.Utils.QueryHelpers._

// the schema changed (i.e. column names) at some point during analysis
// not sure why this manifested just now (8/30) and not before (schema 
// changes happened long before). Regardless (I hope...), this is a 
// work-around
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

val in_dir = "/user/kendra.frederick/shop_vol/raw/v2/markets"
val df = spark.read.parquet(in_dir)

val df2 = df.withColumn(
    "use_me_sol_counts", 
    when('solution_counts.isNull, 'solution_count).otherwise('solution_counts)
    )

val df3 = (df2
    .drop("solution_counts", "solution_count", "tx_counts")
    .withColumnRenamed("use_me_sol_counts", "solution_counts")
    .withColumn("origin_decoded", (numToStringUDF(col("outOriginAirport"))))
    .withColumn("dest_decoded", (numToStringUDF(col("outDestinationAirport"))))
    )

val out_dir = "/user/kendra.frederick/shop_vol/encoded/markets/v2"
// df2.repartition(75).write.mode("overwrite").parquet(out_dir)
// will eventually want to partition by month
df3.repartition(1).write.mode("overwrite").parquet(out_dir)

