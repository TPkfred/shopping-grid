// note: must launch spark-shell with jars:
// spark-shell --jars /projects/apps/cco/estreamingTransformerStream/bin/estreammidtmerger_2.11-1.0.jar


// ====================================
// ENCODE
// ====================================

// top X file derived from daily estreaming counts
import com.tvlp.cco.util.Utils.UDFs._
val hdfs_dir = "/user/kendra.frederick/lookups/top_markets/top_1000_markets.csv"
val out_dir = "/user/kendra.frederick/lookups/top_markets/top_1000_markets_encoded.csv"

val df = spark.read.option("header", "true").csv(hdfs_dir)

val df2 = (df
    .withColumn("org_dst_array", split(col("market_key"), "-"))
    .withColumn("origin", col("org_dst_array").getItem(0))
    .withColumn("dest", col("org_dst_array").getItem(1))
    .withColumnRenamed("market_key", "market")
    .withColumn("OriginAirport", (stringToNumUDF(col("origin"))))
    .withColumn("DestinationAirport", (stringToNumUDF(col("dest"))))
    .withColumn("market_key", concat_ws("-",
        col("OriginAirport"), col("DestinationAirport")))
    )

df2.drop("org_dst_array").write.option("header", "true").csv(out_dir)

// -----------------------------
// 2021 csv
import com.tvlp.cco.util.Utils.UDFs._
val hdfs_dir = "/user/kendra.frederick/lookups/top_markets/top_markets_2021.csv"
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


// ====================================
// ESTREAMING MIDT_1_5 FORMAT
import com.tvlp.cco.util.Utils.UDFs._
import com.tvlp.cco.util.Utils.QueryHelpers._


// note: in v5, we started saving the data by data folder, so format has 
// become nested
// Copy output dir from `estream_analysis` script and append "/*". Ex:
// val in_dir = "/user/kendra.frederick/shop_vol/v6/raw/with_pcc/*"
val input_dir = "/user/kendra.frederick/shop_vol/v7/raw/*"

// ** Be sure to update out_dir below to match

val df = spark.read.parquet(input_dir)
// count: 256417600

val df2 = (df.withColumn("origin", (numToStringUDF(col("outOriginAirport"))))
    .withColumn("destination", (numToStringUDF(col("outDestinationAirport"))))
    .withColumn("pos_decoded", (numToStringUDF(col("pos"))))
    .withColumn("currency_decoded", (numToStringUDF(col("currency"))))
    // .withColumn("pcc_decoded", (longNumToStringUDF(col("pcc"))))
    // .withColumn("outMrktCxr_decoded", (numToStringUDF(col("outMrktCxr_single"))))
    // .withColumn("inMrktCxr_decoded", (numToStringUDF(col("inMrktCxr_single"))))
    )

// drop & rename columns

val df3 = (df2
    .drop("outOriginAirport", "outDestinationAirport", "pos", "currency", "pcc")
    .withColumn("market", concat_ws("-", col("origin"), col("destination")))
    .withColumnRenamed("pos_decoded", "pos")
    .withColumnRenamed("currency_decoded", "currency")
    // .withColumnRenamed("pcc_decoded", "pcc")
    .select("market",  "origin", "destination", "round_trip",
        "pos", "currency", 
        "outDeptDt", "inDeptDt", "searchDt", 
        "shop_counts", "min_fare"
        // , "pcc"
       )
)

// val out_dir = "/user/kendra.frederick/shop_vol/encoded/markets/v2"
// df2.repartition(75).write.mode("overwrite").parquet(out_dir)
// will eventually want to partition by month
// df2.repartition(1).write.mode("overwrite").parquet(out_dir)
val out_dir = "/user/kendra.frederick/shop_vol/v7/decoded/"
df3.write.partitionBy("searchDt").mode("overwrite").parquet(out_dir)


// ====================================
// ESTREAMING MIDT_1_5 FORMAT
// "new" output format
// selectively process only certain date subfolders

import com.tvlp.cco.util.Utils.UDFs._
import com.tvlp.cco.util.Utils.QueryHelpers._
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Period}

// Copy output dir from `estream_analysis` script and append "/*".
// val input_dir = "/user/kendra.frederick/shop_vol/v7/raw/*"
// val out_dir = "/user/kendra.frederick/shop_vol/v7/decoded/"
val base_dir = "/user/kendra.frederick/shop_vol/v7/"
val input_dir = base_dir + "raw/"
val output_dir = base_dir + "decoded_new_format/"

// val coalesceCols = Seq("a", "b")
val startDateStr = "2022-09-21"
val endDateStr = "2022-09-28"
val startDate = LocalDate.parse(startDateStr)
val endDate = LocalDate.parse(endDateStr)
val days = Period.between(startDate, endDate).getDays()

for (i <- 0 to days by 1) {
    var nextDate = startDate.plusDays(i)
    var nextDateStr = nextDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    println("Processing " + nextDateStr)
    val df = spark.read.parquet(input_dir + nextDateStr)
    val df2 = (df.withColumn("origin", (numToStringUDF(col("outOriginAirport"))))
        .withColumn("destination", (numToStringUDF(col("outDestinationAirport"))))
        .withColumn("pos_decoded", (numToStringUDF(col("pos"))))
        .withColumn("currency_decoded", (numToStringUDF(col("currency"))))
        )

    // drop & rename columns
    val df3 = (df2
        .drop("outOriginAirport", "outDestinationAirport", "pos", "currency", "pcc")
        .withColumn("market", concat_ws("-", col("origin"), col("destination")))
        .withColumnRenamed("pos_decoded", "pos")
        .withColumnRenamed("currency_decoded", "currency")
        .select("market",  "origin", "destination", "round_trip",
            "pos", "currency", 
            "outDeptDt", "inDeptDt", "searchDt", 
            "shop_counts", "min_fare"
        )
    )
    // save
    df3.write.mode("overwrite").parquet(output_dir + nextDateStr)
}

println("Done!")
