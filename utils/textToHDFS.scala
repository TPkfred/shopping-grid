// note: must launch spark-shell with jars:
// spark-shell --jars /projects/apps/cco/estreamingTransformerStream/bin/estreammidtmerger_2.11-1.0.jar

// must upload text file to hdfs using command:
// hdfs dfs -put </local/path/to/file.txt> <hdfs path>

// -----------------------------
// markets text file
import com.tvlp.cco.util.Utils.UDFs._

val hdfs_dir = "/user/kendra.frederick/shop_grid/markets.txt"
val text_df = spark.read.text(hdfs_dir)

text_df.createOrReplaceTempView("data")
val text_df2 = spark.sql("SELECT * FROM data WHERE value NOT LIKE '#%'")

val text_df3 = (text_df2
    .withColumn("org_dst_array", split(col("value"), "-"))
    .withColumn("org", col("org_dst_array").getItem(0))
    .withColumn("dst", col("org_dst_array").getItem(1))
    .withColumnRenamed("value", "market_key_decoded")
)

val text_df4 = (text_df3
    .withColumn("OriginAirport", (stringToNumUDF(col("org"))))
    .withColumn("DestinationAirport", (stringToNumUDF(col("dst"))))
    .drop(col("org_dst_array"))
    )

(text_df4.coalesce(1).write
    .option("header", "true")
    .mode("overwrite")
    .csv("/user/kendra.frederick/shop_grid/markets.csv")
)

// -----------------------------
// destinations text file
import com.tvlp.cco.util.Utils.UDFs._

val input = "destinations"
val hdfs_dir = "/user/kendra.frederick/shop_grid/" + input + ".txt"
val text_df = spark.read.text(hdfs_dir)

text_df.createOrReplaceTempView("data")
val text_df2 = spark.sql("SELECT * FROM data WHERE value NOT LIKE '#%'")

val text_df3 = (text_df2
    .withColumnRenamed("value", "dst")
    .withColumn("DestinationAirport", (stringToNumUDF(col("dst"))))
    )

(text_df3.write
    .option("header", "true")
    .mode("overwrite")
    .csv("/user/kendra.frederick/shop_grid/" + input + ".csv")
)

