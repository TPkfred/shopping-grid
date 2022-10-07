// zero-dollar min fares occur across shopping dates. Pick on at random
import com.tvlp.cco.util.Utils.QueryHelpers._

val us_enc = stringToNum("US")
> us_enc: Int = 353566720

val usd_enc = stringToNum("USD")
> usd_enc: Int = 353567744

val lax_enc = stringToNum("LAX")
lax_enc: Int = 201398272

val ewr_enc = stringToNum("EWR")
ewr_enc: Int = 85398016

val df = spark.read.parquet("/data/estreaming/midt_1_5/20220915/*")

val us_df = df.filter((col("pos") === us_enc) && (col("currency") === usd_enc))

val market_df = us_df.filter(
    (col("outOriginAirport") === lax_enc) && (col("outDestinationAirport") === ewr_enc)
    && (col("inOriginAirport") === ewr_enc) && (col("inDestinationAirport") === lax_enc)
)
// count:

val travel_df = market_df.filter(
    (col("outDeptDt") === 20220929) && (col("inDeptDt") === 20221003)
)
// count:  (expecting 776)


val travel_df = market_df.filter(
    (col("outDeptDt") === 20221108) && (col("inDeptDt") === 20221111)
)
// expecting: 4. there were over 20
// that took forever


// run via a script & load
val in_dir = "/user/kendra.frederick/tmp/lax-ewr_0915/"

val df = spark.read.parquet(in_dir)

df.select(
    $"PTC", $"responsePTC", 
    posexplode(col("responsePTC")).alias("ptc_pos", "PTC2")
    ).show(10)

val df2 = df.select("*", 
    posexplode(col("responsePTC")) as Seq("ptc_pos", "PTC2")
    )
// indexes were mis-aligned. sometimes infant fare (which was 0) was
// getting assigned to ADT