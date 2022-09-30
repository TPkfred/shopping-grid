
val data_dir = "/data/estreaming/midt_1_5/20220901/00/"

val df = spark.read.parquet(data_dir)
// count: 1812500000    

val df2 = df.withColumn("pos_decoded", (numToStringUDF(col("pos"))))

val pos_list: Array[String] = Array("US", "GB", "HK", "RU", "CA", "IN", "TW", "DE", "AU", "TH", "JP", "KR", "PH", "ES", "IT", "FR", "IL", "MY", "AE", "PT")

val df2_filt = df2.filter(col("pos_decoded") isin(pos_list: _*))
// count: 1717982887, or 94.7%. SWEET

// remove RU
val pos_list: Array[String] = Array("US", "GB", "HK", "CA", "IN", "TW", "DE", "AU", "TH", "JP", "KR", "PH", "ES", "IT", "FR", "IL", "MY", "AE", "PT")

// 1518557499353989120, 504966108218916864, 579556977047240704, 216454257090494464, 652458996015300608, 1447625805222903808, 289637751035265024, 77968568548851712, 1443403680572243968, 725079540006649856, 797700083997999104, 1155173304420532224, 365635994747142144, 654147845875564544, 437412113808359424, 651896046061879296, 943785596910829568, 73464968921481216, 1158551004141060096
// count: 1690007820, 93.2%

val pos_list_short: Array[String] = Array("US", "GB", "HK", "CA", "IN", "AU", "TH", "FR")
val df3_filt = df2.filter(col("pos_decoded") isin(pos_list_short: _*))
// count: 1387078760 = 76.5%

// alt: endode the pos_list:
val new_arr = for (pos <- pos_list) yield stringToLongNum(pos)
