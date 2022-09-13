date_str=$(date -d "yesterday" +'%Y-%m-%d')
spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf spark.sql.shuffle.partitions=8 \
    /data/16/kendra.frederick/scripts/estream_analysis_datalake.py --run-mode spark-submit --shop-start $date_str --shop-end $date_str --include-pcc
