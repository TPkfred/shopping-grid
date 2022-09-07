
# ESTREAM DATA
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 10 \
    --executor-memory 20g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf spark.sql.shuffle.partitions=8 \
    vol_counts_markets.py --run-mode spark-submit --shop-start 2022-08-19 --shop-end 2022-0906 > kf-vol-counts-out.txt 2> kf-vol-counts-err.txt | tee &
