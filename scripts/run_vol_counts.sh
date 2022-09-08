
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
    estream_analysis.py --run-mode spark-submit --shop-start 2022-09-06 --shop-end 2022-09-06 > out-estream-analysis.txt 2> err-estream-analysis.txt | tee &

