
# ESTREAM DATA

# MIDT format
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 10 \
    --executor-memory 20g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf spark.sql.shuffle.partitions=8 \
    estream_analysis.py --run-mode spark-submit --shop-start 2022-08-22 --shop-end 2022-08-31 --include-pcc > out-estream-analysis.txt 2> err-estream-analysis.txt | tee &


# datalake format
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 10 \
    --executor-memory 20g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf spark.sql.shuffle.partitions=8 \
    estream_analysis_datalake.py --run-mode spark-submit --shop-start 2022-09-01 --shop-end 2022-09-01 --include-pcc > out-datalake.txt 2> err-datalake.txt | tee &