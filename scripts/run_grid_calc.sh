
# ESTREAM DATA
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 4 \
    --executor-memory 20g \
    --executor-cores 2 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf spark.sql.shuffle.partitions=8 \
    grid_calc.py --run-mode spark-submit --month 10 --shop-start 2022-08-08 --shop-end 2022-08-14 -n 10 --process-mode process-only > kf-shop-grid-out.txt 2> kf-shop-grid-err.txt | tee &
