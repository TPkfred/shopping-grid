# eStreaming initial aggregation
date_str="2022-09-13"
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf spark.sql.shuffle.partitions=8 \
    /data/16/kendra.frederick/scripts/estream_analysis_datalake.py --run-mode spark-submit --shop-start $date_str --shop-end $date_str --include-pcc >> shop-vol-counts-out.txt 2> shop-vol-counts-err.txt &


# Filter on POS, not top markets. Exclude PCC
    # remove shuffle partitions conf
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    /home/kendra.frederick/shopping_grid/estream_analysis_pos.py --run-mode spark-submit --shop-start 2022-09-21 --shop-end 2022-09-28 >> stdout.txt 2> stderr.txt &




# Grid analysis
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


# Analysis
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    /home/kendra.frederick/shopping_grid/zero_min_fares.py > analysis_out.txt 2> analysis_err.txt &
