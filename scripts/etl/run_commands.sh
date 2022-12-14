# CURRENT RUN SCRIPTS

# agg estreaming - old data format
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    /home/kendra.frederick/shopping_grid/agg_estream_data.py --run-mode spark-submit --shop-start 2022-10-02 --shop-end 2022-10-11 >> stdout.txt 2> stderr.txt &

# tuning params for new /midt_1_5_pn format
    # this runs faster (15 min vs. 30 min) but still stalls out after
    # first day. Run in loop using `run_agg_loop.sh`
nohup spark-submit \
    --driver-memory 10g \
    --num-executors 40 \
    --executor-memory 5g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    /home/kendra.frederick/shopping_grid/agg_estream_data_v2.py --run-mode spark-submit --shop-start 2022-11-20 --shop-end 2022-11-20 >> stdout.txt 2> stderr.txt &



# further processing
nohup spark-submit \
    --driver-memory 20g \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    --jars /projects/apps/cco/estreamingTransformerStream/bin/estreammidtmerger_2.11-1.0.jar \
    /home/kendra.frederick/shopping_grid/preprocess.py --shop-start 2022-08-30 --shop-end 2022-09-20 > pp_stdout.txt 2> stderr.txt &

# update dtd & LOS
nohup spark-submit \
    --driver-memory 20g \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    --jars /projects/apps/cco/estreamingTransformerStream/bin/estreammidtmerger_2.11-1.0.jar \
    preprocess.py --shop-start 2022-09-21 --shop-end 2022-10-17 --max-stay-duration 21 --max-days-til-dept 120 > pp_stdout.txt 2> /dev/null &
# --shop-start 2022-08-30 --shop-end 2022-09-20


# generate market pickle files
nohup spark-submit \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-memory 5g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    --conf "spark.yarn.driver.memoryOverhead=2g" \
    generate_market_pkl_files.py


# -------------------------
# old run code
# -------------------------

nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    /home/kendra.frederick/shopping_grid/estream_analysis_pos.py --run-mode spark-submit --shop-start 2022-09-29 --shop-end 2022-10-10 >> stdout.txt 2> stderr.txt &


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


./run_gen_output.sh --pos US --currency USD >>rerun-stdout.txt 2> rerun-stderr.txt &