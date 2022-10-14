
nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    /home/kendra.frederick/shopping_grid/restrict_data.py --run-mode ranges --all >> stdout_restrict.txt 2> stderr.txt &


nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    /home/kendra.frederick/shopping_grid/restrict_data.py --run-mode default >> stdout_restrict.txt 2> stderr.txt &



nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    $script_dir/generate_output_file_with_args.py --dow-filter --rsd 1 --ratio-median 3 > $script_dir/stdout.txt 2> $script_dir/stderr.txt &


nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    $script_dir/generate_output_file_with_args.py --stale-after 30 > $script_dir/stdout.txt 2> $script_dir/stderr.txt &