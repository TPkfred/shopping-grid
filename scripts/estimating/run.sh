#!/bin/sh

# script_dir="/data/16/kendra.frederick/scripts"
script_dir="/home/kendra.frederick/shopping_grid/estimating"

# this must match what is in config.py
hdfs_output_dir="/user/kendra.frederick/tmp/calendar_data"

# TODO: update this once get location from Jon
# local_output_dir="/projects/app/XXX"
local_output_dir="/home/kendra.frederick/tmp"

nohup spark-submit \
    --driver-memory 30g \
    --num-executors 15 \
    --executor-memory 20g \
    --executor-cores 8 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    $script_dir/generate_output_file.py --run-mode config >> $script_dir/stdout.txt 2> $script_dir/stderr.txt

echo "Fetching file from HDFS"
hdfs dfs -get $hdfs_output_dir $local_output_dir
echo "Done"