#! /bin/bash

if [ $# -eq 0 ] || [ "$1" = "-h" ]
then
    echo "Please pass in arguments: start date, end date"
    exit
else
    d=$1
    end_d=$2
fi

echo $d
echo $end_d

while [ "$d" != "$end_d" ]; do
    echo $d
    nohup spark-submit \
        --driver-memory 10g \
        --num-executors 40 \
        --executor-memory 5g \
        --executor-cores 5 \
        --master yarn \
        --conf spark.pyspark.python=python2 \
        --conf spark.pyspark.driver.python=python2 \
        --conf "spark.yarn.executor.memoryOverhead=2g" \
        /home/kendra.frederick/shopping_grid/final_scripts/agg_estream_data.py --run-mode spark-submit --shop-start $d --shop-end $d >> stdout.txt 2> stderr.txt

    d=$(date -I -d "$d + 1 day")
done