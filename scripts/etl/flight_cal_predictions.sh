#! /bin/bash

echo "STARTING FLIGHT CAL PREDICTIONS"

# where scripts are located
script_dir=/projects/apps/cco/flightCalendarPredictions

# variables we don't want/need to define as arguments
## don't provide predictions older than this many days
staleness=30
max_dtd=120
max_los=21

# temporary HDFS output directory
# this must match `output_dir` in generate_output_file script
hdfs_base_dir="/tmp/calendar_data"

# Define default values for arguments
test=false
pos=US
currency=USD

# this is intended to be run nightly, on yesterday's data
# but date can be supplied as an argument to override this
d=$(date -d "yesterday" +'%Y-%m-%d')

usage="""$(basename "$0") --pos POS --curency CUR [-d|--date DATE] [-t] [-h]  
-- runs series of spark jobs for predicting lead price for lowest fare 
calendar. Date defaults to yesterday"""

while [ "$1" != "" ]; do
    case $1 in
        --pos )                 shift
                                pos=$1
                                ;;
        --currency )            shift
                                currency=$1
                                ;;
        -d | --date )           shift
                                d=$1
                                ;;
        -t | --test )           test=true
                                ;;
        -h | --help )           echo $usage
                                exit
                                ;;
        * )                     echo $usage
                                exit 1
    esac
    shift
done     

# where output / lookup file gets downloaded to locally
if [[ $test = true ]]; then
    local_base_dir="/tmp/calendar_data"
else
    local_base_dir="/projects/apps/cco/FlightCalendarUpload"
fi

hdfs_output_dir=$hdfs_base_dir/$pos-$currency
local_output_dir=$local_base_dir/$pos-$currency


# eStreaming data aggregation
spark-submit \
    --driver-memory 10g \
    --num-executors 40 \
    --executor-memory 5g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    $script_dir/agg_estream_data.py --run-mode spark-submit --shop-start $d --shop-end $d

# pre-process aggregated data
spark-submit \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-memory 5g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    --jars /projects/apps/cco/estreamingTransformerStream/bin/estreammidtmerger_2.11-1.0.jar \
    $script_dir/preprocess.py --shop-start $d --shop-end $d --pos $pos --currency $currency --max-stay-duration $max_los --max-days-til-dept $max_dtd

# generate output file
spark-submit \
    --driver-memory 20g \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    $script_dir/generate_output_file_with_args.py --pos $pos --currency $currency --stale-after $staleness

# retrieve file from HDFS
## must first clear target dir
echo "Cleaning local output dir"
rm -rf $local_output_dir
echo "Fetching output file from HDFS"
hdfs dfs -get $hdfs_output_dir $local_output_dir
rm $local_output_dir/_SUCCESS
## add trigger file
touch $local_base_dir/trigger.txt
echo "Trigger file created"
chmod -R 777 $local_base_dir/*
echo "DONE!"
echo ""
echo ""