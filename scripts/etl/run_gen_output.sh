#!/bin/sh

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


while [ "$1" != "" ]; do
    case $1 in
        --pos )                 shift
                                pos=$1
                                ;;
        --currency )            shift
                                currency=$1
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


# # this must match `output_dir` in generate_output_file
# hdfs_base_dir="/user/kendra.frederick/tmp/calendar_data"
# hdfs_output_dir=$hdfs_base_dir/$pos-$currency

# if [[ $test = true ]]; then
#     local_base_dir="/tmp/calendar_data"
# else
#     local_base_dir="/projects/apps/cco/FlightCalendarUpload"
# fi

# local_output_dir=$local_base_dir/$pos-$currency

# where output / lookup file gets downloaded to locally
if [[ $test = true ]]; then
    local_base_dir="/tmp/calendar_data"
else
    local_base_dir="/projects/apps/cco/FlightCalendarUpload"
fi

hdfs_output_dir=$hdfs_base_dir/$pos-$currency
local_output_dir=$local_base_dir/$pos-$currency

# Define function to execute file handling
    function fetch_hdfs_csv {
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

}

echo "Running spark job"
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
    $script_dir/generate_output_file_with_args.py --pos $pos --currency $currency --stale-after $staleness --ratio-mean 3 \
    && fetch_hdfs_csv || echo "Problem generating HDFS .csv's; skipping file handling & exiting"

echo ""
echo ""
