#!/bin/sh


# if [ $# -eq 0 ] || [ "$1" = "-h" ]
# then
#     echo "Please pass in arguments: pos currency"
#     exit
# else
#     pos=$1
#     currency=$2
# fi

test=false

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

script_dir="/home/kendra.frederick/shopping_grid/"

# this must match `output_dir` in generate_output_file
hdfs_base_dir="/user/kendra.frederick/tmp/calendar_data"
hdfs_output_dir=$hdfs_base_dir/$pos-$currency

if [[ $test = true ]]; then
    local_base_dir="/tmp/calendar_data"
else
    local_base_dir="/projects/apps/cco/FlightCalendarUpload"
fi

local_output_dir=$local_base_dir/$pos-$currency

echo "Running spark job"
spark-submit \
    --driver-memory 20g \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 5 \
    --master yarn \
    --conf spark.pyspark.python=python2 \
    --conf spark.pyspark.driver.python=python2 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    $script_dir/generate_output_file_with_args.py --pos $pos --currency $currency --stale-after 30 > $script_dir/stdout.txt 2> $script_dir/stderr.txt

echo "Fetching file from HDFS"
# must first clear target dir
rm -r $local_output_dir
hdfs dfs -get $hdfs_output_dir $local_output_dir
touch $local_base_dir/trigger.txt
echo "Done"

