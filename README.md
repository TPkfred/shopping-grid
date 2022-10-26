

# Shopping grid

See Confluence page + write-up: https://confluence.prod.tvlport.com/display/~kendra.frederick/Shopping+Grid

**THIS REPO/PROJECT IS STILL IN DEVELOPMENT**

## To build
Clone repo locally and transfer to a dev environment. See `min_requirements.txt` for minimum package versions required. 

Scripts are backwards-compatible with Python 2.7.5 and Spark 2.1.0.

Modeling code may require additional and/or higher version packages. scikit-learn >= 1.0. See `modeling/requirements.txt`.


## To Run
The `scripts/` folder houses the main scripts for this repository. Their actions / purpose are described in the Pipeline section below.

To run any script, see examples in `run_commands.sh` in the sub-folders in `scripts/`.

On shldvfsdh016.tvlport.net, scripts are saved to `/projects/apps/cco/flightCalendarPredictions`. (They are uploaded / updated manually.)

They are triggered to run nightly by a cronjob on shldvfsdh016:
```
########################################
#####FLIGHT CALENDAR PREDICTIONS########
########################################
fcp_script_dir=/projects/apps/cco/flightCalendarPredictions
00 01 * * * nohup $fcp_script_dir/flight_cal_predictions.sh --pos US --currency USD  >>$fcp_script_dir/fc-preds-stdout.txt 2> /dev/null &
```

# Pipeline
This pipeline generates "Flight Calendar predictions". There are several different versions of the pipeline:
- on-premise
- aws, source = raw data
- aws, source = summary data


|execution location|code location|source data|data format|num scripts|
|---|---|---|---|---|
|on-premise|`scripts/etl`|raw|"midt_1_5"|3|
|aws|`scripts/aws/source_raw`|raw|"datalake"|3|
|aws|`scripts/aws/source_summaries`|summary|n/a|1|


The script that operates on raw data perform the following functions

## eStreaming data aggregation 

This step reads in "raw" estreaming data and performs aggregations by market, travel dates, and optionally other dimensions to support Lowest Fare Calendar predictions / modeling, and data analysis, including:
- shopping volume 
- lowest fare 
- data coverage (by inference)


eStreaming data found on dev HDFS is in a format to make it conducive for merging with MIDT data (thus its monniker, "midt_1_5"). This is not an ideal long-term data source, as its format could change and break the below code. However, it is what this project began using.

Scripts:
- scripts/etl/agg_estream_data.py
- scripts/aws_source_raw/aws_estream_datalake.py

Note that summary data has already effectively been aggregated, so when using this as a data source, we do not need to perform this step.


## processing aggregated data
The "MIDT" format of eStreaming data has been "encoded" to reduce its size; strings have been converted to int's. We thus need to "decode" it back to string to make it intelligible.

The "datalake" format of eStreaming data has not been encoded, so we do not need to decode it.

In either case, we perform some datatype conversion, and filter on POS and currency.

Scripts:
- scripts/etl/preprocess.py
- scripts/aws_source_raw/aws_preprocess.py
- scripts/aws_source_summaries/process_summary_data.py
    - this script combines preprocessing and output file generation.

## Generating lookup file of estimates

As a stop gap while models are being developed, lowest fares are estimated based on the most recent shopping data. Various restrictions and filters can be placed on the data in order to deliver more "reasonable" estimations. 

Scripts:
- scripts/etl/generate_output_file_with_args.py
- scripts/aws_source_raw/aws_generate_output.py
- scripts/aws_source_summaries/process_summary_data.py
    - this script combines preprocessing and output file generation.

`scripts/etl/run_gen_output.sh` also contains bash code for retrieving the resulting .csv files from HDFS and saving them locally for upload to S3, to serve the Flight Calendar API.


# Modeling
`./scripts/modeling`
Machine Learning models are being developed to fill in any gaps in the shopping data

- Be sure to keep `search_start` and `search_end` up to date in modeling/global-configs.yaml.
    - Eventually, this script would run nightly, and end could be set to yesterday.


# Misc
- `./utils`: utility code
