

# Shopping grid

See Confluence page + write-up: https://confluence.prod.tvlport.com/display/~kendra.frederick/Shopping+Grid

**THIS REPO IS STILL IN DEVELOPMENT**

## To build
Clone repo locally and transfer to a dev environment. 

**TBD**: requirements file to replicate the python environment

## To Run
The `scripts/` folder houses the main scripts for this repository. Their actions / purpose are described below.


# Pipeline
Unless otherwise specified below, the files referenced below are located in `./scripts/etl/`

On shldvfsdh016.tvlport.net, scripts are saved to `/data/16/kendra.frederick/scripts`. (They are upload / updated manually.)

This pipeline generate "Flight Calendar predictions", and is triggered to run nightly by a cronjob on shldvfsdh016:
```
########################################
#####FLIGHT CALENDAR PREDICTIONS########
########################################
fcp_script_dir=/data/16/kendra.frederick/scripts
00 01 * * * nohup $fcp_script_dir/flight_cal_predictions.sh --pos US --currency USD  >>$fcp_script_dir/fc-preds-stdout.txt 2> /dev/null &
```

## eStreaming data aggregation 

This step reads in "raw" estreaming data and performs aggregations to support Lowest Fare Calendar predictions / modeling, and data analysis, including:
- data coverage
- shopping volume 
- lowest fare by market, travel dates, and optionally other dimensions


eStreaming data found on dev HDFS is used as input, which is in a format to make it conducive for merging with MIDT data (thus its monniker). This is not an ideal long-term data source, as its format could change and break the below code. However, it is what this project began using.

- input: hdfs: /data/estreaming/midt_1_5 & /data/estreaming/midt_1_5_pn
    - Note: only ~21 days are retained
    - Note: as of 10/01/2022, data are being saved to the latter path
- current ETL script: `agg_estream_data.py`
- location:
    - repo: ./scripts/etl
- to run:
    - see `./scripts/etl/run_commands.sh` (top section, "CURRENT RUN SCRIPT") and `./scripts/etl/run_agg_loop.sh` for example run commands
    - modify `--shop-start` and `--shop-end` args as needed
    - SSH into shldvfsdh016, navigate to the script location, copy & paste the run command into a terminal.
- output: 
    - the output from the script will be saved to /home/kendra.frederick/shopping_grid/stdout.txt
    - see the script output (or the .py file) for the exact location, but aggregated data will be saved to HDFS at /user/kendra.frederick/shop_vol/vX/raw or /raw_with_pcc


## processing aggregated data
The "MIDT" format of eStreaming data has been "encoded" to reduce its size; strings have been converted to int's. We thus need to "decode" it back to string to make it intelligible.

- source code: 
    - `./scripts/etl/preprocess.py`
- running:
    - see `./scripts/etl/run_commands.sh`
- output:
    - /user/kendra.frederick/shop_grid/{pos}-{currency}

## Generating lookup file of estimates

As a stop gap while models are being developed, lowest fares are estimated based on the most recent shopping data. Various restrictions and filters can be placed on the data in order to deliver more "reasonable" estimations. 

- source code: 
    - `./scripts/etl/generate_output_file_with_args.py`
- running:
    - see `./scripts/etl/run_gen_output.sh`
- output (csv):
    - /tmp/calendar_data/

`run_gen_output.sh` also contains bash code for retrieving the resulting .csv file(s) from HDFS and saving them locally for upload to S3, to serve the Flight Calendar API.


# Modeling
`./scripts/modeling`
Machine Learning models are being developed to fill in any gaps in the shopping data

- Be sure to keep `search_start` and `search_end` up to date in modeling/global-configs.yaml.
    - Eventually, this script would run nightly, and end could be set to yesterday.


# Misc
- `./utils`: utility code
