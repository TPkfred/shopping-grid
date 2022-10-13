

# Shopping grid

See Confluence page + write-up: https://confluence.prod.tvlport.com/display/~kendra.frederick/Shopping+Grid

**THIS REPO IS STILL IN DEVELOPMENT**

## To build
Clone repo locally and transfer to a dev environment. 

**TBD**: requirements file to replicate the python environment

## To Run
The `scripts/` folder houses the main scripts for this repository. Their actions / purpose are described below.


# eStreaming data aggregation 

`./scripts/etl/`

This step reads in "raw" estreaming data and performs aggregations for various kinds of analysis:
- Data to support a Lowest Fare Calendar, including:
    - shop counts (data coverage)
    - lowest fare by market and PCC


**Note what follows below is being used for development, and is not intended to be a production workflow**

## E & first part of T
eStreaming data found on dev HDFS is used as input, which is in a format to make it conducive for merging with MIDT data (thus its monniker). This is not an ideal long-term data source, as its format could change and break the below code. However, it is what this project began using.

- input: hdfs: /data/estreaming/midt_1_5 & /data/estreaming/midt_1_5
    - Note: only ~21 days are retained
    - Note: as of 10/01/2022, data are being saved to the latter path
- current ETL script: `agg_estream_data.py`
- location:
    - local: ./scripts/etl
    - shldvfsdh016.tvlport.net: /home/kendra.frederick/shopping_grid
- run command:
    - see ./scripts/etl/run_commands.sh. See top section, "CURRENT RUN SCRIPT"
    - modify `--shop-start` and `--shop-end` args
    - SSH into shldvfsdh016, navigate to the script location, copy & paste the run command into a terminal.
- output: 
    - the output from the script will be saved to /home/kendra.frederick/shopping_grid/stdout.txt
    - see the script output (or the .py file) for the exact location, but aggregated data will be saved to HDFS at /user/kendra.frederick/shop_vol/vX/raw or /raw_with_pcc

## Last part of T + L
The MIDT data format has been "encoded" to reduce its size; strings have been converted to int's. We thus need to "decode" it back to string to make it intelligible.

- source code: 
    - ./utils/encode-decode.scala. See last section, ESTREAMING MIDT_1_5 FORMAT
- running:
    - I typically run this manually from a `spark-shell` on shldvfsdh016, but it could be converted to a script (.jar file) to automate.
    - update the `base_dir` if needed, and the `startDateStr` and `endDateStr`
- output:
    - /user/kendra.frederick/shop_vol/vX/decoded

# Estimating
`./scripts/estimating/`

As a stop gap while models are being developed, lowest fares are estimated based on the most recent shopping data. Various restrictions and filters are placed on the data in order to deliver "reasonable" estimations (i.e. we cannot provide a reasonable estimate for a market for which we have very little data).


# Modeling
`./scripts/modeling`
Machine Learning models are used to fill in any gaps in the shopping data

- Be sure to keep `search_start` and `search_end` up to date in modeling/global-configs.yaml.
    - Eventually, I imagine data agg script would run nightly, and end could be set to yesterday.


# Misc

## utility code
See `/utils` folder for description of code therein. 
