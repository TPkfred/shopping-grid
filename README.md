

# Shopping grid

See Confluence page + write-up: https://confluence.prod.tvlport.com/display/~kendra.frederick/Shopping+Grid


## To build
Clone repo locally and transfer to a dev environment. 


## To Run

The `scripts/` folder houses the main scripts for this repository. Their actions / purpose are described below.

### eStreaming data aggregation / combined analysis

This step reads in "raw" estreaming data and performs aggregations for various kinds of analysis:
- Data to support a Lowest Fare Calendar, including:
    - shop counts (data coverage)
    - lowest fare by market and PCC
- Top Markets
    - counts number of shops by market

To run:
- `scripts/shop_vol_counts.sh` runs the python/Pyspark script
- `scripts/restream_analysis_datalake.py` uses /data/estreaming/datalake_1_5 input format and is the preferred script moving forward
- `scripts/restream_analysis.py` uses /data/estreaming/midt_1_5 input format, and is deprecated as of ~09/07/2022

The datalake version of the data aggregation script is set to run daily via a cronjob. This is defined in the crontab on shldvfsdh016.tvlport.net: 

```bash
### eStreaming Data Aggregation (KF)  ###
kf_script_dir=/data/16/kendra.frederick/scripts
00 02 * * * nohup $kf_script_dir/shop_vol_counts.sh >> $kf_script_dir/shop-vol-counts-out.txt 2> $kf_script_dir/shop-vol-counts-err.txt &
##########################################
```

### Coverage Analysis

- grid_calc.py
- run_grid_calc.sh


## utility code
See `/utils` folder for description of code therein. 

# Modeling

- Be sure to keep `search_start` and `search_end` up to date in modeling/global-configs.yaml.
    - Eventually, I imagine data agg script would run nightly, and end could be set to yesterday.