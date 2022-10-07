

# Current ETL process

## E & first part of T:
This estreaming data found on dev HDFS input, which is in a format to make it conducive for merging with MIDT data (thus its monniker). This is not an ideal long-term data source, as its format could change and break the below code.

- input: /data/estreaming/midt_1_5
    - Note: only ~21 days are retained
- current ETL script: `estream_analysis_pos.py`
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



## OBSOLETE CRONJOB
To run:
- `scripts/shop_vol_counts.sh` runs the python/Pyspark script
- `scripts/restream_analysis_datalake.py` uses /data/estreaming/datalake_1_5 input format and is the preferred script moving forward
- `scripts/restream_analysis.py` uses /data/estreaming/midt_1_5 input format, and is deprecated as of ~09/07/2022

**Cronjob has been deleted**
The datalake version of the data aggregation script is set to run daily via a cronjob. This is defined in the crontab on shldvfsdh016.tvlport.net: 

```bash
### eStreaming Data Aggregation (KF)  ###
kf_script_dir=/data/16/kendra.frederick/scripts
00 02 * * * nohup $kf_script_dir/shop_vol_counts.sh >> $kf_script_dir/shop-vol-counts-out.txt 2> $kf_script_dir/shop-vol-counts-err.txt &
##########################################
```