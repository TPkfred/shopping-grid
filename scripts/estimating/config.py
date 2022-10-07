
zscore_filter = True
dow_filter = True
rsd_filter = True

zscore_threshold = 3
shop_counts_threshold = 0
rsd_threshold = 1.0

# Note: don't actually need to filter on rank;
# the above restrictions filter out more than the top 15,000 markets
rank_filter = False
rank_threshold = 15000

hdfs_input_dir = "/user/kendra.frederick/shop_vol/v7/US-pos_extra-days/"
# this is copied in run.sh. If modified here, modify there
hdfs_output_dir = "/user/kendra.frederick/tmp/calendar_data"


# columns from estreaming data to write to the output file
# these may need updating if data source changes
cols_to_write = [
 'pos',
 'currency',
 'origin',
 'destination',
 'outDeptDt',
 'inDeptDt',
 'min_fare'
]