

# agg_estream_datalake.py
--shop-start 2022-10-19
--shop-end 2022-10-19

# aws_preprocess.py
# arguments:
--pos US --currency USD
--shop-start 2022-10-19 --shop-end 2022-10-19
--test

# aws_generate_output.py
--pos US --currency USD
--stale-after 30
--ratio-mean 3
--test