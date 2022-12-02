

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

# ===============================
# agg_estream_datalake_pos

# 5 x c6g.12xl 
--deploy-mode cluster 
--conf spark.dynamicAllocation.enabled=false
--conf spark.executor.cores=5 
--conf spark.driver.cores=5 
--conf spark.executor.memoryOverhead=2g 
--conf spark.driver.memoryOverhead=2g 
--conf spark.executor.memory=18g 
--conf spark.driver.memory=18g 
--conf spark.executor.instances=40

# 8 x c6g.12xl 
--deploy-mode cluster 
--conf spark.dynamicAllocation.enabled=false
--conf spark.executor.cores=5 
--conf spark.driver.cores=5 
--conf spark.executor.memoryOverhead=2g 
--conf spark.driver.memoryOverhead=2g 
--conf spark.executor.memory=8g 
--conf spark.driver.memory=8g 
--conf spark.executor.instances=60

# 10 x c6g.12xl 
--deploy-mode cluster 
--conf spark.dynamicAllocation.enabled=false
--conf spark.executor.cores=5 
--conf spark.driver.cores=5 
--conf spark.executor.memoryOverhead=2g 
--conf spark.driver.memoryOverhead=2g 
--conf spark.executor.memory=8g 
--conf spark.driver.memory=8g 
--conf spark.executor.instances=80

# 10 x c5.9xlarge core nodes
--deploy-mode cluster
--conf spark.executor.cores=5
--conf spark.driver.cores=5
--conf spark.dynamicAllocation.enabled=false
--conf spark.yarn.driver.memoryOverhead=2g
--conf spark.yarn.executor.memoryOverhead=2g
--conf spark.executor.memory=8g
--conf spark.driver.memory=8g 
--conf spark.executor.instances=60
# --conf spark.default.parallelism=690
--conf spark.sql.shuffle.partitions=200


# 8 x c5.9xl 64GB
--deploy-mode cluster 
--conf spark.executor.cores=5 
--conf spark.driver.cores=5 
--conf spark.dynamicAllocation.enabled=false 
--conf spark.yarn.executor.memoryOverhead=2g 
--conf spark.executor.memory=8g 
--conf spark.driver.memory=8g 
--conf spark.executor.instances=40

# 8 x m5.8xl 64GB
--deploy-mode cluster 
--conf spark.dynamicAllocation.enabled=false 
--conf spark.executor.cores=5 
--conf spark.driver.cores=5 
--conf spark.executor.memoryOverhead=2g 
--conf spark.driver.memoryOverhead=2g 
--conf spark.executor.memory=8g 
--conf spark.driver.memory=8g 
--conf spark.executor.instances=40


# 10 x d3.8xl
--deploy-mode cluster 
--conf spark.dynamicAllocation.enabled=false
--conf spark.executor.cores=5 
--conf spark.driver.cores=5 
--conf spark.executor.memoryOverhead=2g 
--conf spark.driver.memoryOverhead=2g 
--conf spark.executor.memory=40g 
--conf spark.driver.memory=40g 
--conf spark.executor.instances=50


# ================
# pull data


spark-submit --deploy-mode cluster s3://tvlp-ds-users/kendra-frederick/scripts/pull_data.py 
-o JFK -d LHR -dd 20221113 -sd 20221031