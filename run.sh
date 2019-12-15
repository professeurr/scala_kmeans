#!/bin/bash

output=$2/../kmeans_output
hdfs dfs -rm -f -r $output

spark-submit\
  --master yarn --deploy-mode cluster \
  --executor-cores 4 \
  --num-executors 3 \
  --executor-memory 1g \
  --conf spark.yarn.executor.memoryOverhead=1g \
  --conf spark.driver.memory=1g \
  --conf spark.driver.cores=1 \
  --conf spark.yarn.jars="file:///home/cluster/shared/spark/jars/*.jar" \
  $1 $2

hdfs dfs -cat $output/part-00000

#--conf spark.yarn.jars="file:///home/cluster/shared/spark/jars/*.jar" \
#./run.sh kmeans_scala_klouvi_riva_2.11-1.0.jar hdfs:///user/user159/iris.data.txt hdfs:///user/user159/kmeans_output

#hdfs dfs -cat $3/log/part-00000
