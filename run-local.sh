#!/bin/bash

output=$2/../kmeans_output
hdfs dfs -rm -f -r $output

spark-submit\
  --master yarn --deploy-mode cluster \
  --executor-cores 1 \
  --num-executors 1 \
  --executor-memory 1g \
  --conf spark.yarn.executor.memoryOverhead=1g \
  --conf spark.driver.memory=1g \
  --conf spark.driver.cores=1 \
  $1 $2

hdfs dfs -cat $output/part-00000

#./run-local.sh ./target/scala-2.11/kmeans_scala_klouvi_riva_2.11-1.0.jar  hdfs://localhost:9090/bigdata/project/kmeans/iris.data.txt

