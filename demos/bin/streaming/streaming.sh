#!/usr/bin/env bash
get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

export SPARK_MAJOR_VERSION=2
spark-submit \
--name "streaming" \
--master yarn-client \
--queue default \
--num-executors 10 \
--executor-cores 5 \
--driver-memory 25g \
--executor-memory 25g \
--conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
--conf mapreduce.job.complete.cancel.delegation.tokens=false \
--class com.fx.demo.streaming.streamingDemo $appdir/../../lib/lib.jar \
--configLocation /parms-csv-$1.properties \
--log4j /log4j-local.properties