#!/bin/bash

# Start a Spark shell (e.g., for ad-hoc analysis on extracted data).

source ./set-runtime.sh

SPARK_TMP="tmp"
SPARK_MASTER="local[24]"
SPARK_MEMORY="31g"

$SPARK_HOME/bin/spark-shell \
 --master "${SPARK_MASTER}" -Xmx${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}"
