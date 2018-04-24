#!/bin/bash

# Dump uuids, path and lastModified from the infoton table.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

SPARK_TMP="tmp"
SPARK_MASTER="local[24]"
SPARK_MEMORY="31g"
WORKING_DIRECTORY="dump-uuids"

SPARK_ANALYSIS_JAR="cmwell-spark-analysis-assembly-0.1.jar"

rm -rf "${WORKING_DIRECTORY}/infoton"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.DumpInfotonWithKeyFields" "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/infoton" \
 "${CMWELL_INSTANCE}"
