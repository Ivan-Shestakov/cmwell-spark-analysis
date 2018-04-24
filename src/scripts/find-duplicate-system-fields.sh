#!/bin/bash

# Find duplicate system fields in the infoton table.

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

SPARK_TMP="tmp"
SPARK_MASTER="local[24]"
SPARK_MEMORY="31g"

WORKING_DIRECTORY="duplicate-system-fields"
DETAILED_RESULT="infoton"

SPARK_ANALYSIS_JAR="cmwell-spark-analysis-assembly-0.1.jar"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}"

$SPARK_HOME/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "${SPARK_MASTER}" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class "cmwell.analytics.main.FindDuplicatedSystemFields" "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/${DETAILED_RESULT}" \
 "${CMWELL_INSTANCE}"

# Extract the detail data in CSV form for easier access.
mv "${WORKING_DIRECTORY}"/${DETAILED_RESULT}/part*.csv "${WORKING_DIRECTORY}"/${DETAILED_RESULT}.csv
rm -rf "${WORKING_DIRECTORY}/${DETAILED_RESULT}"
