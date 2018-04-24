#!/bin/bash

# Run an set difference analysis of the uuids between two systems.
# This script assumes that the extract of key fields from the upstream system (in parquet format)
# has already been placed in the working directory in the sub-directory "upstream"

if [ -z $1 ]; then
 echo "usage: $0 <cmwell-url>"
 exit 1
fi

source ./set-runtime.sh

CMWELL_INSTANCE=$1

SPARK_TMP="tmp"
SPARK_MASTER="local[24]"
SPARK_MEMORY="31g"
WORKING_DIRECTORY="inter-system-uuid-differences"
EXTRACT_DIRECTORY_UPSTREAM="upstream"
EXTRACT_DIRECTORY_DOWNSTREAM="downstream"

if [ ! -d "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_UPSTREAM}" ]; then
  echo "The key fields extract from the upstream system must be placed in ${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_UPSTREAM}"
  exit 1
fi

CONSISTENCY_THRESHOLD="1d"

EXTRACT_ES_UUIDS_JAR="extract-index-from-es-assembly-0.1.jar"
SPARK_ANALYSIS_JAR="cmwell-spark-analysis-assembly-0.1.jar"

set -e # bail out if any command fails

rm -rf "${WORKING_DIRECTORY}/*.csv"
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_DOWNSTREAM}"

# Extract key fields from index, path and infoton.

${SPARK_HOME}/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master "$SPARK_MASTER" --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.DumpInfotonWithKeyFields "${SPARK_ANALYSIS_JAR}" \
 --out "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_DOWNSTREAM}" \
 "${CMWELL_INSTANCE}"

# Do the set difference operations

${SPARK_HOME}/bin/spark-submit \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 --master ${SPARK_MASTER} --driver-memory ${SPARK_MEMORY} --conf "spark.local.dir=${SPARK_TMP}" \
 --class cmwell.analytics.main.InterSystemSetDifference "${SPARK_ANALYSIS_JAR}" \
 --current-threshold="${CONSISTENCY_THRESHOLD}" \
 --upstream "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_UPSTREAM}" \
 --downstream "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_DOWNSTREAM}" \
 --out "${WORKING_DIRECTORY}"

# Remove the extract directories since they may be large
# The upstream extract is left for the caller to clean up.
rm -rf "${WORKING_DIRECTORY}/${EXTRACT_DIRECTORY_DOWNSTREAM}"

# Extract the csv files for each set difference from the hadoop-format directory.
# They will be in hadoop format (one file per partition), so concatenate them all.
for x in "upstream-except-downstream" "downstream-except-upstream"
do
 cat "${WORKING_DIRECTORY}/${x}"/*.csv > "${WORKING_DIRECTORY}/${x}.csv"
 rm -rf "${WORKING_DIRECTORY}/${x}"
done
