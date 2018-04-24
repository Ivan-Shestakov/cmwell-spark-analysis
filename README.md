# CM-Well Spark Data Analytics

This project implements a collection of tools for analyzing the data integrity and consistency of data
within a CM-Well instance. The analysis done using Apache Spark.

## Installation

While these analytics are implemented using Apache Spark, it does not require a dedicated Spark cluster
to operate. The scripts provided to run the analytics will start up a suitable Spark instance that runs
in local mode (i.e., within a single JVM process). This could easily be adapted to run on a Spark cluster
if one was available.

This project requires direct access to the Cassandra and Elasticsearch nodes used by CM-Well,
so if the CM-Well instance being analyzed is running within an isolated virtual network, these components
must be installed within that virtual network. While you could use dedicated hardware, we have had good
results installing it on a CM-Well node. If you do install it on a CM-Well node, make sure that the
installation directory is on a device that has plenty of free space, since Spark temporary data and
extracted working datasets may be large.

Scripts in this project use the assembly from the *extract-index-from-es* project to extract data from ES
indexes. While this project includes use of the Spark ES connector, and that capability can be used, 
the Spark ES connector has proven to be unreliable, 
and the *extract-index-from-es* project appear to be both more reliable and faster.

The installation steps are:
* Create a directory that all the components will be installed in.
* If a JRE and Apache Spark are not already installed:
    * Download a JRE (currently, jre1.8.0_161 is assumed), and unpack it into installation directory.
    * Download Apache Spark (currently, spark-2.2.1-bin-hadoop2.7 is assumed), and unpack it into the 
    installation directory.
    * Ensure that the names of the JRE and Spark directories are consistent with the *set-runtime.sh* script.
* Copy *src/main/resources/log4j.properties* to *spark-2.2.1-bin-hadoop2.7/conf*.
* Create an assembly for this project (sbt assembly). 
    * Copy the assembly *cmwell-spark-analysis-assembly-0.1.jar* to the installation directory.
* Create an assembly for the extract-index-from-es project (sbt assembly). 
    * Copy the assembly *extract-index-from-es-assembly-0.1.jar* to the installation directory.
* Copy the bash scripts in *src/scripts* to the installation directory (both from this project
and from the *extract-index-from-es project*) to the installation directory.
    * Make each of the scripts executable (chmod 777 *.sh).
* Copy the *extract-index-from-es-assembly.0.1.jar* assembly from the *extract-index-from-es* project into the
installation directory.

If you have a pre-existing JRE and/or Spark installation, you can modify the script _set-runtime.sh_ to do nothing,
or to set the JAVA_HOME and/or SPARK_HOME variables accordingly. 

# Running the Analysis

Each analysis has an associated script, and in all cases, the script can be
invoked with a single parameter that is a CM-Well URL. For example:

`./find-infoton-inconsistencies "http://my.cmwell.host"`

In general, each script is designed to do some analysis, possibly extract some data into a given directory,
produce a file summarizing the results. The detailed data are typically retained for further analysis.
In general, bulk data extracted for further analysis will be stored in Parquet format, and smaller summary
data is stored in CSV format.

Some of the tools simply extract data for further analysis (e.g., within Spark). For example, to move to another
downstream system to compare the population of uuids (i.e., in an ad-hoc analysis via a Spark shell).

**check-infoton-data-integrity.sh** - Reads from the Cassandra infoton table and recalculates the uuid.
If the calculated uuid is not the same as the stored uuid, the infoton is extracted for further study.
This analysis also does some structural checks on the infoton data (e.g., data content, duplicate system fields).
A summary of the failure counts, by date, is generated.

**dump-infoton-with-uuid-only.sh** - Extract the uuids for all infotons from the infoton table. 

**dump-path-with-uuid-only.sh** - Extract the uuids for all infotons from the path table.

**find-duplicate-system-fields.sh** - Looks for infotons that have duplicated system fields. A similar check is done by
*check-infoton-data-integrity*, but this analysis is much faster.

**find-infoton-index-inconsistencies.sh** - Compares the system fields in the infoton table and the index, and looks
for inconsistencies.

**set-difference-uuids.sh** - Gets the uuids from the index, infoton and path tables, and calculates the set
difference between each pair. This looks for uuids that are missing.
