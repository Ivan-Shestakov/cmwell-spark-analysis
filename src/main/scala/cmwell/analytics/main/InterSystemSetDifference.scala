package cmwell.analytics.main

import cmwell.analytics.data.Spark
import cmwell.analytics.util._
import org.apache.log4j.LogManager
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.{ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

import scala.concurrent.duration.Duration

/**
  * This analysis compares the uuids between two systems (presumably extracted from the infoton table).
  * In this code, the two systems are referred to upstream and downstream (i.e., the downstream system follows
  * the upstream system).
  *
  * The set difference between the two systems are calculated in both directions (upstream - downstream and
  * downstream - upstream).
  *
  * The result is filtered to exclude false positives, which are current infotons
  * (i.e., lastModified > now - currentThreshold) that have not reached consistency yet.
  *
  * The results are written as CSV files. There will be a single CSV file within each result.
  */
object InterSystemSetDifference {

  private val logger = LogManager.getLogger(SetDifferenceUuids.getClass)

  def main(args: Array[String]): Unit = {

    try {

      object Opts extends ScallopConf(args) {

        val durationConverter: ValueConverter[Long] = singleArgConverter[Long](Duration(_).toMillis)

        val upstream: ScallopOption[String] = opt[String]("upstream", short = 'i', descr = "The path to the upstream {uuid,lastModified,path} in parquet format", required = true)
        val downstream: ScallopOption[String] = opt[String]("downstream", short = 'x', descr = "The path to the downstream {uuid,lastModified,path} in parquet format", required = true)

        val currentThreshold: ScallopOption[Long] = opt[Long]("current-threshold", short = 'c', descr = "Filter out any inconsistencies that are more current than this duration (e.g., 24h)", default = Some(Duration("1d").toMillis))(durationConverter)

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The directory to save the output to (in csv format)", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))

        verify()
      }

      Connector(
        appName = "Compare UUIDs between CM-Well instances",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { implicit spark =>

        // Since we will be doing multiple set differences with the same files, do an initial repartition and cache to
        // avoid repeating shuffles. We also want to calculate an ideal partition size to avoid OOM.

        def load(name: String): Dataset[KeyFields] = {
          import spark.implicits._
          spark.read.parquet(name).as[KeyFields]
        }

        val upstreamRaw = load(Opts.upstream())
        val count = upstreamRaw.count()
        val rowSize = KeyFields.estimateTungstenRowSize(upstreamRaw)
        val numPartitions = Spark.idealPartitioning(rowSize * count * 2)

        def repartition(ds: Dataset[KeyFields]): Dataset[KeyFields] =
          ds.repartition(numPartitions, ds("uuid")).persist(StorageLevel.DISK_ONLY)

        val upstream = repartition(upstreamRaw)
        val downstream = repartition(load(Opts.downstream()))

        SetDifferenceAndFilter(upstream, downstream, Opts.currentThreshold(), filterOutMeta = true)
          .write.csv(Opts.out() + "/upstream-except-downstream")

        SetDifferenceAndFilter(downstream, upstream, Opts.currentThreshold(), filterOutMeta = true)
          .write.csv(Opts.out() + "/downstream-except-upstream")
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
