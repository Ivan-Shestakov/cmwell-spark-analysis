package cmwell.analytics.main

import cmwell.analytics.data.IndexWithUuidsOnly
import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * This doesn't work very well due to issues with the ES Spark connector.
  * Use the separate extract-index-from-es project instead.
  */
object DumpIndexWithUuidOnly {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(DumpIndexWithUuidOnly.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to (in parquet format)", required = true)
        val currentOnly: ScallopOption[Boolean] = opt[Boolean]("current-only", short = 'c', descr = "Only include current", required = false, default = Some(true))
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Dump UUIDs from Elasticsearch indexes",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { spark =>

        val ds = IndexWithUuidsOnly(currentOnly = Opts.currentOnly())(spark)
        ds.write.parquet(Opts.out())
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
