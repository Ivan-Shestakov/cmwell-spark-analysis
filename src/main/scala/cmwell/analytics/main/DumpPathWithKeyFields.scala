package cmwell.analytics.main

import cmwell.analytics.data.PathWithKeyFields
import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

object DumpPathWithKeyFields {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(DumpPathWithKeyFields.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to (in parquet format)", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Dump path table - key fields",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { spark =>

        val ds = PathWithKeyFields()(spark)
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
