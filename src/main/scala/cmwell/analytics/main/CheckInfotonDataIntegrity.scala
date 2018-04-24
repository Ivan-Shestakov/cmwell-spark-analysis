package cmwell.analytics.main

import cmwell.analytics.data.InfotonDataIntegrity
import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.apache.spark.sql.Column
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.apache.spark.sql.functions._

object CheckInfotonDataIntegrity {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(CheckInfotonDataIntegrity.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Check infoton data integrity"
      ).withSparkSessionDo { spark =>

        val ds = InfotonDataIntegrity()(spark)

        val damagedInfotons = ds.filter(infoton =>
          infoton.hasIncorrectUuid ||
            infoton.hasDuplicatedSystemFields ||
            infoton.hasInvalidContent ||
            infoton.hasMissingOrIllFormedSystemFields
        ).coalesce(1) // We expect the count to be zero, or at least very small, so this should be safe
          .cache()

        // Preserve the data in Parquet format so that it can be examined later without loss.
        damagedInfotons.write.parquet(Opts.out() + "/failed-infotons")

        def countCol(booleanColumn: String): Column =
          sum(when(damagedInfotons(booleanColumn), 1L).otherwise(0L)).as(booleanColumn)

        // Create a summary that shows where any failure occurs over time.
        damagedInfotons
          .groupBy(to_date(to_timestamp(damagedInfotons("lastModified"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")).as("Date"))
          .agg(
            countCol("hasIncorrectUuid"),
            countCol("hasDuplicatedSystemFields"),
            countCol("hasInvalidContent"),
            countCol("hasMissingOrIllFormedSystemFields"))
          .sort(asc("Date"))
          .write
          .option("header", value = true) // include header for easy ingestion into a spreadsheet or report
          .csv(Opts.out() + "/failed-infotons-summary-by-date")
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}