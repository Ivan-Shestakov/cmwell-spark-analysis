package cmwell.analytics

import cmwell.analytics.data.PathWithKeyFields
import cmwell.analytics.util.CmwellConnector

object CountPathWithAllFields {

  def main(args: Array[String]): Unit = {

    CmwellConnector(
      cmwellUrl = "http://localhost:9000",
      appName = "Test Count IndexWithUuidsOnly",
      sparkShell = true
    ).withSparkSessionDo { implicit spark =>

      val ds = PathWithKeyFields()

      ds.show(truncate = false)
    }
  }
}
