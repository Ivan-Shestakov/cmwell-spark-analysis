package cmwell.analytics.util

import java.io.File

import org.apache.commons.io.FileUtils

import scala.util.control.NonFatal

/** Use the CM-Well /_es API to get the index data for the uuids from a file. */
object ExtractFromIndexByUuid extends App {

  override def main(args: Array[String]): Unit = {

    val cmwellUrl = "http://api-egf-prod-cmwell-la.int.thomsonreuters.com"

    val directory = "/Users/u6066830/Documents/la-analysis/egf/"

    val prefix = directory + "index-except-infoton"
//    val prefix = directory + "index-except-path"

    // not really proper json, but a collection of fragments
    val out = new File(prefix + "-index-data.tsv")

    val uuids = prefix + ".csv"

    FileUtils.writeStringToFile(out, "uuid\tdocument\n")

    for (uuid <- FileUtils.readFileToString(new File(uuids)).split("\n")) {
      try {
        val result = HttpUtil.get(s"$cmwellUrl/_es/$uuid")

        if (result != "null") {
          FileUtils.writeStringToFile(out, s"$uuid\t$result\n", true)
        }
        else {
          FileUtils.writeStringToFile(out, s"$uuid\t<retrieval from es returned null>\n", true)
        }
      }
      catch {
        case NonFatal(ex) =>
          FileUtils.writeStringToFile(out, s"uuid\t<retrieval from es failed>\n", true)
      }
    }
  }
}
