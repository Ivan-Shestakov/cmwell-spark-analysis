package cmwell.analytics.util

import java.io.File

import org.apache.commons.io.FileUtils


/** Use the CM-Well /_cas API to get the infoton data for the uuids from a file. */
object ExtractFromInfotonByUuid extends App {

  override def main(args: Array[String]): Unit = {

    val cmwellUrl = "http://api-ege-prod-cmwell-la.int.thomsonreuters.com"

    val directory = "/Users/u6066830/Documents/la-analysis/"

    // val prefix = directory + "path-except-index"
    // val prefix = directory + "infoton-except-index"
    // val prefix = directory + "path-except-infoton"
    // val prefix = directory + "infoton-except-path"

    val prefix = directory + "ege-except-egf"
//    val prefix = directory + "egf-except-ege"


    val out = new File(prefix + "-infoton-data.csv")

    FileUtils.writeStringToFile(out, "uuid,lastModified,path\n", false)

    val uuids = prefix + ".csv"

    for (uuid <- FileUtils.readFileToString(new File(uuids)).split("\n")) {
      val result = HttpUtil.get(s"$cmwellUrl/_cas/$uuid")

      val indexOfFirstNewline = result.indexOf("\n")
      if (indexOfFirstNewline != -1) {
        val resultLessHeader = result.substring(indexOfFirstNewline + 1)
        val lines = resultLessHeader.split("\n").map(_.split(","))

        val lastModified = lines.find { row: Array[String] =>
          row(1) == "cmwell://meta/sys" && row(2) == "lastModified"
        }.map(_ (3)).getOrElse("")

        val path = lines.find { row: Array[String] =>
          row(1) == "cmwell://meta/sys" && row(2) == "path"
        }.map(_ (3)).getOrElse("")

        FileUtils.writeStringToFile(out, s"$uuid,$lastModified,$path\n", true)
      }
      else {
        FileUtils.writeStringToFile(out, s"$uuid,,\n", true) // Not found in infoton table
      }
    }
  }
}
