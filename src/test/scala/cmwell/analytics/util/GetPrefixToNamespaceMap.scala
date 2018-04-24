package cmwell.analytics.util

object GetPrefixToNamespaceMap extends App {

  override def main(args: Array[String]): Unit = {

    println(EsUtil.nsPrefixes("localhost:9201"))

  }
}
