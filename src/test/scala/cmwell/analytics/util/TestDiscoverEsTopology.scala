package cmwell.analytics.util

import org.scalatest.{FlatSpec, Matchers}

class TestDiscoverEsTopology extends FlatSpec with Matchers {

  val hostPort = "localhost:9201"
  val esTopology = DiscoverEsTopology(hostPort, "cm_well_all")

  esTopology.nodes should not be empty
  esTopology.shards should not be empty

  for {
    (_, nodeIds) <- esTopology.shards
    nodeId <- nodeIds
  } esTopology.nodes.keys should contain(nodeId)
}