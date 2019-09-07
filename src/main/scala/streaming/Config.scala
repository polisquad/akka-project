package streaming

import scala.concurrent.duration._

object Config {

  // Where to store the snapshots
  val StatePath = "./tmp/state/"

  // Time after which the initial deploy of the graph is considered failed
  val DeployTimeout = 10 seconds

  // Start a new checkpoint every SnapshotTime
  val SnapshotTime = 10 seconds

  // Time after which a snapshot is considered failed
  val SnapshotTimeout = 10 seconds

  // Time after which a snapshot restore is considered failed
  val RestoreSnapshotTimeout = 10 seconds

  // All for one strategy adopted by Master node of a graph
  val MaxNrOfRetries = 100
  val WithinTimeRange = 5 seconds

  // Time after which a marker message is considered lost
  val MarkersTimeout = 5 seconds

  // Key,value separator for the default text source
  val KeyValueSeparator = ":"

}