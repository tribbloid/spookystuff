package com.tribbloids.spookystuff


object CommonConst {

  import scala.concurrent.duration.*

  val driverClosingTimeout: FiniteDuration = 5.seconds
  val driverClosingRetries: Int = 5

  val localResourceLocalRetries: Int = 3 // In-node/partition retries
  val remoteResourceLocalRetries: Int = 2 // In-node/partition retries
  val DFSLocalRetries: Int = 2
  val clusterRetries: Int = 3

  object Interaction {

    val delayMax: Duration = 60.seconds
    val delayMin: Duration = 0.second
    val blocking: Boolean = true
  }
}
