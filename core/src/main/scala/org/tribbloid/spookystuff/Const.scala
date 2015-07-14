package org.tribbloid.spookystuff

import org.json4s.DefaultFormats
import org.tribbloid.spookystuff.dsl.LeftOuter

/**
 * Created by peng on 04/06/14.
 */
object Const {

  import scala.concurrent.duration._

  val maxLoop = Int.MaxValue

  val defaultCharset = "ISO-8859-1"

  val defaultJoinType = LeftOuter

  //  val webClientOptions = new WebClientOptions
  //  webClientOptions.setUseInsecureSSL(true)

  val sessionInitializationTimeout = 40.seconds

  val localResourceLocalRetries = 3
  val remoteResourceLocalRetries = 2
  val DFSLocalRetries = 2
  val clusterRetries = 3

  val interactionDelayMax: Duration = 60.seconds
  val interactionDelayMin: Duration = 1.seconds
  val interactionBlock: Boolean = true

  val hardTerminateOverhead: Duration = 20.seconds

  val defaultInputKey = "_"
  val keyDelimiter = "'"
  val onlyPageWildcard = "$"
  val allPagesWildcard = "$_*"
  val defaultJoinKey = "A"

  val jsonFormats = DefaultFormats
}