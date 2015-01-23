package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.dsl.LeftOuter

/**
 * Created by peng on 04/06/14.
 */
object Const {

  import scala.concurrent.duration._

  val maxLoop = 500

  val defaultCharset = "ISO-8859-1"

  val defaultJoinType = LeftOuter

//  val webClientOptions = new WebClientOptions
//  webClientOptions.setUseInsecureSSL(true)

  val sessionInitializationTimeout = 40.seconds

  val localResourceLocalRetry = 3
  val remoteResourceLocalRetry = 2
  val DFSLocalRetry = 2

  val actionDelayMax: Duration = 60.seconds
  val actionDelayMin: Duration = 2.seconds

  val hardTerminateOverhead: Duration = 20.seconds

  val phantomJSPath = System.getenv("PHANTOMJS_PATH")

  val defaultInputKey = "_"
  val keyDelimiter = "'"
  val onlyPageWildcard = "$"
  val allPagesWildcard = "$_*"
  val defaultJoinKey = "A"
}