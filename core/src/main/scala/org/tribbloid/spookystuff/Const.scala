package org.tribbloid.spookystuff

import com.fasterxml.jackson.databind.ObjectMapper
import org.tribbloid.spookystuff.operator.LeftOuter

/**
 * Created by peng on 04/06/14.
 */
//TODO: propose to merge with SpookyContext
//TODO: can use singleton pattern? those values never changes after SparkContext is defined
object Const {

  import scala.concurrent.duration._

  val maxLoop = 500

  val defaultCharset = "ISO-8859-1"

//  type Logging = com.typesafe.scalalogging.slf4j.Logging

  val defaultJoinType = LeftOuter

//  val webClientOptions = new WebClientOptions
//  webClientOptions.setUseInsecureSSL(true)

  val sessionInitializationTimeout = 60.seconds

  val inPartitionRetry = 3
  val remoteResourceInPartitionRetry = 2

  val actionDelayMax: Duration = 20.seconds
  val actionDelayMin: Duration = 2.seconds
}
