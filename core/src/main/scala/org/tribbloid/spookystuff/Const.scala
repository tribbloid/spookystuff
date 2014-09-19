package org.tribbloid.spookystuff

import com.fasterxml.jackson.databind.{ObjectMapper, ObjectWriter}
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory
import org.tribbloid.spookystuff.operator.{LeftOuter, Inner}

import scala.concurrent.duration.Duration

/**
 * Created by peng on 04/06/14.
 */
//TODO: propose to merge with SpookyContext
//TODO: can use singleton pattern? those values never changes after SparkContext is defined
object Const {

  import scala.concurrent.duration._

//  val usePageCache = false //delegated to smart execution
  val pageExpireAfter: Duration = 30.minutes

  //default max number of elements scraped from a page, set to Int.max to allow unlimited fetch
  val fetchLimit = 500

  val defaultCharset = "ISO-8859-1"

//  type Logging = com.typesafe.scalalogging.slf4j.Logging

  val defaultJoinType = LeftOuter

  val jsonMapper = new ObjectMapper()

//  val webClientOptions = new WebClientOptions
//  webClientOptions.setUseInsecureSSL(true)

  val sessionInitializationTimeout = 60

  val defaultLocalRetry = 3
  val resolveRetry = 2

  val actionDelayMax: Duration = 20.seconds
  val actionDelayMin: Duration = 2.seconds
}
