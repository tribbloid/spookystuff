package org.tribbloid.spookystuff

import com.fasterxml.jackson.databind.{ObjectMapper, ObjectWriter}
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory
import org.tribbloid.spookystuff.operator.Inner

/**
 * Created by peng on 04/06/14.
 */
//TODO: propose to merge with SpookyContext
//TODO: can use singleton pattern? those values never changes after SparkContext is defined
object Const {

  val pageDelay = 20
  val resourceTimeout = 60
//  val usePageCache = false //delegated to smart execution
  val pageExpireAfter = 1800

  //default max number of elements scraped from a page, set to Int.max to allow unlimited fetch
  val fetchLimit = 500

  val defaultCharset = "ISO-8859-1"

//  type Logging = com.typesafe.scalalogging.slf4j.Logging

  def phantomJSPath = System.getenv("PHANTOMJS_PATH")

  val defaultJoinType = Inner

  val jsonMapper = new ObjectMapper()

//  val webClientOptions = new WebClientOptions
//  webClientOptions.setUseInsecureSSL(true)

  val sessionInitializationTimeout = 60

  val localRetry = 3
}
