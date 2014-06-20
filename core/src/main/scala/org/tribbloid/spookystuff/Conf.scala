package org.tribbloid.spookystuff

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.openqa.selenium.phantomjs.PhantomJSDriverService
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}

/**
 * Created by peng on 04/06/14.
 */
//TODO: propose to merge with SpookyContext
//TODO: can use singleton pattern? those values never changes after SparkContext is defined
final object Conf {

  var hConf: Broadcast[Configuration] = null

  def init(sc: SparkContext) {
    this.hConf = sc.broadcast(sc.hadoopConfiguration)
  }

  val pageDelay = 5
  val pageTimeout = 50
//  val usePageCache = false //delegated to smart execution
  val pageExpireAfter = 1800

  //default max number of elements scraped from a page, set to Int.max to allow unlimited fetch
  val fetchLimit = 100

  val savePagePath = "file:///home/peng/spookystuff/"
  val saveScreenshotPath = "file:///home/peng/spookystuffScreenShots/"

  val errorPageDumpDir = "file:///home/peng/spookystuff/error"

  val phantomJSCaps = new DesiredCapabilities;
  phantomJSCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
  phantomJSCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
//  phantomJSCaps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
//  val cliArgs = "--logLevel=ERROR"
//  phantomJSCaps.setCapability(PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_CLI_ARGS, cliArgs);//TODO: logLevel parameter doesn't work, why?
  val phantomJSRootPath = "/usr/lib/phantomjs/"
  phantomJSCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSRootPath + "bin/phantomjs");

//  type Logging = com.typesafe.scalalogging.slf4j.Logging
}
