package org.tribbloid.spookystuff

import com.gargoylesoftware.htmlunit.WebClientOptions
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.openqa.selenium.phantomjs.PhantomJSDriverService
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}

/**
 * Created by peng on 04/06/14.
 */
//TODO: propose to merge with SpookyContext
//TODO: can use singleton pattern? those values never changes after SparkContext is defined
final object Conf {

  val pageDelay = 5
  val pageTimeout = 50
//  val usePageCache = false //delegated to smart execution
  val pageExpireAfter = 1800

  //default max number of elements scraped from a page, set to Int.max to allow unlimited fetch
  val fetchLimit = 100

  val defaultCharset = "ISO-8859-1"

  val savePagePath = "s3n://spooky_page"

  val localSavePagePath = "temp/spooky_page/"
//  val saveScreenshotPath = "file:///home/peng/spookystuffScreenShots/"

//  val errorPageDumpDir = "s3n://spooky_errorpage"
  val localErrorPageDumpDir = "temp/spooky_errorpage"

  val phantomJSCaps = new DesiredCapabilities;
  phantomJSCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
  phantomJSCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
//  phantomJSCaps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
//  val cliArgs = "--logLevel=ERROR"
//  phantomJSCaps.setCapability(PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_CLI_ARGS, cliArgs);//TODO: logLevel parameter doesn't work, why?
  val phantomJSRootPath = "/usr/lib/phantomjs/"
  phantomJSCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSRootPath + "bin/phantomjs");

//  type Logging = com.typesafe.scalalogging.slf4j.Logging

//  val webClientOptions = new WebClientOptions
//  webClientOptions.setUseInsecureSSL(true)
}
