package org.tribbloid.spookystuff

import org.openqa.selenium.phantomjs.PhantomJSDriverService
import org.openqa.selenium.remote.DesiredCapabilities

/**
 * Created by peng on 04/06/14.
 */
//TODO: propose to merge with SpookyContext
final object Conf {

  val pageDelay = 5
  val pageTimeout = 50
//  val usePageCache = false //delegated to smart execution
  val pageExpireAfter = 1800

  val savePagePath = "file:///home/peng/spookystuff/"
  val saveScreenshotPath = "file:///home/peng/spookystuffScreenShots/"

  val errorPageDumpDir = "file:///home/peng/spookystuff/error"

  val phantomJSCaps = new DesiredCapabilities;
  phantomJSCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
//  phantomJSCaps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
//  val cliArgs = "--logLevel=ERROR"
//  phantomJSCaps.setCapability(PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_CLI_ARGS, cliArgs);//TODO: logLevel parameter doesn't work, why?
  val phantomJSRootPath = "/usr/lib/phantomjs/"
  phantomJSCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSRootPath + "bin/phantomjs");

//  type Logging = com.typesafe.scalalogging.slf4j.Logging
}
