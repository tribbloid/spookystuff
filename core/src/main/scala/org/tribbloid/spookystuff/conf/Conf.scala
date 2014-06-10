package org.tribbloid.spookystuff.conf

import org.openqa.selenium.remote.DesiredCapabilities
import org.openqa.selenium.WebDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriverService, PhantomJSDriver}

/**
 * Created by peng on 04/06/14.
 */
final object Conf {

  val pageDelay = 5
  val pageTimeout = 50
//  val usePageCache = false //deprecated to findOldPagesByKey
  val pageExpireAfter = 1800

  val savePagePath = "/home/peng/spookystuff/"
  val saveScreenshotPath = "/home/peng/spookystuffScreenShots/"

  val phantomJSCaps = new DesiredCapabilities();
  phantomJSCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
//  phantomJSCaps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
  phantomJSCaps.setCapability(
    PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,
    "/usr/lib/phantomjs/bin/phantomjs"
  );
}
