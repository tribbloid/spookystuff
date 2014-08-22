package org.tribbloid.spookystuff.factory

import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.server.DriverFactory
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Capabilities, WebDriver}
import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.utils._

/**
 * Created by peng on 25/07/14.
 */
class NaiveDriverFactory extends DriverFactory {

  val baseCaps = new DesiredCapabilities;
  baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
  //  baseCaps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
  val phantomJSRootPath = "/usr/lib/phantomjs/"
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSRootPath + "bin/phantomjs");
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"loadImages", false);
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

  override def registerDriver(capabilities: Capabilities, implementation: Class[_ <: WebDriver]) {
    throw new UnsupportedOperationException("cannot add new driver type")
  }

  override def newInstance(capabilities: Capabilities): WebDriver = {
    val newCap = baseCaps.merge(capabilities)

    return retry () {
      withDeadline(Const.driverCallTimeout) {
        new PhantomJSDriver(newCap)
      }
    }
  }

  override def hasMappingFor(capabilities: Capabilities): Boolean = {
    return true
  }
}
