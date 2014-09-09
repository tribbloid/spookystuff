package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.DesiredCapabilities
import org.openqa.selenium.{Capabilities, WebDriver}
import org.tribbloid.spookystuff.{Const, Utils}

/**
 * Created by peng on 25/07/14.
 */
class NaiveDriverFactory(phantomJSPath: String)
  extends DriverFactory {

//  val phantomJSPath: String

  val baseCaps = new DesiredCapabilities
//  baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
//  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
  //  baseCaps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSPath)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"loadImages", false)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

  override def newInstance(capabilities: Capabilities): WebDriver = {
    val newCap = baseCaps.merge(capabilities)

    Utils.retry () {
      Utils.withDeadline(Const.sessionInitializationTimeout) {
        new PhantomJSDriver(newCap)
      }
    }
  }
}

object NaiveDriverFactory {

  def apply(phantomJSPath: String) = new NaiveDriverFactory(phantomJSPath)

  def apply() = new NaiveDriverFactory(System.getenv("PHANTOMJS_PATH"))
}

//case class NaiveDriverFactory(phantomJSPath: String) extends NaiveDriverFactory(phantomJSPath)
