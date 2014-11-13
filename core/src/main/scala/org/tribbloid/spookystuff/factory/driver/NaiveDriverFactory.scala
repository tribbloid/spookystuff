package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.Capabilities
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.CleanWebDriver
import org.tribbloid.spookystuff.utils.Const

/**
 * Created by peng on 25/07/14.
 */
class NaiveDriverFactory(
                          phantomJSPath: String,
                          loadImages: Boolean,
                          resolution: (Int,Int)
                          )
  extends DriverFactory {

  //  val phantomJSPath: String

  val baseCaps = new DesiredCapabilities
  baseCaps.setJavascriptEnabled(true); //< not really needed: JS enabled by default
  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS, true)
  baseCaps.setCapability(CapabilityType.HAS_NATIVE_EVENTS, false)
  baseCaps.setCapability("takesScreenshot", true)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSPath)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "loadImages", loadImages)

  //    baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

  def newCap(capabilities: Capabilities, spooky: SpookyContext): DesiredCapabilities = {
    baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", spooky.remoteResourceTimeout*1000)

    val userAgent = spooky.userAgent
    if (userAgent != null) baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)

    baseCaps.merge(capabilities)
  }

  override def newInstance(capabilities: Capabilities, spooky: SpookyContext): CleanWebDriver =
    new PhantomJSDriver(newCap(capabilities, spooky))
}

object NaiveDriverFactory {

  def apply(
             phantomJSPath: String = Const.phantomJSPath,
             loadImages: Boolean = false,
             resolution: (Int, Int) = (1920, 1080)
             ) = new NaiveDriverFactory(phantomJSPath, loadImages, resolution)
}

//case class NaiveDriverFactory(phantomJSPath: String) extends NaiveDriverFactory(phantomJSPath)
