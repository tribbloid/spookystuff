package org.tribbloid.spookystuff.dsl.driverfactory

import org.openqa.selenium.Capabilities
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.tribbloid.spookystuff.session.{CleanWebDriver, CleanWebDriverHelper}
import org.tribbloid.spookystuff.{Const, SpookyContext}

/**
 * Created by peng on 25/07/14.
 */
case class NaiveDriverFactory(
                          phantomJSPath: String = Const.phantomJSPath,
                          loadImages: Boolean = false,
                          resolution: (Int, Int) = (1920, 1080)
                          )
  extends DriverFactory {

  //  val phantomJSPath: String

  val baseCaps = new DesiredCapabilities
  baseCaps.setJavascriptEnabled(true); //< not really needed: JS enabled by default
  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS, true)
//  baseCaps.setCapability(CapabilityType.HAS_NATIVE_EVENTS, false)
  baseCaps.setCapability("takesScreenshot", true)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSPath)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "loadImages", loadImages)

  //    baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

  def newCap(capabilities: Capabilities, spooky: SpookyContext): DesiredCapabilities = {
    val result = new DesiredCapabilities(baseCaps)

    result.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", spooky.remoteResourceTimeout*1000)

    val userAgent = spooky.userAgent
    if (userAgent != null) result.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)

    val proxy = spooky.proxy()

    if (proxy != null)
      result.setCapability(
        PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
        Array("--proxy=" + proxy.addr+":"+proxy.port, "--proxy-type=" + proxy.protocol)
      )

    result.merge(capabilities)
  }

  override def _newInstance(capabilities: Capabilities, spooky: SpookyContext): CleanWebDriver = {

    new PhantomJSDriver(newCap(capabilities, spooky)) with CleanWebDriverHelper
  }
}

//case class NaiveDriverFactory(phantomJSPath: String) extends NaiveDriverFactory(phantomJSPath)
