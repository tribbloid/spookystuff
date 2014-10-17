package org.tribbloid.spookystuff.factory.driver

import java.util.concurrent.TimeUnit

import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Dimension, Capabilities, WebDriver}
import org.tribbloid.spookystuff.{Const, Utils}

/**
 * Created by peng on 25/07/14.
 */
class NaiveDriverFactory(
                          phantomJSPath: String,
                          loadImages: Boolean = false,
                          userAgent: String = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                          resolution: (Int,Int) = (1920, 1080)
                          )
  extends DriverFactory {

  //  val phantomJSPath: String

  val baseCaps = new DesiredCapabilities
  baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
  baseCaps.setCapability("takesScreenshot", true)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomJSPath)
  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"loadImages", loadImages)
  if (userAgent!=null) baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"userAgent", userAgent)
  //  baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

  override def newInstance(capabilities: Capabilities): WebDriver = {
    val newCap = baseCaps.merge(capabilities)

    Utils.retry () {
      Utils.withDeadline(Const.sessionInitializationTimeout) {
        val driver = new PhantomJSDriver(newCap)

        driver.manage().timeouts()
          .pageLoadTimeout(Const.sessionInitializationTimeout,TimeUnit.SECONDS)
          .setScriptTimeout(Const.sessionInitializationTimeout,TimeUnit.SECONDS)

        if (resolution!=null) driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))

          driver
      }
    }
  }
}

object NaiveDriverFactory {

  def apply(
             phantomJSPath: String = System.getenv("PHANTOMJS_PATH"),
             loadImages: Boolean = false
             ) = new NaiveDriverFactory(phantomJSPath, loadImages)
}

//case class NaiveDriverFactory(phantomJSPath: String) extends NaiveDriverFactory(phantomJSPath)
