package org.tribbloid.spookystuff.factory.driver

import java.util.concurrent.TimeUnit

import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Dimension, Capabilities, WebDriver}
import org.tribbloid.spookystuff.{SpookyContext, Const, Utils}

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

  def newCap(capabilities: Capabilities) = baseCaps.merge(capabilities)

  override def newInstance(capabilities: Capabilities, spooky: SpookyContext): WebDriver = {

    Utils.retry () {
      Utils.withDeadline(Const.sessionInitializationTimeout) {
        val driver = new PhantomJSDriver(newCap(capabilities))

        driver.manage().timeouts()
          .implicitlyWait(spooky.remoteResourceTimeout.toSeconds,TimeUnit.SECONDS)
          .pageLoadTimeout(spooky.remoteResourceTimeout.toSeconds,TimeUnit.SECONDS)
          .setScriptTimeout(spooky.remoteResourceTimeout.toSeconds,TimeUnit.SECONDS)

        if (resolution!=null) driver.manage().window().setSize(new Dimension(resolution._1, resolution._2))

        driver
      }
    }
  }
}

object NaiveDriverFactory {

  def apply(
             phantomJSPath: String = System.getenv("PHANTOMJS_PATH"),
             loadImages: Boolean = false,
             userAgent: String = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
             resolution: (Int,Int) = (1920, 1080)
             ) = new NaiveDriverFactory(phantomJSPath, loadImages, userAgent, resolution)
}

//case class NaiveDriverFactory(phantomJSPath: String) extends NaiveDriverFactory(phantomJSPath)
