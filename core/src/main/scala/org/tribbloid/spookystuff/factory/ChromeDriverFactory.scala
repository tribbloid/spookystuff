package org.tribbloid.spookystuff.factory

import java.util

import org.openqa.selenium.chrome.{ChromeOptions, ChromeDriverService, ChromeDriver}
import org.openqa.selenium.remote.server.DriverFactory
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Capabilities, WebDriver}
import org.tribbloid.spookystuff.{Const, Utils}

/**
 * Created by peng on 9/5/14.
 */
object ChromeDriverFactory extends DriverFactory {

  val baseCaps = DesiredCapabilities.chrome()
  //   baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
  //   baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)

  val options = new ChromeOptions()
  options.setBinary("/opt/google/chrome/chromedriver")
  val params = new util.ArrayList[String]
  params.add("--verbose")
  params.add("--log-path=chromedriver.log")
  options.addArguments(params)
  baseCaps.setCapability(ChromeOptions.CAPABILITY, options)

  override def registerDriver(capabilities: Capabilities, implementation: Class[_ <: WebDriver]): Unit = {
    throw new UnsupportedOperationException("cannot add new driver type")
  }

  override def newInstance(capabilities: Capabilities): WebDriver = {

    System.setProperty("webdriver.chrome.driver", "/opt/google/chrome/chromedriver")

    val newCap = baseCaps.merge(capabilities)

    return Utils.retry () {
      Utils.withDeadline(Const.sessionInitializationTimeout) {
        new ChromeDriver(newCap)
      }
    }
  }

  override def hasMappingFor(capabilities: Capabilities): Boolean = {
    return true
  }

}