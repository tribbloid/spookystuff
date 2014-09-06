package org.tribbloid.spookystuff.factory

import org.openqa.selenium.firefox.FirefoxDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.remote.server.DriverFactory
import org.openqa.selenium.{Capabilities, WebDriver}
import org.tribbloid.spookystuff.{Const, Utils}

/**
  * Created by peng on 9/5/14.
  */
//a bug in this driver has caused it unusable in Firefox 32
object HtmlUnitDriverFactory extends DriverFactory {

   val baseCaps = new DesiredCapabilities;
   baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
   baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)

 //  val FirefoxRootPath = "/usr/lib/phantomjs/"
 //  baseCaps.setCapability("webdriver.firefox.bin", "firefox");
 //  baseCaps.setCapability("webdriver.firefox.profile", "WebDriver");

   override def registerDriver(capabilities: Capabilities, implementation: Class[_ <: WebDriver]): Unit =  {
     throw new UnsupportedOperationException("cannot add new driver type")
   }

   override def newInstance(capabilities: Capabilities): WebDriver = {
     val newCap = baseCaps.merge(capabilities)

     return Utils.retry () {
       Utils.withDeadline(Const.sessionInitializationTimeout) {
         new HtmlUnitDriver(newCap)
       }
     }
   }

   override def hasMappingFor(capabilities: Capabilities): Boolean = {
     return true
   }

 }