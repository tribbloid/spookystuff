package org.tribbloid.spookystuff.factory.driver

import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Capabilities, WebDriver}
import org.tribbloid.spookystuff.{Const, Utils}

/**
  * Created by peng on 9/5/14.
  */
//a bug in this driver has caused it unusable in Firefox 32
object HtmlUnitDriverFactory extends DriverFactory {

   val baseCaps = new DesiredCapabilities
   baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
   baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)

 //  val FirefoxRootPath = "/usr/lib/phantomjs/"
 //  baseCaps.setCapability("webdriver.firefox.bin", "firefox");
 //  baseCaps.setCapability("webdriver.firefox.profile", "WebDriver");

   override def newInstance(capabilities: Capabilities): WebDriver = {
     val newCap = baseCaps.merge(capabilities)

     Utils.retry () {
       Utils.withDeadline(Const.sessionInitializationTimeout) {
         new HtmlUnitDriver(newCap)
       }
     }
   }

 }