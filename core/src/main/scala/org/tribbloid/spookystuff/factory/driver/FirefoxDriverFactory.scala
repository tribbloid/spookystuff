//package org.tribbloid.spookystuff.factory.driver
//
//import org.openqa.selenium.firefox.FirefoxDriver
//import org.openqa.selenium.remote.DesiredCapabilities
//import org.openqa.selenium.{Capabilities, WebDriver}
//import org.tribbloid.spookystuff.utils.{Utils, Const}
//import org.tribbloid.spookystuff.SpookyContext
//
///**
// * Created by peng on 9/5/14.
// */
////just for debugging
////a bug in this driver has caused it unusable in Firefox 32
//object FirefoxDriverFactory extends DriverFactory {
//
//  val baseCaps = new DesiredCapabilities
//  //  baseCaps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
//  //  baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,true)
//
//  //  val FirefoxRootPath = "/usr/lib/phantomjs/"
//  //  baseCaps.setCapability("webdriver.firefox.bin", "firefox");
//  //  baseCaps.setCapability("webdriver.firefox.profile", "WebDriver");
//
//  override def newInstance(capabilities: Capabilities, spooky: SpookyContext): WebDriver = {
//    val newCap = baseCaps.merge(capabilities)
//
//    Utils.retry(Const.DFSInPartitionRetry) {
//      Utils.withDeadline(spooky.distributedResourceTimeout) {new FirefoxDriver(newCap)}
//    }
//  }
//}