/*
Copyright 2007-2010 Selenium committers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.tribbloids.spookystuff.dsl

import com.gargoylesoftware.htmlunit.BrowserVersion
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.caching._
import com.tribbloids.spookystuff.session.{CleanWebDriver, CleanWebDriverMixin, ProxySetting, Session}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.{SparkFiles, TaskContext}
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.CapabilityType._
import org.openqa.selenium.remote.{BrowserType, CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Capabilities, Platform, Proxy, WebDriver}
import org.slf4j.LoggerFactory

//local to TaskID, if not exist, local to ThreadID
//for every new worker created, add a taskCompletion listener that salvage it.
abstract class Factory[T] extends Serializable {

  def getTaskOrThreadID: Either[Long, Long] = {
    try {
      Left(TaskContext.get().taskAttemptId())
    }
    catch {
      case e: Throwable =>
        Right(Thread.currentThread().getId)
    }
  }

  // If get is called again before the previous worker is released, the old worker is destroyed to create a new one.
  // this is to facilitate multiple retries
  def get(session: Session): T
  def release(session: Session): Unit

  //all tasks are cleaned at the end of the
}

abstract class PerSessionFactory[T] extends Factory[T] {

  val pool: ConcurrentMap[Session, T] = ConcurrentMap()

  // at the end of
  val taskPool: ConcurrentMap[Long, T] = ConcurrentMap()

  def get(session: Session): T = {
    release(session)
    val worker = create(session)
    pool += session -> worker
    worker
  }

  def create(session: Session): T

  def release(session: Session): Unit = {
    val existingOpt = pool.get(session)
    existingOpt.foreach {
      worker =>
        destroy(worker)
        session.spooky.metrics.driverReclaimed += 1
    }
  }

  def destroy(worker: T): Unit
}

case class PerTaskFactory[T](
                              delegate: PerSessionFactory[T]
                            ) extends Factory[T] {

  val pool: ConcurrentMap[Long, T] = ConcurrentMap()

  override def get(session: Session): T = ???

  override def release(session: Session): Unit = ???
}

object WebDriverFactories {

  abstract class WebDriverFactory extends PerSessionFactory[WebDriver]{

    override def destroy(worker: WebDriver): Unit = {

      worker.close()
      worker.quit()
    }
  }

  object PhantomJS {

    val remotePhantomJSURL = "https://s3-us-west-1.amazonaws.com/spooky-bin/phantomjs-linux/phantomjs"

    def pathOptionFromEnv = SpookyUtils.validateLocalPath(System.getenv("PHANTOMJS_PATH"))
      .orElse(SpookyUtils.validateLocalPath(System.getProperty("phantomjs.binary.path")))

    def pathFromMaster(nameFromMaster: String) = Option(nameFromMaster).map(SparkFiles.get).orNull

    def path(path: String, nameFromMaster: String): String = pathOptionFromEnv
      .orElse {
        SpookyUtils.validateLocalPath(path)
      }
      .getOrElse {
        LoggerFactory.getLogger(this.getClass).info("$PHANTOMJS_PATH does not exist, downloading from master")
        pathFromMaster(nameFromMaster)
      }

    //only accessable from driver
    @transient def fileName = pathOptionFromEnv.flatMap {
      _.split("/").lastOption
    }.getOrElse {
      remoteFileName
    }

    @transient def remoteFileName =
      remotePhantomJSURL.split("/").last
  }

  case class PhantomJS(
                        path: String = PhantomJS.pathOptionFromEnv.orNull,
                        loadImages: Boolean = false,
                        fileNameFromMaster: String = PhantomJS.fileName,
                        ignoreSysEnv: Boolean = false
                      )
    extends WebDriverFactory {

    @transient lazy val exePath = {
      val effectivePath = if (!ignoreSysEnv) PhantomJS.path(path, fileNameFromMaster)
      else PhantomJS.pathFromMaster(fileNameFromMaster)

      assert(effectivePath != null, "INTERNAL ERROR: PhantomJS has null path")
      effectivePath
    }

    @transient lazy val baseCaps = {
      val baseCaps = new DesiredCapabilities(BrowserType.PHANTOMJS, "", Platform.ANY)

      baseCaps.setJavascriptEnabled(true); //< not really needed: JS enabled by default
      baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS, true)
      //  baseCaps.setCapability(CapabilityType.HAS_NATIVE_EVENTS, false)
      baseCaps.setCapability(TAKES_SCREENSHOT, true)
      baseCaps.setCapability(ACCEPT_SSL_CERTS, true)
      baseCaps.setCapability(SUPPORTS_ALERTS, true)
      baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, exePath)
      baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "loadImages", loadImages)
      baseCaps
    }

    //    baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

    def newCap(capabilities: Capabilities, spooky: SpookyContext): DesiredCapabilities = {
      val result = new DesiredCapabilities(baseCaps)

      result.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout", spooky.conf.remoteResourceTimeout.toMillis)

      val userAgent = spooky.conf.userAgentFactory
      if (userAgent != null) result.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)

      val proxy = spooky.conf.proxy()

      if (proxy != null)
        result.setCapability(
          PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
          Array("--proxy=" + proxy.addr + ":" + proxy.port, "--proxy-type=" + proxy.protocol)
        )

      result.merge(capabilities)
    }

    //called from executors
    override def create(session: Session): CleanWebDriver = {


      new PhantomJSDriver(newCap(null, session.spooky)) with CleanWebDriverMixin
    }
  }

  case class HtmlUnit(
                       browser: BrowserVersion = BrowserVersion.getDefault
                     ) extends WebDriverFactory {

    val baseCaps = new DesiredCapabilities(BrowserType.HTMLUNIT, "", Platform.ANY)

    def newCap(capabilities: Capabilities, spooky: SpookyContext): DesiredCapabilities = {
      val result = new DesiredCapabilities(baseCaps)

      val userAgent = spooky.conf.userAgentFactory
      //TODO: this is useless, need custom BrowserVersion
      //see http://stackoverflow.com/questions/12853715/setting-user-agent-for-htmlunitdriver-selenium
      if (userAgent != null) result.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)

      val proxy: ProxySetting = spooky.conf.proxy()

      if (proxy != null) {
        result.setCapability(PROXY, proxy.toSeleniumProxy)
      }

      result.merge(capabilities)
    }

    override def create(session: Session): CleanWebDriver = {

      val cap = newCap(null, session.spooky)
      val driver = new HtmlUnitDriver(browser) with CleanWebDriverMixin
      driver.setJavascriptEnabled(true)
      driver.setProxySettings(Proxy.extractFrom(cap))

      driver
    }
  }

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
}