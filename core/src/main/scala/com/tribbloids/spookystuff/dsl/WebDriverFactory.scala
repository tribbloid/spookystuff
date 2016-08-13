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

import scala.collection.mutable

//local to TaskID, if not exist, local to ThreadID
//for every new driver created, add a taskCompletion listener that salvage it.
abstract class DriverFactory[T] extends Serializable {

  // If get is called again before the previous driver is released, the old driver is destroyed to create a new one.
  // this is to facilitate multiple retries
  def get(session: Session): T
  def release(session: Session): Unit

  //all tasks are cleaned at the end of the
}

abstract class WebDriverFactory extends DriverFactories.Transient[CleanWebDriver]{

  override def _destroy(driver: CleanWebDriver): Unit = {

    driver.finalize()
  }

  override def reset(driver: CleanWebDriver): CleanWebDriver = {
    driver.get("")
    driver
  }
}

object DriverFactories {

  sealed abstract class Transient[T] extends DriverFactory[T] {

    @transient lazy val map: ConcurrentMap[Session, T] = ConcurrentMap()

    @transient lazy val cleanUpCache: ConcurrentMap[Long, mutable.Set[T]] = ConcurrentMap()

    def get(session: Session): T = {
      release(session)
      val driver = create(session)
      map += session -> driver
      driver
    }

    def registerListener(v: TaskContext): Unit = {
      if (!cleanUpCache.contains(v.taskAttemptId())) {
        v.addTaskCompletionListener {
          listenerFn
        }
      }
    }

    val listenerFn: (TaskContext) => Unit = {
      tc =>
        val buffer = cleanUpCache
          .remove(tc.taskAttemptId())
          .getOrElse(mutable.Set.empty)
        buffer.foreach {
          driver =>
            destroy(driver, Some(tc))
        }
    }

    final def create(session: Session): T = {
      val created = _create(session)
      session.tcOpt.foreach {
        tc =>
          registerListener(tc)
          val buffer = cleanUpCache
            .getOrElseUpdate(
              tc.taskAttemptId(),
              mutable.Set.empty
            )
          buffer += created
      }

      created
    }

    def _create(session: Session): T

    def reset(driver: T): T

    def release(session: Session): Unit = {
      val existingOpt = map.get(session)
      existingOpt.foreach {
        driver =>
          destroy(driver, session.tcOpt)
      }
    }

    final def destroy(driver: T, tcOpt: Option[TaskContext]): Unit = {
      _destroy(driver)

      tcOpt.foreach {
        tc =>
          cleanUpCache.get(tc.taskAttemptId())
            .foreach {
              buffer =>
                buffer -= driver
            }
      }
    }

    def _destroy(driver: T): Unit

    final def pooled = Pooled(this)
  }

  case class DriverInUse[T](
                             var driver: T,
                             var inUse: Boolean
                           )

  /**
    * delegate create & destroy to PerSessionFactory
    * first get() create a driver as usual
    * calling get() without release() reboot the driver
    * first release() return driver to the pool to be used by the same Spark Task
    * call any function with a new Spark Task ID will add a cleanup TaskCompletionListener to the Task that destroy all drivers
    */
  case class Pooled[T](
                        delegate: Transient[T]
                      ) extends DriverFactory[T] {

    @transient lazy val pool: ConcurrentMap[Either[Long, Long], DriverInUse[T]] = ConcurrentMap()

    def taskOrThreadID(tcOpt: Option[TaskContext]): Either[Long, Long] = {
      Option(TaskContext.get())
        .map{
          v =>
            Left(v.taskAttemptId())
        }
        .getOrElse{
          Right(Thread.currentThread().getId)
        }
    }

    override def get(session: Session): T = {

      val taskOrThreadID = this.taskOrThreadID(session.tcOpt)
      val opt = pool.get(taskOrThreadID)
      opt
        .map {
          tuple =>
            if (!tuple.inUse) {
              tuple.inUse = true
              delegate.reset(tuple.driver)
            }
            else {
              // destroy old and create new
              delegate.destroy(tuple.driver, session.tcOpt)
              val fresh = delegate.create(session)
              tuple.driver = fresh
              fresh
            }
        }
        .getOrElse {
          //create new
          val fresh = delegate.create(session)
          pool.put(taskOrThreadID, DriverInUse(fresh, inUse = true))
          fresh
        }
    }

    override def release(session: Session): Unit = {

      val taskOrThreadID = this.taskOrThreadID(session.tcOpt)
      val opt = pool.get(taskOrThreadID)
      opt.foreach{
        tuple =>
          if (tuple.inUse)
            tuple.inUse = false
      }
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
                      ) extends WebDriverFactory {

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

    def newCap(spooky: SpookyContext, extra: Option[Capabilities] = None): DesiredCapabilities = {
      val result = new DesiredCapabilities(baseCaps)

      result.setCapability (
        PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout",
        spooky.conf.remoteResourceTimeout.toMillis
      )

      val userAgent = spooky.conf.userAgentFactory
      if (userAgent != null) result.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)

      val proxy = spooky.conf.proxy()

      if (proxy != null)
        result.setCapability(
          PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
          Array("--proxy=" + proxy.addr + ":" + proxy.port, "--proxy-type=" + proxy.protocol)
        )

      result.merge(extra.orNull)
    }

    //called from executors
    override def _create(session: Session): CleanWebDriver = {

      new PhantomJSDriver(newCap(session.spooky)) with CleanWebDriverMixin
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

    override def _create(session: Session): CleanWebDriver = {

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