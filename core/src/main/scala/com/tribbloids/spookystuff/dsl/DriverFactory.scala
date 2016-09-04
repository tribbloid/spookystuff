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

import java.io.File

import com.gargoylesoftware.htmlunit.BrowserVersion
import com.tribbloids.spookystuff.caching._
import com.tribbloids.spookystuff.dsl.DriverFactories.Pooling
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{SpookyConf, SpookyContext, SpookyException}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkFiles, TaskContext}
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.CapabilityType._
import org.openqa.selenium.remote.{BrowserType, CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Capabilities, Platform, Proxy}
import org.slf4j.LoggerFactory

import scala.collection.mutable

//local to TaskID, if not exist, local to ThreadID
//for every new driver created, add a taskCompletion listener that salvage it.
//TODO: get(session) should have 2 impl:
// if from the same session release the existing one immediately.
// if from a different session but same taskAttempt wait for the old one to be released.
// in any case it should ensure 1 taskAttempt only has 1 active driver
//TODO: delay Future-based waiting control until asynchonous Action exe is implemented. Right now it works just fine
abstract class DriverFactory[+T] extends Serializable {

  // If get is called again before the previous driver is released, the old driver is destroyed to create a new one.
  // this is to facilitate multiple retries
  def get(session: Session): T
  def release(session: Session): Unit

  def deploy(spooky: SpookyContext): Unit = {}
}

sealed abstract class Transient[T] extends DriverFactory[T] {

  // session -> driver
  @transient lazy val sessionToDriver: ConcurrentMap[Session, T] = ConcurrentMap()
  // taskAttemptID -> Set[driver]
  @transient lazy val taskCleanUpCache: ConcurrentMap[Long, mutable.Set[T]] = ConcurrentMap()

  def get(session: Session): T = {
    release(session)
    val driver = create(session)
    sessionToDriver += session -> driver
    driver
  }

  def addTaskCompletionListener(v: TaskContext): Unit = {
    if (!taskCleanUpCache.contains(v.taskAttemptId())) {
      v.addTaskCompletionListener {
        listenerFn
      }
    }
  }

  val listenerFn: (TaskContext) => Unit = {
    tc =>
      val buffer = taskCleanUpCache
        .remove(tc.taskAttemptId())
        .getOrElse(mutable.Set.empty)
      buffer.foreach {
        driver =>
          destroy(driver, Some(tc))
      }
  }

  final def create(session: Session): T = {
    val created = _createImpl(session)
    session.tcOpt.foreach {
      tc =>
        addTaskCompletionListener(tc)
        val buffer = taskCleanUpCache
          .getOrElseUpdate(
            tc.taskAttemptId(),
            mutable.Set.empty
          )
        buffer += created
    }

    created
  }

  def _createImpl(session: Session): T

  def factoryReset(driver: T): T

  def release(session: Session): Unit = {
    val existingOpt = sessionToDriver.remove(session)
    existingOpt.foreach {
      driver =>
        destroy(driver, session.tcOpt)
    }
  }

  final def destroy(driver: T, tcOpt: Option[TaskContext]): Unit = {
    _destroyImpl(driver)

    tcOpt.foreach {
      tc =>
        taskCleanUpCache.get(tc.taskAttemptId())
          .foreach {
            buffer =>
              buffer -= driver
          }
    }
  }

  def _destroyImpl(driver: T): Unit

  final def pooling = Pooling(this)
}

abstract class WebDriverFactory extends Transient[CleanWebDriver]{

  override def _destroyImpl(driver: CleanWebDriver): Unit = {

    driver.finalize()
  }

  override def factoryReset(driver: CleanWebDriver): CleanWebDriver = {
    driver.get("")
    driver
  }
}

abstract class PythonDriverFactory extends Transient[PythonDriver]{

  override def _destroyImpl(driver: PythonDriver): Unit = {

    driver.finalize()
  }

  override def factoryReset(driver: PythonDriver): PythonDriver = {
    driver
  }
}

//TODO: deploy lazily/as failover
object DriverNotDeployedException extends SpookyException("INTERNAL: driver should be automatically deployed")

object DriverFactories {

  case class DriverInPool[T](
                              var driver: T,
                              var busy: Boolean
                            )

  /**
    * delegate create & destroy to PerSessionFactory
    * first get() create a driver as usual
    * calling get() without release() reboot the driver
    * first release() return driver to the pool to be used by the same Spark Task
    * call any function with a new Spark Task ID will add a cleanup TaskCompletionListener to the Task that destroy all drivers
    */
  case class Pooling[T](
                         delegate: Transient[T]
                       ) extends DriverFactory[T] {

    //taskOrThreadID -> (driver, busy)
    @transient lazy val pool: ConcurrentMap[Either[Long, Long], DriverInPool[T]] = ConcurrentMap()

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
            if (!tuple.busy) {
              tuple.busy = true
              delegate.factoryReset(tuple.driver)
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
          pool.put(taskOrThreadID, DriverInPool(fresh, busy = true))
          fresh
        }
    }

    override def release(session: Session): Unit = {

      val taskOrThreadID = this.taskOrThreadID(session.tcOpt)
      val opt = pool.get(taskOrThreadID)
      opt.foreach{
        tuple =>
          if (tuple.busy)
            tuple.busy = false
      }
    }

    override def deploy(spooky: SpookyContext): Unit = delegate.deploy(spooky)
  }

  import com.tribbloids.spookystuff.utils.ImplicitUtils._

  object PhantomJS {

    final val HTTP_RESOURCE_URI = "https://s3-us-west-1.amazonaws.com/spooky-bin/phantomjs-linux/phantomjs"

    final def uri2fileName(path: String) = path.split("/").last

    final def DEFAULT_PATH = System.getProperty("user.home") :/ ".spookystuff/phantomjs"

    def path: SpookyContext => String = {
      _ =>
        SpookyConf.getDefault("phantomjs.path", DEFAULT_PATH)
    }

    def asynchCopyIfNotExist(src: String, dst: String): Unit = this.synchronized {
      val srcFile = new File(src)
      val dstFile = new File(dst)
      if (!dstFile.exists()) {
        SpookyUtils.universalCopy(srcFile.toPath, dstFile.toPath)
      }
    }

    def asynchDelete(dst: String): Unit = this.synchronized {
      val dstFile = new File(dst)
      FileUtils.forceDelete(dstFile)
    }
  }

  case class PhantomJS(
                        path: SpookyContext => String = PhantomJS.path,
                        loadImages: Boolean = false,
                        redeploy: Boolean = false
                      ) extends WebDriverFactory {

    /**
      * can only used on driver
      */
    override def deploy(spooky: SpookyContext): Unit = {
      if ((!isDeployedOnWorkers(spooky)) || redeploy) {

        val pathOptOnDriver = SpookyUtils.validateLocalPath(path(spooky))
        val isDeployedOnDriver: Boolean = pathOptOnDriver.nonEmpty

        val uri = if (!isDeployedOnDriver) {
          // add binary from internet
          val uri = PhantomJS.HTTP_RESOURCE_URI
          LoggerFactory.getLogger(this.getClass).info(s"Downloading PhantomJS from Internet ($uri)")
          uri
        }
        else {
          // add binary from driver
          val uri = pathOptOnDriver.get
          LoggerFactory.getLogger(this.getClass).info(s"Downloading PhantomJS from Driver ($uri)")
          uri
        }

        val sc = spooky.sqlContext.sparkContext
        sc.addFile(uri)
        val fileName = PhantomJS.uri2fileName(uri)

        //deploy
        if (redeploy) {
          sc.exePerCore {
            val dstStr = path(spooky)
            PhantomJS.asynchDelete(dstStr)
          }
            .count()
        }

        sc.exePerCore {
          val srcStr = SparkFiles.get(fileName)
          val dstStr = path(spooky)
          PhantomJS.asynchCopyIfNotExist(srcStr, dstStr)
        }
          .count()
        LoggerFactory.getLogger(this.getClass).info(s"Finished deploying PhantomJS from $uri")
      }
      else {
        // no need to deploy
        LoggerFactory.getLogger(this.getClass).info(s"PhantomJS already exists, no need to deploy")
      }

      assert(isDeployedOnWorkers(spooky))
    }

    def isDeployedOnWorkers(spooky: SpookyContext): Boolean = {
      val isDeployedOnWorkers: Boolean = {

        val sc = spooky.sqlContext.sparkContext
        val pathRDD: RDD[Option[String]] = sc.exePerCore {
          val pathOpt = SpookyUtils.validateLocalPath(path(spooky))
          pathOpt
        }
        pathRDD
          .map(_.nonEmpty)
          .reduce(_ && _)
      }
      isDeployedOnWorkers
    }

    @transient lazy val baseCaps = {
      val baseCaps = new DesiredCapabilities(BrowserType.PHANTOMJS, "", Platform.ANY)

      baseCaps.setJavascriptEnabled(true); //< not really needed: JS enabled by default
      baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS, true)
      //  baseCaps.setCapability(CapabilityType.HAS_NATIVE_EVENTS, false)
      baseCaps.setCapability(TAKES_SCREENSHOT, true)
      baseCaps.setCapability(ACCEPT_SSL_CERTS, true)
      baseCaps.setCapability(SUPPORTS_ALERTS, true)
      baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "loadImages", loadImages)
      baseCaps
    }

    //    baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

    def newCap(spooky: SpookyContext, extra: Option[Capabilities] = None): DesiredCapabilities = {
      val pathStr = path(spooky)

      baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, pathStr)
      baseCaps.setCapability (
        PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout",
        spooky.conf.remoteResourceTimeout.toMillis
      )

      val userAgent = spooky.conf.userAgentFactory
      if (userAgent != null) baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)

      val proxy = spooky.conf.proxy()

      if (proxy != null)
        baseCaps.setCapability(
          PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
          Array("--proxy=" + proxy.addr + ":" + proxy.port, "--proxy-type=" + proxy.protocol)
        )

      baseCaps.merge(extra.orNull)
    }

    //called from executors
    override def _createImpl(session: Session): CleanWebDriver = {

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

    override def _createImpl(session: Session): CleanWebDriver = {

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

  case class Python(
                     path: String = "python"
                   ) extends PythonDriverFactory {

    override def _createImpl(session: Session): PythonDriver = {
      PythonDriver(path)
    }
  }
}