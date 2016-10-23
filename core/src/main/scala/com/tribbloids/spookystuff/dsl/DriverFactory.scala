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

//local to TaskID, if not exist, local to ThreadID
//for every new driver created, add a taskCompletion listener that salvage it.
//TODO: get(session) should have 2 impl:
// if from the same session release the existing one immediately.
// if from a different session but same taskAttempt wait for the old one to be released.
// in any case it should ensure 1 taskAttempt only has 1 active driver
//TODO: delay Future-based waiting control until asynchronous Action exe is implemented. Right now it works just fine
abstract sealed class DriverFactory[+T] extends Serializable {

  // If get is called again before the previous driver is released, the old driver is destroyed to create a new one.
  // this is to facilitate multiple retries
  def get(session: Session): T
  def release(session: Session): Unit

  final def deploy(spooky: SpookyContext): Unit = {
    try {
      _deploy(spooky)
    }
    catch {
      case e: Throwable =>
        throw DriverNotDeployedException(
          s"${this.getClass.getSimpleName} cannot find resource for deployment, " +
            s"please provide Internet Connection or deploy manually",
          e
        )
    }
  }
  protected[dsl] def _deploy(spooky: SpookyContext): Unit = {}
}

/**
  * session local
  * @tparam T AutoCleanable preferred
  */
abstract sealed class TransientFactory[T] extends DriverFactory[T] {

  // session -> driver
  // cleanup: this has no effect whatsoever
  @transient lazy val sessionLocals: ConcurrentMap[Session, T] = ConcurrentMap()

  def get(session: Session): T = {
    release(session)
    val driver = create(session)
    sessionLocals += session -> driver
    driver
  }

  final def create(session: Session): T = {
    val created = _createImpl(session)

    created
  }

  def _createImpl(session: Session): T

  def factoryReset(driver: T): Unit

  def release(session: Session): Unit = {
    val existingOpt = sessionLocals.remove(session)
    existingOpt.foreach {
      driver =>
        destroy(driver, session.taskContextOpt)
    }
  }

  final def destroy(driver: T, tcOpt: Option[TaskContext]): Unit = {
    driver match {
      case v: AutoCleanable => v.finalize()
      case _ =>
    }
  }

  final lazy val pooling = PoolingFactory(this)
}

/**
  * delegate create & destroy to PerSessionFactory
  * first get() create a driver as usual
  * calling get() without release() reboot the driver
  * first release() return driver to the pool to be used by the same Spark Task
  * call any function with a new Spark Task ID will add a cleanup TaskCompletionListener to the Task that destroy all drivers
  */
case class PoolingFactory[T](
                              delegate: TransientFactory[T]
                            ) extends DriverFactory[T] {

  import DriverFactories.DriverStatus

  //taskOrThreadID -> (driver, busy)
  @transient lazy val taskOrThreadLocals: ConcurrentMap[TaskOrThread, DriverStatus[T]] = ConcurrentMap()

  //  override def _clean(): Unit = {
  //    pool.values.foreach {
  //      _.driver match {
  //        case d: AutoCleanable =>
  //          d.finalize()
  //        case _ =>
  //      }
  //    }
  //  }

  override def get(session: Session): T = {

    val opt = taskOrThreadLocals.get(session.taskOrThread)

    def refreshDriver: T = {
      val fresh = delegate.create(session)
      taskOrThreadLocals.put(session.taskOrThread, new DriverStatus(fresh, isBusy = true))
      fresh
    }

    opt
      .map {
        tuple =>
          if (!tuple.isBusy) {
            try{
              delegate.factoryReset(tuple.driver)
              tuple.isBusy = true
              tuple.driver
            }
            catch {
              case e: Throwable =>
                delegate.destroy(tuple.driver, session.taskContextOpt)
                refreshDriver
            }
          }
          else {
            // TODO: should wait until its no longer busy, instead of destroying it.
            delegate.destroy(tuple.driver, session.taskContextOpt)
            refreshDriver
          }
      }
      .getOrElse {
        refreshDriver
      }
  }

  override def release(session: Session): Unit = {

    val opt = taskOrThreadLocals.get(session.taskOrThread)
    opt.foreach{
      tuple =>
        tuple.isBusy = false
    }
  }

  override def _deploy(spooky: SpookyContext): Unit = delegate._deploy(spooky)
}

abstract sealed class WebDriverFactory extends TransientFactory[CleanWebDriver]{

  override def factoryReset(driver: CleanWebDriver): Unit = {
    driver.get("about:blank")
  }
}

abstract sealed class PythonDriverFactory extends TransientFactory[PythonDriver]{

  override def factoryReset(driver: PythonDriver): Unit = {
  }
}

//TODO: deploy lazily/as failover
case class DriverNotDeployedException(
                                       override val message: String,
                                       override val cause: Throwable
                                     ) extends SpookyException(
  message, cause
)

object DriverFactories {

  //  def taskOrThreadID(tcOpt: Option[TaskContext]): Either[Long, Long] = {
  //    tcOpt
  //      .map{
  //        v =>
  //          Left(v.taskAttemptId())
  //      }
  //      .getOrElse{
  //        Right(Thread.currentThread().getId)
  //      }
  //  }

  class DriverStatus[T](
                         val driver: T,
                         @volatile var isBusy: Boolean
                       )

  import com.tribbloids.spookystuff.utils.SpookyViews._

  object PhantomJS {

    final val HTTP_RESOURCE_URI = "https://s3-us-west-1.amazonaws.com/spooky-bin/phantomjs-linux/phantomjs"

    final def uri2fileName(path: String) = path.split("/").last

    final def DEFAULT_PATH = System.getProperty("user.home") \\ ".spookystuff" \\ "phantomjs"

    def defaultGetPath: SpookyContext => String = {
      _ =>
        SpookyConf.getDefault("phantomjs.path", DEFAULT_PATH)
    }

    def asynchDelete(dst: String): Unit = this.synchronized {
      val dstFile = new File(dst)
      FileUtils.forceDelete(dstFile)
    }
  }

  case class PhantomJS(
                        getPath: SpookyContext => String = PhantomJS.defaultGetPath,
                        loadImages: Boolean = false,
                        redeploy: Boolean = false
                      ) extends WebDriverFactory {

    /**
      * can only used on driver
      */
    override def _deploy(spooky: SpookyContext): Unit = {
      if ((!isDeployedOnWorkers(spooky)) || redeploy) {

        val pathOptOnDriver = SpookyUtils.validateLocalPath(getPath(spooky))
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
          sc.mapPerExecutor {
            val dstStr = getPath(spooky)
            PhantomJS.asynchDelete(dstStr)
          }
            .count()
        }

        sc.mapPerExecutor {
          val srcStr = SparkFiles.get(fileName)
          val dstStr = getPath(spooky)
          val srcFile = new File(srcStr)
          val dstFile = new File(dstStr)
          SpookyUtils.asynchIfNotExist(dstStr) {
            SpookyUtils.universalCopy(srcFile.toPath, dstFile.toPath)
          }
        }
          .count()
        LoggerFactory.getLogger(this.getClass).info(s"Finished deploying PhantomJS from $uri")
      }
      else {
        // no need to deploy
        LoggerFactory.getLogger(this.getClass).debug(s"PhantomJS already exists, no need to deploy")
      }

      assert(isDeployedOnWorkers(spooky))
    }

    def isDeployedOnWorkers(spooky: SpookyContext): Boolean = {
      val isDeployedOnWorkers: Boolean = {

        val sc = spooky.sqlContext.sparkContext
        val pathRDD: RDD[Option[String]] = sc.mapPerExecutor {
          val pathOpt = SpookyUtils.validateLocalPath(getPath(spooky))
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
      val pathStr = getPath(spooky)

      baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, pathStr)
      baseCaps.setCapability (
        PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout",
        spooky.conf.remoteResourceTimeout.toMillis
      )

      val userAgent = spooky.conf.userAgentFactory
      if (userAgent != null) {
        baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent)
      }

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
      CleanWebDriver(
        new PhantomJSDriver(newCap(session.spooky)),
        session.taskOrThread
      )
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
      val self = new HtmlUnitDriver(browser)
      self.setJavascriptEnabled(true)
      self.setProxySettings(Proxy.extractFrom(cap))
      val driver = CleanWebDriver(
        self,
        session.taskOrThread
      )

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
                     getExecutable: SpookyContext => String = _ => "python"
                   ) extends PythonDriverFactory {

    override def _createImpl(session: Session): PythonDriver = {
      val exeStr = getExecutable(session.spooky)
      new PythonDriver(exeStr, taskOrThread = session.taskOrThread)
    }
  }
}