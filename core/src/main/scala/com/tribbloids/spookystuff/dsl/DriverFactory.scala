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
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.{PythonDriver, _}
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.io.LocalResolver
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan}
import com.tribbloids.spookystuff.utils.{ConfUtils, SpookyUtils, TreeThrowable}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkFiles, TaskContext}
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.CapabilityType._
import org.openqa.selenium.remote.{BrowserType, CapabilityType, DesiredCapabilities}
import org.openqa.selenium.{Capabilities, Platform, Proxy}
import org.slf4j.LoggerFactory

import scala.util.Try

//local to TaskID, if not exist, local to ThreadID
//for every new driver created, add a taskCompletion listener that salvage it.
//TODO: get(session) should have 2 impl:
// if from the same session release the existing one immediately.
// if from a different session but same taskAttempt wait for the old one to be released.
// in any case it should ensure 1 taskAttempt only has 1 active driver
//TODO: delay Future-based waiting control until asynchronous Action exe is implemented. Right now it works just fine
sealed abstract class DriverFactory[+T] extends Serializable {

  // If get is called again before the previous driver is released, the old driver is destroyed to create a new one.
  // this is to facilitate multiple retries
  def dispatch(session: Session): T
  def release(session: Session): Unit

  def driverLifespan(session: Session): Lifespan = Lifespan.TaskOrJVM(ctxFactory = () => session.lifespan.ctx)

  def deployGlobally(spooky: SpookyContext): Unit = {}
}

abstract sealed class WebDriverFactory extends DriverFactories.Transient[CleanWebDriver] {

  override def factoryReset(driver: CleanWebDriver): Unit = {
    driver.get("about:blank")
  }
}

abstract sealed class PythonDriverFactory extends DriverFactories.Transient[PythonDriver] {

  override def factoryReset(driver: PythonDriver): Unit = {}
}

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

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * session local
    * @tparam T AutoCleanable preferred
    */
  abstract class Transient[T] extends DriverFactory[T] {

    // session -> driver
    // cleanup: this has no effect whatsoever
    @transient lazy val sessionLocals: ConcurrentMap[Session, T] = ConcurrentMap()

    def dispatch(session: Session): T = {
      release(session)
      val driver = create(session)
      sessionLocals += session -> driver
      driver
    }

    final def create(session: Session): T = {
      _createImpl(session, driverLifespan(session))
    }

    def _createImpl(session: Session, lifespan: Lifespan): T

    def factoryReset(driver: T): Unit

    def release(session: Session): Unit = {
      val existingOpt = sessionLocals.remove(session)
      existingOpt.foreach { driver =>
        destroy(driver, session.taskContextOpt)
      }
    }

    final def destroy(driver: T, tcOpt: Option[TaskContext]): Unit = {
      driver match {
        case v: Cleanable => v.tryClean()
        case _            =>
      }
    }

    final lazy val taskLocal = TaskLocal(this)
  }

  /**
    * delegate create & destroy to PerSessionFactory
    * first get() create a driver as usual
    * calling get() without release() reboot the driver
    * first release() return driver to the pool to be used by the same Spark Task
    * call any function with a new Spark Task ID will add a cleanup TaskCompletionListener to the Task that destroy all drivers
    */
  case class TaskLocal[T](
      delegate: Transient[T]
  ) extends DriverFactory[T] {

    //taskOrThreadIDs -> (driver, busy)
    @transient lazy val taskLocals: ConcurrentMap[Seq[Any], DriverStatus[T]] = {
      ConcurrentMap()
    }

    override def dispatch(session: Session): T = {

      val ls = driverLifespan(session)
      val taskLocalOpt = taskLocals.get(ls.batchIDs)

      def newDriver: T = {
        val fresh = delegate.create(session)
        taskLocals.put(ls.batchIDs, new DriverStatus(fresh))
        fresh
      }

      taskLocalOpt
        .map { status =>
          def recreateDriver: T = {
            delegate.destroy(status.self, session.taskContextOpt)
            newDriver
          }

          if (!status.isBusy) {
            try {
              delegate.factoryReset(status.self)
              status.isBusy = true
              status.self
            } catch {
              case e: Throwable =>
                recreateDriver
            }
          } else {
            // TODO: should wait until its no longer busy, instead of destroying it.
            recreateDriver
          }
        }
        .getOrElse {
          newDriver
        }
    }

    override def release(session: Session): Unit = {

      val ls = driverLifespan(session)
      val opt = taskLocals.get(ls.batchIDs)
      opt.foreach { status =>
        status.isBusy = false
      }
    }

    override def deployGlobally(spooky: SpookyContext): Unit = delegate.deployGlobally(spooky)
  }

  object PhantomJS {

    // TODO: separate win/mac/linux32/linux64 versions
    final val HTTP_RESOURCE_URI = "https://s3-us-west-1.amazonaws.com/spooky-bin/phantomjs-linux/phantomjs"

    final def uri2fileName(path: String): String = path.split("/").last

    final def DEFAULT_PATH: String = System.getProperty("user.home") \\ ".spookystuff" \\ "phantomjs"

    def verifyExe(pathStr: String): Try[String] = Try {
      val isExists = LocalResolver.execute(pathStr).satisfy { v =>
        v.getLength >= 1024 * 1024 * 60
      }
      assert(isExists, s"PhantomJS executable at $pathStr doesn't exist")
      pathStr
    }

    def defaultGetLocalURI: SpookyContext => String = { _ =>
      ConfUtils.getOrDefault("phantomjs.path", DEFAULT_PATH)
    }

    def forceDelete(dst: String): Unit = this.synchronized {
      val dstFile = new File(dst)
      FileUtils.forceDelete(dstFile)
    }
  }

  case class PhantomJS(
      getLocalURI: SpookyContext => String = PhantomJS.defaultGetLocalURI,
      getRemoteURI: SpookyContext => String = _ => PhantomJS.HTTP_RESOURCE_URI,
      loadImages: Boolean = false,
      redeploy: Boolean = false
  ) extends WebDriverFactory {

    override def deployGlobally(spooky: SpookyContext): Unit = {
      try {
        //        spooky.sparkContext.clearFiles()
        _deployGlobally(spooky)
      } catch {
        case e: Throwable =>
          //          spooky.sparkContext.clearFiles()
          throw new UnsupportedOperationException(
            s"${this.getClass.getSimpleName} cannot find resource for deployment, " +
              s"please provide Internet Connection or deploy manually",
            e
          )
      }
    }

    def copySparkFile2Local(sparkFile: String, dstStr: String): Option[Any] = {

      val srcStr = SparkFiles.get(sparkFile)
      val srcFile = new File(srcStr)
      val dstFile = new File(dstStr)
      SpookyUtils.ifFileNotExist(dstStr) {
        SpookyUtils.treeCopy(srcFile.toPath, dstFile.toPath)
      }
    }

    /**
      * can only used on driver
      */
    def _deployGlobally(spooky: SpookyContext): Unit = {
      val sc = spooky.sparkContext
      val localURI = getLocalURI(spooky)

      def localURITry: Try[String] = PhantomJS.verifyExe(localURI)

      if (localURITry.isFailure) {
        val remoteURI = getRemoteURI(spooky)

        LoggerFactory.getLogger(this.getClass).info(s"Downloading PhantomJS from Internet ($remoteURI)")

        sc.addFile(remoteURI)
        val fileName = PhantomJS.uri2fileName(remoteURI)

        copySparkFile2Local(fileName, localURI)
      }

      val _localURI = localURITry.get
      val _localFileName = PhantomJS.uri2fileName(_localURI)

      Try {
        SparkFiles.get(_localFileName)
      }.recover {
        case e: Throwable =>
          sc.addFile(_localURI)
          LoggerFactory.getLogger(this.getClass).info(s"PhantomJS Deployed to $localURI")
      }
    }

    /**
      * do nothing if local already exists.
      * otherwise download from driver
      * if failed download from remote
      * @param spooky
      */
    def _deployLocally(spooky: SpookyContext): String = {
      val localURI = getLocalURI(spooky)
      def localURITry = PhantomJS.verifyExe(localURI)

      val result: Option[String] = TreeThrowable.|||^(
        Seq(
          //already exists
          { () =>
            localURITry.get
          },
          //copy from Spark local file
          { () =>
            val fileName = PhantomJS.uri2fileName(localURI)
            copySparkFile2Local(fileName, localURI)
            localURITry.get
          },
          //copy from Spark remote file
          { () =>
            val remoteURI = getRemoteURI(spooky)
            val fileName = PhantomJS.uri2fileName(remoteURI)
            copySparkFile2Local(fileName, localURI)
            localURITry.get
          }
        ))
      result.get
    }

    @transient lazy val baseCaps: DesiredCapabilities = {
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
      val newCaps = new DesiredCapabilities(baseCaps)

      val pathStr = _deployLocally(spooky)

      newCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, pathStr)
      newCaps.setCapability(
        PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout",
        spooky.spookyConf.remoteResourceTimeout.toMillis
      )
      importHeaders(newCaps, spooky)

      val proxy = spooky.spookyConf.webProxy()

      if (proxy != null)
        newCaps.setCapability(
          PhantomJSDriverService.PHANTOMJS_CLI_ARGS,
          Array("--proxy=" + proxy.addr + ":" + proxy.port, "--proxy-type=" + proxy.protocol)
        )

      newCaps.merge(extra.orNull)
    }

    //called from executors
    override def _createImpl(session: Session, lifespan: Lifespan): CleanWebDriver = {
      val self = new PhantomJSDriver(newCap(session.spooky))
      new CleanWebDriver(self, lifespan)
    }
  }

  def importHeaders(caps: DesiredCapabilities, spooky: SpookyContext): Unit = {
    val headersOpt = Option(spooky.spookyConf.httpHeadersFactory).flatMap(v => Option(v.apply()))
    headersOpt.foreach { headers =>
      headers.foreach {
        case (k, v) =>
          caps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + k, v)
      }
    }
  }

  case class HtmlUnit(
      browser: BrowserVersion = BrowserVersion.getDefault
  ) extends WebDriverFactory {

    @transient lazy val baseCaps: DesiredCapabilities = new DesiredCapabilities(BrowserType.HTMLUNIT, "", Platform.ANY)

    def newCaps(capabilities: Capabilities, spooky: SpookyContext): DesiredCapabilities = {
      val newCaps = new DesiredCapabilities(baseCaps)

      importHeaders(newCaps, spooky)

      val proxy: WebProxySetting = spooky.spookyConf.webProxy()

      if (proxy != null) {
        newCaps.setCapability(PROXY, proxy.toSeleniumProxy)
      }

      newCaps.merge(capabilities)
    }

    override def _createImpl(session: Session, lifespan: Lifespan): CleanWebDriver = {

      val cap = newCaps(null, session.spooky)
      val self = new HtmlUnitDriver(browser)
      self.setJavascriptEnabled(true)
      self.setProxySettings(Proxy.extractFrom(cap))
      val driver = new CleanWebDriver(self, lifespan)

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
      getExecutable: SpookyContext => String
  ) extends PythonDriverFactory {

    override def _createImpl(session: Session, lifespan: Lifespan): PythonDriver = {
      val exeStr = getExecutable(session.spooky)
      new PythonDriver(exeStr, _lifespan = lifespan)
    }
  }

  object Python2 extends Python((_: SpookyContext) => "python2")
  object Python3 extends Python((_: SpookyContext) => "python3")
}
