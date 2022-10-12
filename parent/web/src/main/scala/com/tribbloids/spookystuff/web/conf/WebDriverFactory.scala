package com.tribbloids.spookystuff.web.conf

import com.gargoylesoftware.htmlunit.BrowserVersion
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.dsl.BinaryDeployment
import com.tribbloids.spookystuff.session.{Session, WebProxySetting}
import com.tribbloids.spookystuff.utils.ConfUtils
import com.tribbloids.spookystuff.utils.io.LocalResolver
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.web.session.CleanWebDriver
import org.apache.commons.io.FileUtils
import org.openqa.selenium.Proxy
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}

import java.io.File
import scala.util.Try

abstract class WebDriverFactory extends DriverFactory.Transient[CleanWebDriver] {

  Web.enableOnce

  override def factoryReset(driver: CleanWebDriver): Unit = {
    driver.get("about:blank")
  }
}

object WebDriverFactory {

  import com.tribbloids.spookystuff.utils.CommonViews._

  def asSeleniumProxy(s: WebProxySetting): Proxy = {
    val seleniumProxy: Proxy = new Proxy
    seleniumProxy.setProxyType(Proxy.ProxyType.MANUAL)
    val proxyStr: String = s"${s.addr}:${s.port}"
    seleniumProxy.setHttpProxy(proxyStr)
    seleniumProxy.setSslProxy(proxyStr)
    seleniumProxy.setSocksProxy(proxyStr)
    seleniumProxy
  }

  case class HtmlUnit(
      browserV: BrowserVersion = BrowserVersion.getDefault
  ) extends WebDriverFactory {

    override def _createImpl(session: Session, lifespan: Lifespan): CleanWebDriver = {

      val caps = DesiredCapabilitiesView.default.Imported(session.spooky).htmlUnit

      val self = new HtmlUnitDriver(browserV)
      self.setJavascriptEnabled(true)
      self.setProxySettings(Proxy.extractFrom(caps))
      val result = new CleanWebDriver(self, _lifespan = lifespan)

      result
    }
  }

  case class PhantomJSDeployment(
      override val localPath: String = PhantomJS.defaultLocalPath,
      override val remoteURL: String = PhantomJS.defaultRemoteURL
  ) extends BinaryDeployment {

    override def verifyLocalPath: String = PhantomJS.verifyExe(localPath).get
  }

  object PhantomJS {

    // TODO: separate win/mac/linux32/linux64 versions
    final val defaultRemoteURL = "https://docs.google.com/uc?export=download&id=1tHWQTXy471_MTu5XBYwgvN6zEg741cD8"

    final def DEFAULT_PATH: String = System.getProperty("user.home") \\ ".spookystuff" \\ "phantomjs"

    // TODO: move to BinaryDeployment
    def verifyExe(pathStr: String): Try[String] = Try {
      val isExists = LocalResolver.execute(pathStr).satisfy { v =>
        v.getLength >= 1024 * 1024 * 60
      }
      assert(isExists, s"PhantomJS executable at $pathStr doesn't exist")
      pathStr
    }

    def defaultLocalPath: String = {
      ConfUtils.getOrDefault("phantomjs.path", DEFAULT_PATH)
    }

    def forceDelete(dst: String): Unit = this.synchronized {
      val dstFile = new File(dst)
      FileUtils.forceDelete(dstFile)
    }

    lazy val defaultBuilder: PhantomJSDriverService.Builder = {

      new PhantomJSDriverService.Builder()
        .usingCommandLineArguments(Array("--webdriver-loglevel=WARN"))
//        .usingCommandLineArguments(Array.empty)
    }
  }

  case class PhantomJS(
      deploy: SpookyContext => BinaryDeployment = _ => PhantomJSDeployment(),
      loadImages: Boolean = false
  ) extends WebDriverFactory {

    override def deployGlobally(spooky: SpookyContext): Unit = {

      val deployment = deploy(spooky)

      deployment.OnDriver(spooky.sparkContext).deployOnce
    }

    case class DriverCreation(session: Session, lifespan: Lifespan) {

      val deployment: BinaryDeployment = deploy(session.spooky)

      val caps: DesiredCapabilitiesView = DesiredCapabilitiesView.default.Imported(session.spooky).phantomJS
      caps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, deployment.verifiedLocalPath)

      lazy val service: PhantomJSDriverService = {

        val deployment = deploy(session.spooky)
        val pathStr = deployment.verifiedLocalPath

        val proxyOpt = Option(session.spooky.spookyConf.webProxy()).map { v =>
          asSeleniumProxy(v)
        }

        import scala.collection.JavaConverters._

        var builder = PhantomJS.defaultBuilder
          .usingAnyFreePort()
          .usingPhantomJSExecutable(new File(pathStr))
          .withEnvironment(
            Map(
              "OPENSSL_CONF" -> "/dev/null" // https://github.com/bazelbuild/rules_closure/issues/351
            ).asJava
          )
          .withLogFile(new File(s"${session.SessionLog.dirPath}/PhantomJSDriver.log"))
          //        .withLogFile(new File("/dev/null"))
          .usingGhostDriverCommandLineArguments(
            Array(s"""service_log_path="${session.SessionLog.dirPath}/GhostDriver.log"""")
          )
        //        .usingGhostDriverCommandLineArguments(Array.empty)

        proxyOpt.foreach { proxy =>
          builder = builder.withProxy(proxy)
        }

        builder.build
      }

      //      val driver = new PhantomJSDriver(caps)
      lazy val driver: PhantomJSDriver = new PhantomJSDriver(service, caps)

      lazy val cleanDriver: CleanWebDriver = {

        val result = new CleanWebDriver(driver, Some(service), lifespan)
        result
      }

    }

    // called from executors
    override def _createImpl(session: Session, lifespan: Lifespan): CleanWebDriver = PhantomJS.synchronized {
      // synchronized to avoid 2 builders competing for the same port
      val creation = DriverCreation(session, lifespan)
      creation.cleanDriver
    }
  }
}
