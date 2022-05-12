package com.tribbloids.spookystuff.web.conf

import com.gargoylesoftware.htmlunit.BrowserVersion
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.dsl.BinaryDeployment
import com.tribbloids.spookystuff.session.{Session, WebProxySetting}
import com.tribbloids.spookystuff.utils.ConfUtils
import com.tribbloids.spookystuff.utils.io.LocalResolver
import com.tribbloids.spookystuff.utils.lifespan.Lifespan
import com.tribbloids.spookystuff.web.session.CleanWebDriver
import org.apache.commons.io.FileUtils
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.CapabilityType._
import org.openqa.selenium.remote.DesiredCapabilities
import org.openqa.selenium.{Capabilities, Platform, Proxy}

import java.io.File
import scala.util.Try

abstract class WebDriverFactory extends DriverFactory.Transient[CleanWebDriver] {

  Web.enableOnce

  override def factoryReset(driver: CleanWebDriver): Unit = {
    driver.get("about:blank")
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

    @transient lazy val baseCaps: DesiredCapabilities = new DesiredCapabilities("htmlunit", "", Platform.ANY)

    def newCaps(capabilities: Capabilities, spooky: SpookyContext): DesiredCapabilities = {
      val newCaps = new DesiredCapabilities(baseCaps)

      importHeaders(newCaps, spooky)

      val proxy: WebProxySetting = spooky.spookyConf.webProxy()

      if (proxy != null) {
        newCaps.setCapability(PROXY, asSeleniumProxy(proxy))
      }

      newCaps.merge(capabilities)
    }

    override def _createImpl(session: Session, lifespan: Lifespan): CleanWebDriver = {

      val cap = newCaps(null, session.spooky)
      val self = new HtmlUnitDriver(browserV)
      self.setJavascriptEnabled(true)
      self.setProxySettings(Proxy.extractFrom(cap))
      val driver = new CleanWebDriver(self, lifespan)

      driver
    }
  }

  case class PhantomJSDeployment(
      override val localPath: String = PhantomJS.defaultLocalPath,
      override val remoteURL: String = PhantomJS.defaultRemoteURL
  ) extends BinaryDeployment {

    override def verifyLocalPath(): String = PhantomJS.verifyExe(localPath).get
  }

  object PhantomJS {

    // TODO: separate win/mac/linux32/linux64 versions
    final val defaultRemoteURL = "https://docs.google.com/uc?export=download&id=1tHWQTXy471_MTu5XBYwgvN6zEg741cD8"

    final def DEFAULT_PATH: String = System.getProperty("user.home") \\ ".spookystuff" \\ "phantomjs"

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
        .usingAnyFreePort()
        .withLogFile(new File("phantomjsdriver.log"))
    }
  }

  case class PhantomJS(
      deploy: SpookyContext => BinaryDeployment = _ => PhantomJSDeployment(),
//      serviceBuilder: PhantomJSDriverService.Builder,
      loadImages: Boolean = false
  ) extends WebDriverFactory {

    override def deployGlobally(spooky: SpookyContext): Unit = {

      val deployment = deploy(spooky)

      deployment.OnDriver(spooky.sparkContext).deployOnce
    }

    @transient lazy val baseCaps: DesiredCapabilities = {
      val baseCaps = new DesiredCapabilities()
      baseCaps.setBrowserName("chrome")
      baseCaps.setPlatform(Platform.ANY)

      baseCaps.setJavascriptEnabled(true); //< not really needed: JS enabled by default
//      baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS, true)
      //  baseCaps.setCapability(CapabilityType.HAS_NATIVE_EVENTS, false)
      baseCaps.setCapability(TAKES_SCREENSHOT, true)
      baseCaps.setCapability(ACCEPT_SSL_CERTS, true)
      baseCaps.setCapability(SUPPORTS_ALERTS, true)
      baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "loadImages", loadImages)
      baseCaps
    }

    //    baseCaps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+"resourceTimeout", Const.resourceTimeout*1000)

    def newCaps(spooky: SpookyContext, extra: Option[Capabilities] = None): DesiredCapabilities = {
      val newCaps = new DesiredCapabilities(baseCaps)

      val deployment = deploy(spooky)

      val pathStr = deployment.verifiedLocalPath

      newCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, pathStr)
      newCaps.setCapability(
        PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout",
        spooky.spookyConf.remoteResourceTimeout.max.toMillis
      )
      importHeaders(newCaps, spooky)

      val proxyOpt = Option(spooky.spookyConf.webProxy()).map { v =>
        asSeleniumProxy(v)
      }

      proxyOpt.foreach { proxy =>
        newCaps.setCapability("proxy", proxy)
      }

      newCaps.merge(extra.orNull)
    }

    //called from executors
    override def _createImpl(session: Session, lifespan: Lifespan): CleanWebDriver = {
      val caps = newCaps(session.spooky)

      lazy val service: PhantomJSDriverService = {

        val deployment = deploy(session.spooky)
        val pathStr = deployment.verifiedLocalPath

        val proxyOpt = Option(session.spooky.spookyConf.webProxy()).map { v =>
          asSeleniumProxy(v)
        }

        import scala.collection.JavaConverters._

        var builder = PhantomJS.defaultBuilder
          .usingPhantomJSExecutable(new File(pathStr))
          .withEnvironment(
            Map(
              "OPENSSL_CONF" -> "/dev/null" //https://github.com/bazelbuild/rules_closure/issues/351
            ).asJava
          )
        proxyOpt.foreach { proxy =>
          builder = builder.withProxy(proxy)
        }

        builder.build
      }

      val self = new PhantomJSDriver(service, caps)
      new CleanWebDriver(self, lifespan)
    }
  }
}
