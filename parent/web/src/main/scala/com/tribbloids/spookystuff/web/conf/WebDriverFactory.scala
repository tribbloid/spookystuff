package com.tribbloids.spookystuff.web.conf

import ai.acyclic.prover.commons.spark.Envs
import com.gargoylesoftware.htmlunit.BrowserVersion
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.dsl.BinaryDeployment
import com.tribbloids.spookystuff.agent.{Agent, WebProxySetting}
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.ConfUtils
import com.tribbloids.spookystuff.web.agent.CleanWebDriver
import org.apache.commons.io.FileUtils
import org.openqa.selenium.Proxy
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}

import java.io.File

abstract class WebDriverFactory extends DriverFactory.Transient[CleanWebDriver] {

  Web.enableOnce

  override def factoryReset(driver: CleanWebDriver): Unit = {
    driver.get("about:blank")
  }
}

object WebDriverFactory {

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

    override def _createImpl(agent: Agent, lifespan: Lifespan): CleanWebDriver = {

      val caps = DesiredCapabilitiesView.default.Imported(agent.spooky).htmlUnit

      val self = new HtmlUnitDriver(browserV)
      self.setJavascriptEnabled(true)
      self.setProxySettings(Proxy.extractFrom(caps))
      val result = new CleanWebDriver(self, _lifespan = lifespan)

      result
    }
  }

  case class PhantomJSDeployment(
      override val targetPath: String = PhantomJS.DEFAULT_PATH,
      override val repositoryURI: String = {

        val os = System.getProperty("os.name").toLowerCase
        //        val osVersion = System.getProperty("os.version")
        val osArch = System.getProperty("os.arch")

        (os, osArch) match {

          case ("linux", "amd64") =>
            PhantomJS.RepoURL.linux_amd64
          case ("windows", "amd64") =>
            PhantomJS.RepoURL.windows_amd64
          case ("mac os x", _) =>
            PhantomJS.RepoURL.apple
        }
      }
  ) extends BinaryDeployment {}

  object PhantomJS {

    object RepoURL {

      final val linux_amd64 = "https://storage.googleapis.com/ci_public/phantom_JS/Linux_amd64/phantomjs"
      final val windows_amd64 = "https://storage.googleapis.com/ci_public/phantom_JS/MacOS/phantomjs"
      final val apple = "https://storage.googleapis.com/ci_public/phantom_JS/Windows_amd64/phantomjs.exe"
    }

    @transient lazy val DEFAULT_PATH: String = {
      ConfUtils.getOrDefault(
        "phantomjs.path",
        Envs.USER_HOME \\ ".spookystuff" \\ "phantomjs"
      )
    }

    def forceDelete(dst: String): Unit = this.synchronized {
      val dstFile = new File(dst)
      FileUtils.forceDelete(dstFile)
    }

    lazy val defaultBuilder: PhantomJSDriverService.Builder = {

      new PhantomJSDriverService.Builder()
        .usingCommandLineArguments(
          Array(
            "--webdriver-loglevel=WARN",
            "--verbose",
            "--log-path=fuckyou.log"
          )
        )
//        .usingCommandLineArguments(Array.empty)
    }
  }

  case class PhantomJS(
      deploy: SpookyContext => BinaryDeployment = { _ =>
        PhantomJSDeployment()
      },
      loadImages: Boolean = false
  ) extends WebDriverFactory {

    override def deployGlobally(spooky: SpookyContext): Unit = {

      val deployment = deploy(spooky)

      deployment.OnSparkDriver(spooky.sparkContext).addSparkFile
    }

    case class DriverCreation(agent: Agent, lifespan: Lifespan) {

      val deployment: BinaryDeployment = deploy(agent.spooky)

      private val browserPath = deployment.deploydPath

      val caps: DesiredCapabilitiesView = {

        val result = DesiredCapabilitiesView.default.Imported(agent.spooky).phantomJS
        result.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, browserPath)
        result
      }

      lazy val service: PhantomJSDriverService = {

        val deployment = deploy(agent.spooky)

        val proxyOpt = Option(agent.spooky.conf.webProxy(())).map { v =>
          asSeleniumProxy(v)
        }

        import scala.jdk.CollectionConverters._

        var builder = PhantomJS.defaultBuilder
          .usingAnyFreePort()
          .usingPhantomJSExecutable(new File(browserPath))
          .withEnvironment(
            Map(
              "OPENSSL_CONF" -> "/dev/null" // https://github.com/bazelbuild/rules_closure/issues/351
            ).asJava
          )
//          .withLogFile(new File(s"${agent.Log.dirPath}/PhantomJSDriver.log"))
          .withLogFile(new File("PhantomJSDriver.log"))
          //        .withLogFile(new File("/dev/null"))
          .usingGhostDriverCommandLineArguments(
            Array(s"""service_log_path="${agent.Log.dirPath}/GhostDriver.log"""")
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
    override def _createImpl(agent: Agent, lifespan: Lifespan): CleanWebDriver = PhantomJS.synchronized {
      // synchronized to avoid 2 builders competing for the same port
      val creation = DriverCreation(agent, lifespan)
      creation.cleanDriver
    }
  }
}
