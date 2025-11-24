package com.tribbloids.spookystuff.web

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.multiverse.{CanEqual, Projection}
import ai.acyclic.prover.commons.spark.Envs
import ai.acyclic.prover.commons.util.PathMagnet
import ai.acyclic.prover.commons.util.PathMagnet.LocalFSPath
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.agent.{Agent, WebProxySetting}
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.web.agent.WebDriverBundle.Lt
import com.tribbloids.spookystuff.web.agent.{CleanWebDriver, WebDriverBundle, WebDriverDeployment}
import org.openqa.selenium.chrome.{ChromeDriver, ChromeDriverService, ChromeOptions}
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}
import org.openqa.selenium.remote.NoSuchDriverException
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{Capabilities, Proxy, WebDriver, WebDriverException}

import java.nio.file.Path
import scala.reflect.ClassTag

package object conf {

  type WebDriverFactory = DriverFactory[CleanWebDriver]

  case object WebDriverFactory {

    final val DEFAULT_DRIVER_DIR: LocalFSPath = Envs.USER_HOME \\ ".spookystuff" \\ "webdriver"

    lazy val sharedArgs = Seq(
      "--no-sandbox"
    )

    abstract class Base extends DriverFactory.Transient[CleanWebDriver] {

      Web.enableOnce

      override def factoryReset(driver: CleanWebDriver): Unit = {
        driver.get("about:blank")
      }
    }

    abstract class Typed[T <: WebDriver: ClassTag] extends Base {

      lazy val executableDir: LocalFSPath = DEFAULT_DRIVER_DIR

      def option: Capabilities

      override def deployGlobally(spooky: SpookyContext): Unit =
        super.deployGlobally(spooky) // TODO: need global deployment to minimise download time

      @transient lazy val localDeployment: WebDriverDeployment =
        WebDriverDeployment.fromRootDir(executableDir, option)

      def getBundle: WebDriverBundle.Lt[T]

      final override lazy val toString: String = {

        val driverName = implicitly[ClassTag[T]].runtimeClass.getSimpleName
        s"WebDriverFactory[$driverName]"
      }

      override def _createImpl(agent: Agent, lifespan: Lifespan): CleanWebDriver = {

        val bundle = getBundle

        try {
          bundle.driver
        } catch {
          case e: WebDriverException =>
            // deploy web driver
            localDeployment.deploy()
            bundle.driver
        }

        val result = new CleanWebDriver(bundle, _lifespan = lifespan)
        result
      }
    }

    case class Chrome(
        option: ChromeOptions = {
          val args = sharedArgs :+ "--headless=new"

          new ChromeOptions()
            .addArguments(args*)
        }
    ) extends Typed[ChromeDriver] {

      override def getBundle: WebDriverBundle.Lt[ChromeDriver] = {

        lazy val service: ChromeDriverService = new ChromeDriverService.Builder()
          .usingDriverExecutable(localDeployment.target.toFile)
          .usingAnyFreePort()
          .build()

        WebDriverBundle.Chrome(service, option)
      }
    }

    case object Chrome {

      lazy val default: Chrome = apply()
    }

    case class Firefox(
        option: FirefoxOptions = {

          val args = sharedArgs :+ "-headless"

          new FirefoxOptions()
            .addArguments(args*)
        }
    ) extends Typed[FirefoxDriver] {

      override def getBundle: WebDriverBundle.Lt[FirefoxDriver] = {

        val service = new GeckoDriverService.Builder()
          .usingDriverExecutable(localDeployment.target.toFile)
          .usingAnyFreePort()
          .build()

        WebDriverBundle.Firefox(service, option)
      }
    }

    case object Firefox {

      lazy val default: Firefox = apply()
    }

    def default: Base = Chrome.default

    def asSeleniumProxy(s: WebProxySetting): Proxy = { // TODO: set this in browser capabilities
      val seleniumProxy: Proxy = new Proxy
      seleniumProxy.setProxyType(Proxy.ProxyType.MANUAL)
      val proxyStr: String = s"${s.addr}:${s.port}"
      seleniumProxy.setHttpProxy(proxyStr)
      seleniumProxy.setSslProxy(proxyStr)
      seleniumProxy.setSocksProxy(proxyStr)
      seleniumProxy
    }
  }

}
