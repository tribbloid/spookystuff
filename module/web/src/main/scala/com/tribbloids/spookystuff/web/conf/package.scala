package com.tribbloids.spookystuff.web

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.multiverse.{CanEqual, Projection}
import com.tribbloids.spookystuff.agent.{Agent, WebProxySetting}
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.web.agent.{CleanWebDriver, WebDriverBundle}
import org.openqa.selenium.chrome.{ChromeDriver, ChromeDriverService, ChromeOptions}
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{Proxy, WebDriver}

import scala.reflect.ClassTag

package object conf {

  type WebDriverFactory = DriverFactory[CleanWebDriver]

  case object WebDriverFactory {

    abstract class Base extends DriverFactory.Transient[CleanWebDriver] {

      Web.enableOnce

      override def factoryReset(driver: CleanWebDriver): Unit = {
        driver.get("about:blank")
      }
    }

    class Impl[+T <: WebDriver: ClassTag](
        builder: Unit :=> (DriverService, T)
    ) extends Base
        with Projection.Equals {

      {
        canEqualProjections ++= Seq(
          CanEqual.Native on implicitly[ClassTag[T]],
          CanEqual.Native on builder.definedAt
        )
      }

      final override lazy val toString: String = {

        val driverName = implicitly[ClassTag[T]].runtimeClass.getSimpleName
        s"WebDriverFactory[$driverName]"
      }

      override def _createImpl(agent: Agent, lifespan: Lifespan): CleanWebDriver = {

        val (service, driver) = builder {}
        val bundle = WebDriverBundle(service, driver)

        val result = new CleanWebDriver(bundle, _lifespan = lifespan)
        result
      }
    }

    object Args {

      lazy val shared = Seq(
        "--no-sandbox"
      )
    }

    case object Chrome {

      def apply(
          option: ChromeOptions = {
            val args = Args.shared :+ "--headless=new"

            new ChromeOptions()
              .addArguments(args*)
          }
      ): Impl[ChromeDriver] = {

        new Impl(() => {

          val driverExecutable = new java.io.File("drivers/chromedriver-linux64/chromedriver")
          lazy val service = new ChromeDriverService.Builder()
            .usingDriverExecutable(driverExecutable)
            .usingAnyFreePort()
            .build()

          val driver = new ChromeDriver(service, option)
          service -> driver

        })

      }

      lazy val default: Impl[ChromeDriver] = apply()
    }

    case object Firefox {

      def apply(
          option: FirefoxOptions = {

            val args = Args.shared :+ "-headless"

            new FirefoxOptions()
              .addArguments(args*)
          }
      ): Impl[FirefoxDriver] = {

        new Impl(() => {

          val service = new GeckoDriverService.Builder()
            .usingAnyFreePort()
            .build()
          val driver = new FirefoxDriver(service, option)
          service -> driver
        })
      }

      lazy val default: Impl[FirefoxDriver] = apply(
      )
    }

    def default: Impl[ChromeDriver] = Chrome.default

    def asSeleniumProxy(s: WebProxySetting): Proxy = {
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
