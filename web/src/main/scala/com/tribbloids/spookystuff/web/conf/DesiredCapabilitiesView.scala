package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.web.conf.WebDriverFactory.asSeleniumProxy
import org.openqa.selenium.Platform
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.logging.LoggingPreferences
import org.openqa.selenium.phantomjs.PhantomJSDriverService
import org.openqa.selenium.remote.CapabilityType.{ACCEPT_SSL_CERTS, PROXY, SUPPORTS_ALERTS, TAKES_SCREENSHOT}
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}

import scala.language.implicitConversions

object DesiredCapabilitiesView {

  object Defaults {
    lazy val logPrefs: LoggingPreferences = {

      val result = new LoggingPreferences()

//      result.enable(LogType.BROWSER, Level.SEVERE)
      result
    }

    lazy val caps: DesiredCapabilities = {

      val caps = new DesiredCapabilities()
      caps.setPlatform(Platform.ANY)

      caps.setJavascriptEnabled(true); //< not really needed: JS enabled by default

      caps.setCapability(CapabilityType.LOGGING_PREFS, logPrefs)

      //      baseCaps.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS, true)
      //      baseCaps.setCapability(CapabilityType.HAS_NATIVE_EVENTS, false)
      caps.setCapability(TAKES_SCREENSHOT, true)
      caps.setCapability(ACCEPT_SSL_CERTS, true)
      caps.setCapability(SUPPORTS_ALERTS, true)
      caps
    }
  }

  lazy val default: DesiredCapabilitiesView = DesiredCapabilitiesView(Defaults.caps)

  implicit def unbox(v: DesiredCapabilitiesView): DesiredCapabilities = v.self
}

case class DesiredCapabilitiesView(
    self: DesiredCapabilities
) {

  def fork(): DesiredCapabilitiesView = this.copy(new DesiredCapabilities(self))

  def configure(fn: DesiredCapabilities => Unit): this.type = {
    fn(self)
    this
  }

  case class Imported(
      spooky: SpookyContext,
      loadImage: Boolean = true
  ) {

    def base: DesiredCapabilitiesView = {
      fork().configure { caps =>
        val proxyOpt = Option(spooky.spookyConf.webProxy()).map { v =>
          asSeleniumProxy(v)
        }

        proxyOpt.foreach { proxy =>
          caps.setCapability(PROXY, proxy)
        }

      }
    }

    def htmlUnit: DesiredCapabilitiesView = {
      base.configure { caps =>
        caps.setBrowserName("htmlunit")

        caps.setCapability(HtmlUnitDriver.DOWNLOAD_IMAGES_CAPABILITY, loadImage)
      }
    }

    def phantomJS: DesiredCapabilitiesView = {
      base.configure { caps =>
        caps.setBrowserName("phantomjs")

        caps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "loadImages", loadImage)

        caps.setCapability(
          PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "resourceTimeout",
          spooky.spookyConf.remoteResourceTimeout.max.toMillis
        )

        val headersOpt = Option(spooky.spookyConf.httpHeadersFactory).flatMap(v => Option(v.apply()))
        headersOpt.foreach { headers =>
          headers.foreach {
            case (k, v) =>
              caps.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + k, v)
          }
        }
      }
    }
  }
}
