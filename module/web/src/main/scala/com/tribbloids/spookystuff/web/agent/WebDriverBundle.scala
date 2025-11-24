package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.chrome.{ChromeDriver, ChromeDriverService, ChromeOptions}
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{Capabilities, WebDriver}

trait WebDriverBundle {

  type Driver <: WebDriver

  def service: DriverService
  def option: Capabilities
  def driver: Driver
}

object WebDriverBundle {

  type Lt[T] = WebDriverBundle { type Driver <: T }

  case class Chrome(
      service: ChromeDriverService,
      option: ChromeOptions
  ) extends WebDriverBundle {

    override type Driver = ChromeDriver

    override def driver: ChromeDriver = new ChromeDriver(service, option)
  }

  case class Firefox(
      service: GeckoDriverService,
      option: FirefoxOptions
  ) extends WebDriverBundle {

    override type Driver = FirefoxDriver

    override def driver: FirefoxDriver = new FirefoxDriver(service, option)
  }
}
