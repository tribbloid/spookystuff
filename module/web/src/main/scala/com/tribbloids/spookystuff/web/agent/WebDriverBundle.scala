package com.tribbloids.spookystuff.web.agent

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import org.openqa.selenium.chrome.{ChromeDriver, ChromeDriverService, ChromeOptions}
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{Capabilities, WebDriver}

trait WebDriverBundle extends NOTSerializable with AutoCloseable {

  type Driver <: WebDriver
  final lazy val driver: Driver = {

    val out = getDriver
    existingDriver = out
    existingDriver
  }
  val service: DriverService
  val option: Capabilities

  @volatile private var existingDriver: Driver = _

  override def close(): Unit = {

    try Option(existingDriver).foreach(_.close())
    finally {
      service.close()
    }
  }

  protected def getDriver: Driver
}

object WebDriverBundle {

  type Lt[T] = WebDriverBundle { type Driver <: T }

  case class Chrome(
      service: ChromeDriverService,
      option: ChromeOptions
  ) extends WebDriverBundle {

    override type Driver = ChromeDriver

    override def getDriver: ChromeDriver = new ChromeDriver(service, option)

  }

  case class Firefox(
      service: GeckoDriverService,
      option: FirefoxOptions
  ) extends WebDriverBundle {

    override type Driver = FirefoxDriver

    override def getDriver: FirefoxDriver = new FirefoxDriver(service, option)
  }
}
