package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{Capabilities, WebDriver}

import java.nio.file.{Path, Paths}

class FirefoxWebDriverDeploymentSpec extends WebDriverDeploymentSpec {

  override def driverName: String = "geckodriver"

  // Use a target path in the build directory to avoid polluting source
  override val targetPath: Path = Paths.get("build/test-results/drivers/geckodriver")

  override val capabilities: Capabilities = {
    val options = new FirefoxOptions()
    options.addArguments("-headless")
    options
  }

  override def newDriver(path: Path): (WebDriver, DriverService) = {
    val service = new GeckoDriverService.Builder()
      .usingDriverExecutable(path.toFile)
      .usingAnyFreePort()
      .build()

    val options = new FirefoxOptions()
    options.addArguments("-headless")

    val driver = new FirefoxDriver(service, options)
    (driver, service)
  }
}
