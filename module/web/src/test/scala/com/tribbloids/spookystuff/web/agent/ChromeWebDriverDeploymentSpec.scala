package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{Capabilities, WebDriver}

import java.nio.file.{Path, Paths}

class ChromeWebDriverDeploymentSpec extends WebDriverDeploymentSpec {

  override def driverName: String = "chromedriver"

  // Use a target path in the build directory to avoid polluting source
  override val targetPath: Path = Paths.get("build/test-results/drivers/chromedriver")

  override val capabilities: Capabilities = {
    val options = new ChromeOptions()
    options.addArguments("--headless=new")
    options
  }

  override def newDriver(path: Path): (WebDriver, DriverService) = {
    val service = new org.openqa.selenium.chrome.ChromeDriverService.Builder()
      .usingDriverExecutable(path.toFile)
      .usingAnyFreePort()
      .build()

    val options = new ChromeOptions().merge(capabilities)
    options.addArguments("--headless=new")
    options.addArguments("--no-sandbox")
    options.addArguments("--disable-dev-shm-usage")

    val driver = new ChromeDriver(service, options)
    (driver, service)
  }
}
