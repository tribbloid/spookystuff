package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.Capabilities
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}

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

  override def verifyDriverUsable(path: Path): Unit = {
    val service = new GeckoDriverService.Builder()
      .usingDriverExecutable(path.toFile)
      .usingAnyFreePort()
      .build()

    val options = new FirefoxOptions()
    options.addArguments("-headless")

    val driver = new FirefoxDriver(service, options)

    try {
      driver.get(HTML_URL)
      assert(driver.getTitle != null)
      assert(driver.getTitle.nonEmpty)
    } finally {
      driver.quit()
      service.stop()
    }
  }
}
