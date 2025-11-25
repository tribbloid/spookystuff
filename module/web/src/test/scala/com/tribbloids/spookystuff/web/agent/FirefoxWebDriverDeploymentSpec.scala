package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.Capabilities
import org.openqa.selenium.firefox.{FirefoxOptions, GeckoDriverService}

import java.nio.file.{Path, Paths}

class FirefoxWebDriverDeploymentSpec extends WebDriverDeploymentSpec {

  // Use a target path in the build directory to avoid polluting source
  override val targetPath: Path = Paths.get("build/test-results/drivers/geckodriver")
  override val capabilities: Capabilities = {
    val options = new FirefoxOptions()
    options.addArguments("-headless")
    options
  }

  override def driverName: String = "geckodriver"

  override def verifyDriverPath(path: Path): WebDriverBundle = {
    val service = new GeckoDriverService.Builder()
      .usingDriverExecutable(path.toFile)
      .usingAnyFreePort()
      .build()

    val browserBinary = WebDriverDeployment.resolvePaths(capabilities)._2.toString

    val options = new FirefoxOptions().merge(capabilities)
    options.setBinary(browserBinary)

    WebDriverBundle.Firefox(service, options)
  }
}
