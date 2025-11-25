package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.Capabilities
import org.openqa.selenium.chrome.ChromeOptions

import java.nio.file.{Path, Paths}

class ChromeWebDriverDeploymentSpec extends WebDriverDeploymentSpec {

  // Use a target path in the build directory to avoid polluting source
  override val targetPath: Path = Paths.get("build/test-results/drivers/chromedriver")
  override val capabilities: Capabilities = {
    val options = new ChromeOptions()
    options.addArguments("--headless=new")
    options
  }

  override def driverName: String = "chromedriver"

  override def verifyDriverPath(path: Path): WebDriverBundle = {
    val service = new org.openqa.selenium.chrome.ChromeDriverService.Builder()
      .usingDriverExecutable(path.toFile)
      .usingAnyFreePort()
      .build()

    val browserBinary = WebDriverDeployment.resolvePaths(capabilities)._2.toString

    val options = new ChromeOptions().merge(capabilities)
    options.setBinary(browserBinary)
    options.addArguments("--no-sandbox")
    options.addArguments("--disable-dev-shm-usage")

    WebDriverBundle.Chrome(service, options)
  }
}
