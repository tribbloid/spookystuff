package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxOptions, GeckoDriverService}
import java.nio.file.{Files, Path, Paths}
import java.net.URI

object FirefoxWebDriverDeploymentSpec extends FileURIDocsFixture {

  def verifyDriverUsable(path: Path): Unit = {
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

class FirefoxWebDriverDeploymentSpec extends BaseSpec with FileURIDocsFixture {

  import FirefoxWebDriverDeploymentSpec.*

  // Use a target path in the build directory to avoid polluting source
  val targetPath = Paths.get("build/test-results/drivers/geckodriver")
  val options = new FirefoxOptions()
  options.addArguments("-headless")

  describe("WebDriverDeployment (Firefox)") {

    it("should copy driver from localSrc if defined") {
      // 1. Acquire a real driver first to use as localSrc
      val tempDriverPath = Files.createTempFile("real_geckodriver_src", "")
      Files.delete(tempDriverPath) // start fresh

      // Use WebDriverDeployment to download it to temp location
      WebDriverDeployment(
        localSrc = None,
        target = tempDriverPath,
        capabilities = options
      ).deploy()

      assert(Files.exists(tempDriverPath))
      assert(Files.isExecutable(tempDriverPath))

      // 2. Now test copying this real driver to the target path
      Files.createDirectories(targetPath.getParent)
      Files.deleteIfExists(targetPath)

      val deployment = WebDriverDeployment(
        localSrc = Some(tempDriverPath.toUri),
        target = targetPath,
        capabilities = options
      )

      deployment.deploy()

      assert(Files.exists(targetPath))
      assert(Files.isExecutable(targetPath))
      assert(Files.size(targetPath) == Files.size(tempDriverPath))

      verifyDriverUsable(targetPath)
    }

    it("should download driver if localSrc is not defined") {
      Files.createDirectories(targetPath.getParent)
      Files.deleteIfExists(targetPath)

      val deployment = WebDriverDeployment(
        localSrc = None,
        target = targetPath,
        capabilities = options
      )

      deployment.deploy()

      assert(Files.exists(targetPath))
      assert(Files.isExecutable(targetPath))
      assert(Files.size(targetPath) > 0)

      verifyDriverUsable(targetPath)
    }

    it("should download driver if localSrc is defined but invalid (fallback)") {
      val invalidSrc = new URI("file:///non/existent/path/driver")

      Files.createDirectories(targetPath.getParent)
      Files.deleteIfExists(targetPath)

      val deployment = WebDriverDeployment(
        localSrc = Some(invalidSrc),
        target = targetPath,
        capabilities = options
      )

      deployment.deploy()

      assert(Files.exists(targetPath))
      assert(Files.isExecutable(targetPath))
      assert(Files.size(targetPath) > 0)

      verifyDriverUsable(targetPath)
    }
  }

}
