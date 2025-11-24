package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import com.tribbloids.spookystuff.web.actions.WebInteraction
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.support.ui.WebDriverWait
import org.openqa.selenium.{Capabilities, WebDriver}

import java.net.URI
import java.nio.file.{Files, Path}
import java.time.Duration

abstract class WebDriverDeploymentSpec extends BaseSpec with FileURIDocsFixture {

  def targetPath: Path
  def capabilities: Capabilities
  def newDriver(path: Path): (WebDriver, DriverService)
  def driverName: String

  def verifyDriverUsable(path: Path): Unit = {
    val (driver, service) = newDriver(path)

    try {
      driver.get(HTML_URL)
      assert(driver.getTitle != null)
      assert(driver.getTitle.nonEmpty)

      new WebDriverWait(driver, Duration.ofSeconds(5)).until(WebInteraction.DocumentReadyCondition)
    } finally {
      driver.quit()
      service.stop()
    }
  }

  describe(s"WebDriverDeployment ($driverName)") {

    it("should copy driver from localSrc if defined") {
      // 1. Acquire a real driver first to use as localSrc
      val tempDriverPath = Files.createTempFile(s"real_${driverName}_src", "")
      Files.delete(tempDriverPath) // start fresh

      // Use WebDriverDeployment to download it to temp location
      WebDriverDeployment(
        localSrc = None,
        target = tempDriverPath,
        capabilities = capabilities
      ).deploy()

      assert(Files.exists(tempDriverPath))
      assert(Files.isExecutable(tempDriverPath))

      // 2. Now test copying this real driver to the target path
      Files.createDirectories(targetPath.getParent)
      Files.deleteIfExists(targetPath)

      val deployment = WebDriverDeployment(
        localSrc = Some(tempDriverPath.toUri),
        target = targetPath,
        capabilities = capabilities
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
        capabilities = capabilities
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
        capabilities = capabilities
      )

      deployment.deploy()

      assert(Files.exists(targetPath))
      assert(Files.isExecutable(targetPath))
      assert(Files.size(targetPath) > 0)

      verifyDriverUsable(targetPath)
    }
  }

}
