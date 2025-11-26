package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import com.tribbloids.spookystuff.web.actions.WebInteraction
import com.tribbloids.spookystuff.io.{WindowsFileCompatibility, TestFileHelpers}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.support.ui.WebDriverWait
import org.openqa.selenium.{Capabilities, WebDriver}

import java.net.URI
import java.nio.file.{Files, Path}
import java.time.Duration
import scala.util.{Try, Success, Failure}

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
      TestFileHelpers.withTestResources {
        // 1. Acquire a real driver first to use as localSrc using cross-platform temp file creation
        val tempDriverPath = WindowsFileCompatibility.createWindowsCompatibleTempFile(
          prefix = s"real_${driverName}_src",
          suffix = "",
          registerForCleanup = true
        ).get

        // Use WebDriverDeployment to download it to temp location
        val deployment = WebDriverDeployment(
          localSrc = None,
          target = tempDriverPath,
          capabilities = capabilities
        )

        // Apply Windows-specific retry logic for deployment
        if (WindowsFileCompatibility.isWindows) {
          WindowsFileCompatibility.retryWithBackoff(() => Try(deployment.deploy()), maxRetries = 3).get
        } else {
          deployment.deploy()
        }

        assert(Files.exists(tempDriverPath))
        assert(Files.isExecutable(tempDriverPath))

      // 2. Now test copying this real driver to the target path
        Files.createDirectories(targetPath.getParent)
        Files.deleteIfExists(targetPath)

        val copyDeployment = WebDriverDeployment(
          localSrc = Some(tempDriverPath.toUri),
          target = targetPath,
          capabilities = capabilities
        )

        // Apply Windows-specific retry logic for copy deployment
        if (WindowsFileCompatibility.isWindows) {
          WindowsFileCompatibility.retryWithBackoff(() => Try(copyDeployment.deploy()), maxRetries = 3).get
        } else {
          copyDeployment.deploy()
        }

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) == Files.size(tempDriverPath))

        verifyDriverUsable(targetPath)
      }
    }

    it("should download driver if localSrc is not defined") {
      TestFileHelpers.withTestResources {
        Files.createDirectories(targetPath.getParent)
        Files.deleteIfExists(targetPath)

        val deployment = WebDriverDeployment(
          localSrc = None,
          target = targetPath,
          capabilities = capabilities
        )

        // Apply Windows-specific retry logic for deployment
        if (WindowsFileCompatibility.isWindows) {
          WindowsFileCompatibility.retryWithBackoff(() => Try(deployment.deploy()), maxRetries = 3).get
        } else {
          deployment.deploy()
        }

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) > 0)

        verifyDriverUsable(targetPath)
      }
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

      // Apply Windows-specific retry logic for deployment
      if (WindowsFileCompatibility.isWindows) {
        WindowsFileCompatibility.retryWithBackoff(() => Try(deployment.deploy()), maxRetries = 3).get
      } else {
        deployment.deploy()
      }

      assert(Files.exists(targetPath))
      assert(Files.isExecutable(targetPath))
      assert(Files.size(targetPath) > 0)

      verifyDriverUsable(targetPath)
    }
  }
}
