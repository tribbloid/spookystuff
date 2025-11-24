package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import org.openqa.selenium.Capabilities

import java.net.URI
import java.nio.file.{Files, Path}

/**
  * ## Task implement WebDriverDeployment to enable downloading of web drivers automatically depending on the browser
  * type:
  *   - if `localSrc` is defined, try to copy it to `target` path using [[com.tribbloids.spookystuff.io.HDFSResolver]]
  *   - otherwise or if anything goes wrong, use Selenium to download the web driver automatically from the web into
  *     `target` path
  *   - afterwards, the driver should be usable as a Selenium WebDriver instance
  *   - test suite here should use Chrome as an example, both cases should be tested
  */

/**
  * ## Review 1:
  *   - all tests should use actual Chrome browser web driver instead of fake driver
  *   - all tests should verify that the driver is usable as a Selenium WebDriver instance
  */

/**
  * ## Review 2:
  *   - verifyDriverUsable should use the web driver to visit [[HTML_URL]] and make sure that web page rendered by the
  *     browser has a title
  *     - verifyDriverUsable should be in the companion object
  */

abstract class WebDriverDeploymentSpec extends BaseSpec with FileURIDocsFixture {

  def targetPath: Path
  def capabilities: Capabilities
  def verifyDriverUsable(path: Path): Unit
  def driverName: String

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
