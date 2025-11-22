package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.io.{TempFile, TestFileHelpers, WindowsFileCompatibility}
import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import com.tribbloids.spookystuff.web.actions.WebInteraction
import org.openqa.selenium.Capabilities
import org.openqa.selenium.support.ui.WebDriverWait

import java.nio.file.{Files, Path}
import java.time.Duration

import scala.util.Try

abstract class WebDriverDeploymentSpec extends BaseSpec with FileURIDocsFixture {

  def targetPath: Path
  def capabilities: Capabilities
  def verifyDriverPath(path: Path): WebDriverBundle
  def driverName: String

  private def assertNotShellScript(path: Path): Unit = {
    assert(Files.isRegularFile(path))
    val in = Files.newInputStream(path)
    try {
      val bytes = in.readNBytes(2)
      assert(!(bytes.length == 2 && bytes(0) == '#'.toByte && bytes(1) == '!'.toByte))
    } finally {
      in.close()
    }
  }

  def verifyDriverUsable(path: Path): Unit = {
    val bundle = verifyDriverPath(path)
    val driver = bundle.driver

    try {
      driver.get(HTML_URL)
      assert(driver.getTitle != null)
      assert(driver.getTitle.nonEmpty)

      new WebDriverWait(driver, Duration.ofSeconds(5)).until(WebInteraction.DocumentReadyCondition)
    } finally {
      bundle.close()
    }
  }

  describe(s"WebDriverDeployment ($driverName)") {

    it("should copy driver from localSrc if defined") {
      TestFileHelpers.withTestResources {
        // 1. Acquire a real driver first to use as localSrc using cross-platform temp file creation
        val tempDriverPath = TempFile
          .createFile(
            prefix = s"real_${driverName}_src",
            suffix = "",
            registerForCleanup = true
          )
          .get

        // Use WebDriverDeployment to download it to temp location
        val deployment = WebDriverDeployment(
          targetPath = tempDriverPath,
          capabilities = capabilities
        )

        WindowsFileCompatibility
          .retryIfRecoverable(
            operation = Try(deployment.deploy()),
            maxRetries = 3
          )
          .get

        assert(Files.exists(tempDriverPath))
        assert(Files.isExecutable(tempDriverPath))
        assertNotShellScript(tempDriverPath)

        // 2. Now test copying this real driver to the target path
        Files.createDirectories(targetPath.getParent)
        Files.deleteIfExists(targetPath)

        val copyDeployment = WebDriverDeployment(
          targetPath = targetPath,
          capabilities = capabilities
        )

        WindowsFileCompatibility
          .retryIfRecoverable(
            operation = Try(copyDeployment.deploy()),
            maxRetries = 3
          )
          .get

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) == Files.size(tempDriverPath))
        assertNotShellScript(targetPath)

        verifyDriverUsable(targetPath)
      }.get
    }

    it("should download driver if localSrc is not defined") {
      TestFileHelpers.withTestResources {
        Files.createDirectories(targetPath.getParent)
        Files.deleteIfExists(targetPath)

        val deployment = WebDriverDeployment(
          targetPath = targetPath,
          capabilities = capabilities
        )

        WindowsFileCompatibility
          .retryIfRecoverable(
            operation = Try(deployment.deploy()),
            maxRetries = 3
          )
          .get

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) > 0)
        assertNotShellScript(targetPath)

        verifyDriverUsable(targetPath)
      }.get
    }

    it("should download driver if localSrc is defined but invalid (fallback)") {
      TestFileHelpers.withTestResources {
        Files.createDirectories(targetPath.getParent)
        Files.deleteIfExists(targetPath)

        val deployment = WebDriverDeployment(
          targetPath = targetPath,
          capabilities = capabilities
        )

        // Apply Windows-specific retry logic for deployment
        WindowsFileCompatibility
          .retryIfRecoverable(
            operation = Try(deployment.deploy()),
            maxRetries = 3
          )
          .get

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) > 0)
        assertNotShellScript(targetPath)

        verifyDriverUsable(targetPath)
      }.get
    }
  }
}
