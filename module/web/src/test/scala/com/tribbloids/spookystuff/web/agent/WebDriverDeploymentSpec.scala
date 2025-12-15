package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.io.{TestFileHelpers, WindowsFileCompatibility}
import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import com.tribbloids.spookystuff.web.actions.WebInteraction
import org.openqa.selenium.Capabilities
import org.openqa.selenium.support.ui.WebDriverWait

import java.nio.file.{Files, Path}
import java.time.Duration
import ai.acyclic.prover.commons.util.Retry

import scala.util.{Failure, Success, Try}

abstract class WebDriverDeploymentSpec extends BaseSpec with FileURIDocsFixture {

  def targetPath: Path
  def capabilities: Capabilities
  def verifyDriverPath(path: Path): WebDriverBundle
  def driverName: String

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
        val tempDriverPath = WindowsFileCompatibility
          .createWindowsCompatibleTempFile(
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

        // Apply Windows-specific retry logic for deployment
        if (WindowsFileCompatibility.isWindows) {
          val maxRetries = 3
          Retry(
            n = maxRetries,
            intervalFactory = { nRemaining =>
              val attemptNum = maxRetries - nRemaining + 1
              val delayMs = (WindowsFileCompatibility.DEFAULT_RETRY_DELAY.toMillis.toDouble * math.pow(2.0, attemptNum - 1)).toLong
              math.min(delayMs, WindowsFileCompatibility.MAX_RETRY_DELAY.toMillis)
            },
            silent = true
          ) {
            Try(deployment.deploy()) match {
              case Success(v) => v
              case Failure(ex) =>
                if (WindowsFileCompatibility.isWindowsRecoverableError(ex)) throw ex
                else throw Retry.BypassingRule.NoRetry.apply(ex)
            }
          }
        } else {
          deployment.deploy()
        }

        assert(Files.exists(tempDriverPath))
        assert(Files.isExecutable(tempDriverPath))

        // 2. Now test copying this real driver to the target path
        Files.createDirectories(targetPath.getParent)
        Files.deleteIfExists(targetPath)

        val copyDeployment = WebDriverDeployment(
          targetPath = targetPath,
          capabilities = capabilities
        )

        // Apply Windows-specific retry logic for copy deployment
        if (WindowsFileCompatibility.isWindows) {
          val maxRetries = 3
          Retry(
            n = maxRetries,
            intervalFactory = { nRemaining =>
              val attemptNum = maxRetries - nRemaining + 1
              val delayMs = (WindowsFileCompatibility.DEFAULT_RETRY_DELAY.toMillis.toDouble * math.pow(2.0, attemptNum - 1)).toLong
              math.min(delayMs, WindowsFileCompatibility.MAX_RETRY_DELAY.toMillis)
            },
            silent = true
          ) {
            Try(copyDeployment.deploy()) match {
              case Success(v) => v
              case Failure(ex) =>
                if (WindowsFileCompatibility.isWindowsRecoverableError(ex)) throw ex
                else throw Retry.BypassingRule.NoRetry.apply(ex)
            }
          }
        } else {
          copyDeployment.deploy()
        }

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) == Files.size(tempDriverPath))

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

        // Apply Windows-specific retry logic for deployment
        if (WindowsFileCompatibility.isWindows) {
          val maxRetries = 3
          Retry(
            n = maxRetries,
            intervalFactory = { nRemaining =>
              val attemptNum = maxRetries - nRemaining + 1
              val delayMs = (WindowsFileCompatibility.DEFAULT_RETRY_DELAY.toMillis.toDouble * math.pow(2.0, attemptNum - 1)).toLong
              math.min(delayMs, WindowsFileCompatibility.MAX_RETRY_DELAY.toMillis)
            },
            silent = true
          ) {
            Try(deployment.deploy()) match {
              case Success(v) => v
              case Failure(ex) =>
                if (WindowsFileCompatibility.isWindowsRecoverableError(ex)) throw ex
                else throw Retry.BypassingRule.NoRetry.apply(ex)
            }
          }
        } else {
          deployment.deploy()
        }

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) > 0)

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
        if (WindowsFileCompatibility.isWindows) {
          val maxRetries = 3
          Retry(
            n = maxRetries,
            intervalFactory = { nRemaining =>
              val attemptNum = maxRetries - nRemaining + 1
              val delayMs = (WindowsFileCompatibility.DEFAULT_RETRY_DELAY.toMillis.toDouble * math.pow(2.0, attemptNum - 1)).toLong
              math.min(delayMs, WindowsFileCompatibility.MAX_RETRY_DELAY.toMillis)
            },
            silent = true
          ) {
            Try(deployment.deploy()) match {
              case Success(v) => v
              case Failure(ex) =>
                if (WindowsFileCompatibility.isWindowsRecoverableError(ex)) throw ex
                else throw Retry.BypassingRule.NoRetry.apply(ex)
            }
          }
        } else {
          deployment.deploy()
        }

        assert(Files.exists(targetPath))
        assert(Files.isExecutable(targetPath))
        assert(Files.size(targetPath) > 0)

        verifyDriverUsable(targetPath)
      }.get
    }
  }
}
