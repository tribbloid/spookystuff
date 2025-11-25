package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.dsl.BinaryDeployment
import org.openqa.selenium.Capabilities
import org.openqa.selenium.chrome.ChromeDriverService
import org.openqa.selenium.firefox.GeckoDriverService
import org.openqa.selenium.remote.service.{DriverFinder, DriverService}

import java.nio.file.{Files, Path, StandardCopyOption}

object WebDriverDeployment {

  def fromRootDir(
      executableDir: String,
      option: Capabilities
  ): WebDriverDeployment = {

    val osName = Option(System.getProperty("os.name")).getOrElse("Unknown")
    val osBase =
      if (osName.startsWith("Windows")) "Windows"
      else if (osName.startsWith("Linux")) "Linux"
      else if (osName.startsWith("Mac")) "MacOS"
      else osName.replaceAll("\\s+", "")

    val archRaw = Option(System.getProperty("os.arch")).getOrElse("unknown")
    val archNorm = archRaw match {
      case "x86_64" | "amd64"  => "amd64"
      case "aarch64" | "arm64" => "arm64"
      case other               => other
    }

    val qualifier = s"${osBase}_${archNorm}"

    val executablePath = Path.of(
      executableDir,
      qualifier,
      option.getBrowserName
    )

    WebDriverDeployment(executablePath, option)
  }

  /**
    * Use Selenium Manager (via DriverFinder) to resolve both driver and browser paths for the given capabilities. This
    * will trigger downloads if needed but will not start a real browser session.
    */
  def resolvePaths(
      capabilities: Capabilities
  ): (Path, Path) = {
    val browserName = capabilities.getBrowserName.toLowerCase
    val service: DriverService = createService(browserName)

    try {
      val finder = new DriverFinder(service, capabilities)

      val driverPath = Path.of(finder.getDriverPath)

      val browserPathStr = Option(finder.getBrowserPath)
        .filter(_.nonEmpty)
        .getOrElse {
          throw new RuntimeException(
            s"Selenium Manager did not return a browser path for capabilities: $capabilities"
          )
        }
      val browserPath = Path.of(browserPathStr)

      val userHome = System.getProperty("user.home")
      val cacheRoot = Path.of(userHome, ".cache", "selenium").toAbsolutePath.normalize()
      val normalizedBrowserPath = browserPath.toAbsolutePath.normalize()

      if (!normalizedBrowserPath.startsWith(cacheRoot)) {
        throw new UnsupportedOperationException(
          s"Using system-installed browser is forbidden. " +
            s"Resolved browser path: '$normalizedBrowserPath'. Expected path under '$cacheRoot'. " +
            "Configure Selenium Manager to download and manage browsers."
        )
      }

      driverPath -> normalizedBrowserPath
    } catch {
      case e: UnsupportedOperationException =>
        throw e
      case e: Exception =>
        throw new RuntimeException(s"Failed to resolve driver/browser for $browserName", e)
    } finally {
      service.close() // service is only used for downloading
    }
  }

  private def createService(browserName: String): DriverService = {
    browserName.toLowerCase match {
      case "chrome" =>
        new ChromeDriverService.Builder().build()
      case "firefox" =>
        new GeckoDriverService.Builder().build()
      case other =>
        throw new UnsupportedOperationException(s"Browser $other not supported for auto-download")
    }
  }
}

case class WebDriverDeployment(
    targetPath: Path,
    capabilities: Capabilities // TODO: remove, should be a constructor of WebDriverService
) extends BinaryDeployment {
  // TODO: this can only distribute driver, not browser (which contains mutliple files)
  //  also: downloaded driver is only compatible with OS of the driver, if worker OS is different then it will be useless
  //  BinaryDepolyment logic will need major overhaul to facilitate them

  override protected def downloadWithoutVerify(): Unit = {
    val (driverPath, _) = WebDriverDeployment.resolvePaths(capabilities)

    // Copy to target
    // Ensure parent dir exists
    if (targetPath.getParent != null) {
      Files.createDirectories(targetPath.getParent)
    }

    Files.copy(driverPath, targetPath, StandardCopyOption.REPLACE_EXISTING)
  }
}
