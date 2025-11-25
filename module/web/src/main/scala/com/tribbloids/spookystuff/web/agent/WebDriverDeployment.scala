package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.io.{HDFSResolver, WriteMode}
import org.apache.hadoop.conf.Configuration
import org.openqa.selenium.Capabilities
import org.openqa.selenium.chrome.{ChromeDriverService, ChromeOptions}
import org.openqa.selenium.firefox.{FirefoxOptions, GeckoDriverService}
import org.openqa.selenium.remote.service.{DriverFinder, DriverService}

import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}
import scala.util.{Failure, Success, Try}

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
      case "x86_64" | "amd64" => "amd64"
      case "aarch64" | "arm64" => "arm64"
      case other => other
    }

    val qualifier = s"${osBase}_${archNorm}"

    val executablePath = Path.of(
      executableDir,
      qualifier,
      option.getBrowserName
    )

    WebDriverDeployment(None, executablePath, option)
  }
}

case class WebDriverDeployment(
    localSrc: Option[URI],
    target: Path,
    capabilities: Capabilities
) {

  def deploy(): Unit = {
    val tryLocal = Try {
      localSrc match {
        case Some(src) =>
          val resolver = HDFSResolver(() => new Configuration())
          val srcExe = resolver.on(src.toString)
          // HDFSResolver requires target to be a URI string
          val targetUri = target.toAbsolutePath.toUri.toString
          srcExe.copyTo(targetUri, WriteMode.Overwrite)
        case None =>
          throw new RuntimeException("No local source defined")
      }
    }

    tryLocal match {
      case Success(_) => // Done
      case Failure(_) =>
        // Fallback to Selenium download
        downloadDriver()
    }

    // Ensure executable
    val targetFile = target.toFile
    if (targetFile.exists()) {
      targetFile.setExecutable(true)
    }
  }

  private def downloadDriver(): Unit = {
    val browserName = capabilities.getBrowserName.toLowerCase

    val service: DriverService = browserName match {
      case "chrome" =>
        new ChromeDriverService.Builder().build()
      case "firefox" =>
        new GeckoDriverService.Builder().build()
      case _ =>
        throw new UnsupportedOperationException(s"Browser $browserName not supported for auto-download")
    }

    try {
      // Use DriverFinder to locate/download the driver without starting the service
      val finder = new DriverFinder(service, capabilities)
      val exePath = Path.of(finder.getDriverPath)

      if (finder.hasBrowserPath) {
        val browserPath = finder.getBrowserPath
        capabilities match {
          case v: ChromeOptions =>
            v.setBinary(browserPath)
          case v: FirefoxOptions =>
            v.setBinary(browserPath)
          case _ =>
        }
      }

      // Copy to target
      // Ensure parent dir exists
      if (target.getParent != null) {
        Files.createDirectories(target.getParent)
      }

      Files.copy(exePath, target, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to download driver for $browserName", e)
    }
  }
}
