package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.io.{HDFSResolver, WriteMode}
import org.apache.hadoop.conf.Configuration
import org.openqa.selenium.Capabilities
import org.openqa.selenium.chrome.ChromeDriverService
import org.openqa.selenium.firefox.GeckoDriverService
import org.openqa.selenium.remote.service.DriverService

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
        ChromeDriverService.createDefaultService()
      case "firefox" =>
        new GeckoDriverService.Builder().build()
      case _ =>
        throw new UnsupportedOperationException(s"Browser $browserName not supported for auto-download")
    }

    try {
      // Starting the service triggers the driver discovery/download in Selenium 4.6+
      service.start()
      val exe = service.getExecutable

      // Copy to target
      // Ensure parent dir exists
      if (target.getParent != null) {
        Files.createDirectories(target.getParent)
      }

      // service.getExecutable might return File or String depending on Selenium version/bindings
      // The compiler said it is a String.
      val exePath = new java.io.File(exe.toString).toPath

      Files.copy(exePath, target, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      if (service.isRunning) {
        service.stop()
      }
    }
  }
}
