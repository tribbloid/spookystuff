package com.tribbloids.spookystuff.web.agent

import ai.acyclic.prover.commons.util.Causes
import com.tribbloids.spookystuff.commons.TreeException
import com.tribbloids.spookystuff.dsl.BinaryDeployment
import org.openqa.selenium.Capabilities
import org.openqa.selenium.chrome.ChromeDriverService
import org.openqa.selenium.firefox.GeckoDriverService
import org.openqa.selenium.manager.SeleniumManager
import org.openqa.selenium.remote.service.{DriverFinder, DriverService}

import java.nio.file.{Files, Path, StandardCopyOption}
import java.util
import scala.util.Try

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
    * Use Selenium Manager to resolve both driver and browser paths for the given capabilities. This will trigger
    * downloads if needed but will not start a real browser session.
    */
  object resolvePaths {

    def apply(
        capabilities: Capabilities
    ): (Path, Path) = {
      val browserName = capabilities.getBrowserName.toLowerCase

      // TODO: unfortuantely Selenium have 2 competing APIs that are both unreliable, can it be simplified?
      val resultOpt = TreeException.GetFirstSuccess(
        attempts = Seq(
          () => usingSeleniumManager(browserName, capabilities),
          () => usingDriverFinder(browserName, capabilities)
        ),
        agg = { seq =>
          new RuntimeException(
            s"Failed to resolve driver/browser for $browserName",
            Causes.combine(seq)
          )
        }
      )

      resultOpt.get
    }

    private def usingSeleniumManager(
        browserName: String,
        capabilities: Capabilities
    ): (Path, Path) = {

      val args = new util.ArrayList[String]()
      args.add("--browser")
      args.add(browserName)

      Option(capabilities.getBrowserVersion)
        .filter(_.nonEmpty)
        .foreach { v =>
          args.add("--browser-version")
          args.add(v)
        }

      args.add("--skip-driver-in-path")

      val result = SeleniumManager.getInstance.getBinaryPaths(args)

      val driverPathStr = Option(result.getDriverPath)
        .filter(_.nonEmpty)
        .getOrElse {
          throw new RuntimeException(
            s"Selenium Manager did not return a driver path for capabilities: $capabilities"
          )
        }
      val browserPathStr = Option(result.getBrowserPath)
        .filter(_.nonEmpty)
        .getOrElse {
          throw new RuntimeException(
            s"Selenium Manager did not return a browser path for capabilities: $capabilities"
          )
        }

      val normalizedDriverPath = Path.of(driverPathStr).toAbsolutePath.normalize()
      val normalizedBrowserPath = Path.of(browserPathStr).toAbsolutePath.normalize()

      val resolvedDriverPath = Try(normalizedDriverPath.toRealPath()).getOrElse(normalizedDriverPath)
      if (isShellScript(resolvedDriverPath)) {
        throw new RuntimeException(
          s"Selenium Manager resolved driver to a shell script (likely a snap shim): $resolvedDriverPath"
        )
      }

      resolvedDriverPath -> normalizedBrowserPath
    }

    private def usingDriverFinder(
        browserName: String,
        capabilities: Capabilities
    ): (Path, Path) = {

      val service: DriverService = createService(browserName)
      try {
        val finder = new DriverFinder(service, capabilities)

        val driverPathStr = Option(finder.getDriverPath)
          .filter(_.nonEmpty)
          .getOrElse {
            throw new RuntimeException(
              s"DriverFinder did not return a driver path for capabilities: $capabilities"
            )
          }

        val browserPathStr = Option(finder.getBrowserPath)
          .filter(_.nonEmpty)
          .getOrElse {
            throw new RuntimeException(
              s"DriverFinder did not return a browser path for capabilities: $capabilities"
            )
          }

        val normalizedDriverPath = Path.of(driverPathStr).toAbsolutePath.normalize()
        val normalizedBrowserPath = Path.of(browserPathStr).toAbsolutePath.normalize()

        val resolvedDriverPath = Try(normalizedDriverPath.toRealPath()).getOrElse(normalizedDriverPath)
        if (isShellScript(resolvedDriverPath)) {
          throw new RuntimeException(
            s"DriverFinder resolved driver to a shell script: $resolvedDriverPath"
          )
        }

        resolvedDriverPath -> normalizedBrowserPath
      } finally {
        service.close()
      }
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

  private def isShellScript(path: Path): Boolean = {
    val fileName = Option(path.getFileName).map(_.toString.toLowerCase).getOrElse("")
    if (
      fileName.endsWith(".sh") || fileName.endsWith(".bash") || fileName.endsWith(".cmd") || fileName.endsWith(".bat")
    ) true
    else if (!Files.isRegularFile(path)) false
    else {
      val in = Files.newInputStream(path)
      try {
        val bytes = in.readNBytes(2)
        bytes.length == 2 && bytes(0) == '#'.toByte && bytes(1) == '!'.toByte
      } finally {
        in.close()
      }
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
