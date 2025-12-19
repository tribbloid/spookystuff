package com.tribbloids.spookystuff.io

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Try

object TempFile {

  private val cleanupRegistry = mutable.Set[Path]()
  private val cleanupRegistered = new AtomicBoolean(false)

  private def ensureCleanupHookRegistered(): Unit = {
    if (!cleanupRegistered.get()) {
      synchronized {
        if (!cleanupRegistered.getAndSet(true)) {
          sys.addShutdownHook {
            performCleanup()
          }
        }
      }
    }
  }

  private def performCleanup(): Unit = {
    val paths = cleanupRegistry.toSet
    cleanupRegistry.clear()

    paths.foreach { path =>
      try {
        CrossPlatformFileUtils.forceDeleteFileWithRetry(path, timeout = 3.seconds).get
      } catch {
        case _: Exception =>
      }
    }
  }

  def registerForCleanup(path: Path): Unit = {
    cleanupRegistry += path
    ensureCleanupHookRegistered()
  }

  def unregisterFromCleanup(path: Path): Unit = {
    cleanupRegistry -= path
  }

  def createWindowsCompatibleTempFile(
      prefix: String = "spooky",
      suffix: String = ".tmp",
      directory: Option[Path] = None,
      registerForCleanup: Boolean = true
  ): Try[Path] = {
    WindowsFileCompatibility.retryWindowsRecoverable {
      Try {
        val tempDir = directory.getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))
        Files.createDirectories(tempDir)

        val safePrefix = WindowsFileCompatibility.sanitizeFileName(prefix).take(8)
        val safeSuffix = WindowsFileCompatibility.sanitizeFileName(suffix).take(5)

        val tempFile = Files.createTempFile(tempDir, safePrefix, safeSuffix)

        if (registerForCleanup) {
          this.registerForCleanup(tempFile)
        }

        tempFile
      }
    }
  }

  def createWindowsCompatibleTempDirectory(
      prefix: String = "spooky",
      directory: Option[Path] = None,
      registerForCleanup: Boolean = true
  ): Try[Path] = {
    WindowsFileCompatibility.retryWindowsRecoverable {
      Try {
        val tempDir = directory.getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))
        Files.createDirectories(tempDir)

        val safePrefix = WindowsFileCompatibility.sanitizeFileName(prefix).take(8)

        val tempDirectory = Files.createTempDirectory(tempDir, safePrefix)

        if (registerForCleanup) {
          this.registerForCleanup(tempDirectory)
        }

        tempDirectory
      }
    }
  }
}
