package com.tribbloids.spookystuff.io

import java.nio.file._
import java.io.IOException
import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import scala.annotation.tailrec

/**
 * Cross-platform file handling utilities that automatically handle platform differences
 * between Windows, Unix/Linux, and macOS.
 */
object CrossPlatformFileUtils {

  /**
   * Platform detection
   */
  val isWindows: Boolean = System.getProperty("os.name").toLowerCase.startsWith("windows")
  val isMac: Boolean = System.getProperty("os.name").toLowerCase.startsWith("mac")
  val isUnix: Boolean = !isWindows && !isMac

  /**
   * Platform-specific path separator
   */
  def pathSeparator: String = FileSystems.getDefault.getSeparator

  /**
   * Platform-specific file separator (for paths, not classpath)
   */
  def fileSeparator: String = java.io.File.separator

  /**
   * Default timeout for file operations on different platforms
   */
  def defaultFileOperationTimeout: FiniteDuration = if (isWindows) 5.seconds else 2.seconds

  /**
   * Default retry count for file operations on Windows
   */
  def defaultRetryCount: Int = if (isWindows) 5 else 1

  /**
   * Default retry delay for file operations on Windows
   */
  def defaultRetryDelay: FiniteDuration = if (isWindows) 200.milliseconds else 0.milliseconds

  /**
   * Builds a cross-platform path from components
   */
  def buildPath(components: String*): Path = {
    val normalizedComponents = components.map(_.replace("/", pathSeparator).replace("\\", pathSeparator))
    Paths.get(normalizedComponents.head, normalizedComponents.tail: _*)
  }

  /**
   * Normalizes a path to use the correct platform separator
   */
  def normalizePath(path: String): String = {
    path.replace("/", pathSeparator).replace("\\", pathSeparator)
  }

  /**
   * Joins path components using the platform separator
   */
  def joinPath(components: String*): String = {
    components.map(normalizePath).mkString(pathSeparator)
  }

  /**
   * Splits a path using the platform separator
   */
  def splitPath(path: String): Array[String] = {
    normalizePath(path).split(Pattern.quote(pathSeparator))
  }

  /**
   * Creates a temporary file with cross-platform compatible settings
   */
  def createTempFile(
    prefix: String = "spooky",
    suffix: String = ".tmp",
    directory: Option[Path] = None
  ): Path = {
    createTempFileWithAttrs(prefix, suffix, directory)
  }

  private def createTempFileWithAttrs(
    prefix: String,
    suffix: String,
    directory: Option[Path]
  ): Path = {
    val dir = directory.getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))

    // On Windows, use shorter prefixes to avoid path length issues
    val safePrefix = if (isWindows && prefix.length > 3) prefix.take(3) else prefix

    Files.createTempFile(dir, safePrefix, suffix)
  }

  /**
   * Creates a temporary directory with cross-platform compatible settings
   */
  def createTempDirectory(
    prefix: String = "spooky",
    directory: Option[Path] = None
  ): Path = {
    createTempDirectoryWithAttrs(prefix, directory)
  }

  private def createTempDirectoryWithAttrs(
    prefix: String,
    directory: Option[Path]
  ): Path = {
    val dir = directory.getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))

    // On Windows, use shorter prefixes to avoid path length issues
    val safePrefix = if (isWindows && prefix.length > 3) prefix.take(3) else prefix

    Files.createTempDirectory(dir, safePrefix)
  }

  /**
   * Deletes a file or directory with retry logic for Windows
   */
  def deleteFileWithRetry(
    path: Path,
    timeout: FiniteDuration = defaultFileOperationTimeout,
    retryCount: Int = defaultRetryCount
  ): Try[Boolean] = {
    deleteWithRetry(path, timeout, retryCount, forceDelete = false)
  }

  /**
   * Force deletes a file or directory with retry logic for Windows
   */
  def forceDeleteFileWithRetry(
    path: Path,
    timeout: FiniteDuration = defaultFileOperationTimeout,
    retryCount: Int = defaultRetryCount
  ): Try[Boolean] = {
    deleteWithRetry(path, timeout, retryCount, forceDelete = true)
  }

  private def deleteWithRetry(
    path: Path,
    timeout: FiniteDuration,
    retryCount: Int,
    forceDelete: Boolean
  ): Try[Boolean] = {
    @tailrec
    def attemptDelete(attemptsLeft: Int, lastError: Option[Throwable] = None): Try[Boolean] = {
      val startTime = System.nanoTime()

      Try {
        if (!Files.exists(path)) {
          true // Already deleted
        } else if (Files.isDirectory(path) && forceDelete) {
          deleteDirectoryRecursively(path)
          true
        } else {
          Files.deleteIfExists(path)
        }
      } match {
        case success @ Success(true) => success
        case Success(false) => Success(true) // File didn't exist
        case failure @ Failure(ex) =>
          val elapsedTime = (System.nanoTime() - startTime).nanos

          if (attemptsLeft <= 1 || elapsedTime >= timeout) {
            failure
          } else {
            // On Windows, wait a bit before retrying
            Thread.sleep(defaultRetryDelay.toMillis)
            attemptDelete(attemptsLeft - 1, Some(ex))
          }
      }
    }

    attemptDelete(retryCount)
  }

  private def deleteDirectoryRecursively(directory: Path): Unit = {
    if (Files.exists(directory) && Files.isDirectory(directory)) {
      Files.walk(directory)
        .sorted( java.util.Comparator.reverseOrder[Path]() )
        .forEach { path =>
          try {
            Files.deleteIfExists(path)
          } catch {
            case _: IOException => // Ignore, continue with other files
          }
        }
    }
  }

  /**
   * Checks if a path exists with Windows-specific considerations
   */
  def pathExists(path: Path): Boolean = {
    try {
      Files.exists(path)
    } catch {
      case _: AccessDeniedException if isWindows => false
      case _: IOException => false
    }
  }

  /**
   * Checks if a path is readable with Windows-specific considerations
   */
  def isReadable(path: Path): Boolean = {
    try {
      Files.isReadable(path) && pathExists(path)
    } catch {
      case _: AccessDeniedException if isWindows => false
      case _: IOException => false
    }
  }

  /**
   * Checks if a path is writable with Windows-specific considerations
   */
  def isWritable(path: Path): Boolean = {
    try {
      Files.isWritable(path) && pathExists(path)
    } catch {
      case _: AccessDeniedException if isWindows => false
      case _: IOException => false
    }
  }

  /**
   * Gets file size with Windows-specific handling
   */
  def getFileSize(path: Path): Try[Long] = {
    Try {
      if (!Files.exists(path)) {
        throw new NoSuchFileException(s"File does not exist: $path")
      }
      if (Files.isDirectory(path)) {
        throw new IOException(s"Path is a directory, not a file: $path")
      }
      Files.size(path)
    }
  }

  /**
   * Copies a file with cross-platform error handling
   */
  def copyFileWithRetry(
    source: Path,
    target: Path,
    overwrite: Boolean = false,
    timeout: FiniteDuration = defaultFileOperationTimeout,
    retryCount: Int = defaultRetryCount
  ): Try[Path] = {
    @tailrec
    def attemptCopy(attemptsLeft: Int, lastError: Option[Throwable] = None): Try[Path] = {
      val startTime = System.nanoTime()

      Try {
        if (!Files.exists(source)) {
          throw new NoSuchFileException(s"Source file does not exist: $source")
        }

        if (Files.exists(target) && !overwrite) {
          throw new FileAlreadyExistsException(s"Target file already exists: $target")
        }

        // Create parent directories if they don't exist
        if (target.getParent != null) {
          Files.createDirectories(target.getParent)
        }

        Files.copy(source, target,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES
        )
      } match {
        case success @ Success(_) => success
        case failure @ Failure(ex) =>
          val elapsedTime = (System.nanoTime() - startTime).nanos

          if (attemptsLeft <= 1 || elapsedTime >= timeout) {
            failure
          } else {
            // On Windows, wait a bit before retrying
            Thread.sleep(defaultRetryDelay.toMillis)
            attemptCopy(attemptsLeft - 1, Some(ex))
          }
      }
    }

    attemptCopy(retryCount)
  }

  /**
   * Creates a Path from a URI string with Windows-specific handling
   */
  def pathFromUri(uriString: String): Path = {
    // Handle Windows file URIs that start with "file:/C:" instead of "file:///C:"
    val normalizedUri = if (isWindows && uriString.matches("^file:/[A-Za-z]:.*")) {
      uriString.replaceFirst("^file:/", "file:///")
    } else {
      uriString
    }

    Paths.get(URI.create(normalizedUri))
  }

  /**
   * Converts a Path to a URI string with Windows-specific handling
   */
  def pathToUri(path: Path): String = {
    val uri = path.toUri.toString

    // On Windows, normalize the URI format
    if (isWindows) {
      uri.replace("file:///", "file:/")
    } else {
      uri
    }
  }

  /**
   * Validates a path for platform-specific issues
   */
  def validatePath(path: Path): Try[Unit] = {
    Try {
      val pathString = path.toString

      // Check for Windows path length limitations
      if (isWindows && pathString.length > 260) {
        throw new IOException(s"Path exceeds Windows MAX_PATH limit of 260 characters: $pathString")
      }

      // Check for invalid characters
      val invalidChars = if (isWindows) {
        Set('<', '>', ':', '"', '|', '?', '*')
      } else {
        Set('\u0000')
      }

      if (pathString.exists(invalidChars.contains)) {
        throw new IOException(s"Path contains invalid characters: $pathString")
      }
    }
  }
}