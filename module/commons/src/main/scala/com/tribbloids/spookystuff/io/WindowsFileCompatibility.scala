package com.tribbloids.spookystuff.io

import java.nio.file._
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Windows-specific file compatibility utilities that handle the unique challenges of file operations on Windows
  * operating systems.
  */
object WindowsFileCompatibility {

  /**
    * Check if running on Windows
    */
  def isWindows: Boolean = CrossPlatformFileUtils.isWindows

  // Windows-specific constants
  val MAX_PATH: Int = 260
  val EXTENDED_MAX_PATH: Int = 32767
  val DEFAULT_RETRY_COUNT: Int = 10
  val DEFAULT_RETRY_DELAY: FiniteDuration = 100.milliseconds
  val MAX_RETRY_DELAY: FiniteDuration = 2.seconds
  val FILE_LOCK_TIMEOUT: FiniteDuration = 10.seconds

  /**
    * Windows-specific invalid filename characters
    */
  val invalidFileNameChars: Set[Char] = Set('<', '>', ':', '"', '|', '?', '*')

  /**
    * Windows reserved filenames
    */
  val reservedFileNames: Set[String] = Set(
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9"
  )

  /**
    * Registry for tracking files that need cleanup on JVM exit
    */
  private val cleanupRegistry = mutable.Set[Path]()
  private val cleanupRegistered = new AtomicBoolean(false)

  // Register shutdown hook if needed
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
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  /**
    * Registers a path for cleanup on JVM exit
    */
  def registerForCleanup(path: Path): Unit = {
    if (isWindows) {
      cleanupRegistry += path
      ensureCleanupHookRegistered()
    }
  }

  /**
    * Unregisters a path from cleanup on JVM exit
    */
  def unregisterFromCleanup(path: Path): Unit = {
    if (isWindows) {
      cleanupRegistry -= path
    }
  }

  /**
    * Validates a filename for Windows compatibility
    */
  def validateFileName(fileName: String): Try[Unit] = {
    if (!isWindows) {
      Success(())
    } else {
      Try {
        // Check for invalid characters
        if (fileName.exists(invalidFileNameChars.contains)) {
          throw new IOException(s"Filename contains invalid characters: $fileName")
        }

        // Check for reserved names (case-insensitive)
        val baseName = fileName.split("\\.").head.toUpperCase
        if (reservedFileNames.contains(baseName)) {
          throw new IOException(s"Filename is reserved on Windows: $fileName")
        }

        // Check length
        if (fileName.length > 255) {
          throw new IOException(s"Filename exceeds Windows limit of 255 characters: $fileName")
        }
      }
    }
  }

  /**
    * Sanitizes a filename for Windows compatibility
    */
  def sanitizeFileName(fileName: String): String = {
    if (!isWindows) {
      fileName
    } else {
      // Replace invalid characters with underscores
      var sanitized = fileName.map { char =>
        if (invalidFileNameChars.contains(char)) '_' else char
      }

      // Handle reserved names by prefixing with underscore
      val baseName = sanitized.split("\\.").head
      if (reservedFileNames.contains(baseName.toUpperCase)) {
        sanitized = "_" + sanitized
      }

      // Truncate if too long
      if (sanitized.length > 255) {
        val dotIndex = sanitized.lastIndexOf('.')
        if (dotIndex > 0 && sanitized.length - dotIndex <= 5) {
          // Keep extension, truncate base name
          val ext = sanitized.substring(dotIndex)
          sanitized = sanitized.substring(0, Math.min(255 - ext.length, dotIndex)) + ext
        } else {
          sanitized = sanitized.take(255)
        }
      }

      sanitized
    }
  }

  /**
    * Checks if a path exceeds Windows path length limitations
    */
  def checkPathLength(path: Path): Try[Unit] = {
    if (!isWindows) {
      Success(())
    } else {
      Try {
        val pathString = path.toString
        if (pathString.length > MAX_PATH && !pathString.startsWith("\\\\?\\")) {
          // Suggest using extended-length path syntax
          throw new IOException(
            s"Path exceeds Windows MAX_PATH limit of $MAX_PATH characters. " +
              s"Consider using extended-length path syntax (\\\\?\\ prefix) or shorten the path: $pathString"
          )
        }
      }
    }
  }

  /**
    * Converts a regular path to extended-length path format for Windows
    */
  def toExtendedPath(path: Path): Path = {
    if (!isWindows) {
      path
    } else {
      val pathString = path.toString
      if (pathString.length <= MAX_PATH) {
        path
      } else if (pathString.startsWith("\\\\?\\")) {
        path
      } else if (path.isAbsolute) {
        Paths.get("\\\\?\\" + pathString)
      } else {
        // For relative paths, convert to absolute first
        path.toAbsolutePath
      }
    }
  }

  /**
    * Windows-specific retry logic with exponential backoff
    */
  def retryWithBackoff[T](
      operation: () => Try[T],
      maxRetries: Int = DEFAULT_RETRY_COUNT,
      initialDelay: FiniteDuration = DEFAULT_RETRY_DELAY,
      maxDelay: FiniteDuration = MAX_RETRY_DELAY,
      isRecoverable: Throwable => Boolean = isWindowsRecoverableError
  ): Try[T] = {

    @tailrec
    def attempt(attemptNum: Int, currentDelay: FiniteDuration): Try[T] = {
      operation() match {
        case success @ Success(_)                             => success
        case failure @ Failure(_) if attemptNum >= maxRetries => failure
        case failure @ Failure(ex)                            =>
          if (isRecoverable(ex)) {
            Thread.sleep(currentDelay.toMillis)
            val nextDelay = (currentDelay * 2).min(maxDelay)
            attempt(attemptNum + 1, nextDelay)
          } else {
            failure
          }
      }
    }

    attempt(1, initialDelay)
  }

  /**
    * Determines if an exception is recoverable on Windows
    */
  def isWindowsRecoverableError(ex: Throwable): Boolean = {
    if (!isWindows) {
      false
    } else {
      ex match {
        case _: java.nio.file.AccessDeniedException                         => true
        case _: java.nio.file.FileSystemException                           => true
        case _: java.io.IOException if ex.getMessage.contains("being used") => true
        case _: java.io.IOException if ex.getMessage.contains("locked")     => true
        case _: java.io.IOException if ex.getMessage.contains("process")    => true
        case _: java.nio.file.NoSuchFileException                           => false // Not recoverable
        case _: SecurityException                                           => false // Not recoverable
        case _                                                              => false // Default to not recoverable
      }
    }
  }

  /**
    * Creates a temporary file with Windows-specific considerations
    */
  def createWindowsCompatibleTempFile(
      prefix: String = "spooky",
      suffix: String = ".tmp",
      directory: Option[Path] = None,
      registerForCleanup: Boolean = true
  ): Try[Path] = {
    if (!isWindows) {
      Success(CrossPlatformFileUtils.createTempFile(prefix, suffix, directory))
    } else {
      retryWithBackoff { () =>
        Try {
          // Ensure directory exists
          val tempDir = directory.getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))
          Files.createDirectories(tempDir)

          // Sanitize prefix for Windows
          val safePrefix = sanitizeFileName(prefix).take(8) // Short prefix for safety
          val safeSuffix = sanitizeFileName(suffix).take(5) // Short suffix for safety

          val tempFile = Files.createTempFile(tempDir, safePrefix, safeSuffix)

          if (registerForCleanup) {
            this.registerForCleanup(tempFile)
          }

          tempFile
        }
      }
    }
  }

  /**
    * Creates a temporary directory with Windows-specific considerations
    */
  def createWindowsCompatibleTempDirectory(
      prefix: String = "spooky",
      directory: Option[Path] = None,
      registerForCleanup: Boolean = true
  ): Try[Path] = {
    if (!isWindows) {
      Success(CrossPlatformFileUtils.createTempDirectory(prefix, directory))
    } else {
      retryWithBackoff { () =>
        Try {
          // Ensure directory exists
          val tempDir = directory.getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))
          Files.createDirectories(tempDir)

          // Sanitize prefix for Windows
          val safePrefix = sanitizeFileName(prefix).take(8)

          val tempDirectory = Files.createTempDirectory(tempDir, safePrefix)

          if (registerForCleanup) {
            this.registerForCleanup(tempDirectory)
          }

          tempDirectory
        }
      }
    }
  }

  /**
    * Waits for a file to become available for operation on Windows
    */
  def waitForFileAvailability(
      path: Path,
      timeout: FiniteDuration = FILE_LOCK_TIMEOUT,
      checkInterval: FiniteDuration = 100.milliseconds
  ): Try[Boolean] = {
    if (!isWindows) {
      Success(Files.exists(path))
    } else {
      val startTime = System.nanoTime()
      val timeoutNanos = timeout.toNanos

      @tailrec
      def checkAvailability(): Try[Boolean] = {
        val elapsed = System.nanoTime() - startTime

        if (elapsed >= timeoutNanos) {
          Success(Files.exists(path)) // Return current state
        } else {
          Try {
            // Try to open the file exclusively to check if it's locked
            val channel = Files.newByteChannel(path, StandardOpenOption.READ)
            channel.close()
            true // File is available
          } match {
            case Success(true)                     => Success(true)
            case Failure(_: AccessDeniedException) => // File is locked, wait
              Thread.sleep(checkInterval.toMillis)
              checkAvailability()
            case Failure(_: NoSuchFileException) => // File doesn't exist
              Success(false)
            case failure @ Failure(_) => failure
          }
        }
      }

      checkAvailability()
    }
  }

  /**
    * Safely closes all resources associated with a file before deletion
    */
  def safeFileDelete(path: Path, timeout: FiniteDuration = FILE_LOCK_TIMEOUT): Try[Boolean] = {
    if (!isWindows) {
      CrossPlatformFileUtils.deleteFileWithRetry(path)
    } else {
      // First, wait for file to become available
      waitForFileAvailability(path, timeout)
      // Then attempt deletion with Windows-specific retry logic
      CrossPlatformFileUtils.forceDeleteFileWithRetry(path)
    }
  }

  /**
    * Gets information about Windows file locking
    */
  def getWindowsFileLockInfo(path: Path): Try[String] = {
    if (!isWindows) {
      Success("Not running on Windows")
    } else {
      Try {
        if (!Files.exists(path)) {
          "File does not exist"
        } else {
          try {
            // Try to open file exclusively
            val channel = Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
            channel.close()
            "File is not locked"
          } catch {
            case _: AccessDeniedException => "File is locked (access denied)"
            case ex: IOException          => s"File may be locked: ${ex.getMessage}"
          }
        }
      }
    }
  }
}
