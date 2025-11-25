package com.tribbloids.spookystuff.io

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong
import scala.util.{Failure, Success, Try}
import scala.collection.mutable
import scala.concurrent.duration._
import java.time.Instant
import ai.acyclic.prover.commons.util.Retry

/**
  * Specialized file handling utilities designed for test scenarios. Provides robust temporary file management and
  * cross-platform test support.
  */
object TestFileHelpers {

  // Counter for generating unique test file names
  private val testCounter = new AtomicLong(0)

  // Registry for tracking test resources for cleanup
  private val testResourceRegistry = mutable.Set[TestResource]()

  // Test-wide temporary directory (created on first use)
  private var testTempDir: Option[Path] = None

  /**
    * Represents a test resource that needs cleanup
    */
  case class TestResource(
      path: Path,
      resourceType: String,
      createdAt: Instant = Instant.now(),
      autoCleanup: Boolean = true
  ) {
    def cleanup(): Try[Boolean] = {
      resourceType.toLowerCase match {
        case "file"      => TestFileHelpers.safeDeleteFile(path)
        case "directory" => TestFileHelpers.safeDeleteDirectory(path)
        case _           => TestFileHelpers.safeDeleteFile(path)
      }
    }
  }

  /**
    * Gets or creates the test-wide temporary directory
    */
  def getTestTempDirectory(): Path = {
    testTempDir.synchronized {
      testTempDir match {
        case Some(dir) if Files.exists(dir) => dir
        case _                              =>
          val newDir = CrossPlatformFileUtils.createTempDirectory(
            prefix = s"spooky-test-${System.currentTimeMillis()}"
          )
          testTempDir = Some(newDir)
          registerTestResource(newDir, "directory")
          newDir
      }
    }
  }

  /**
    * Creates a test-specific temporary file
    */
  def createTestTempFile(
      prefix: String = "test",
      suffix: String = ".tmp",
      content: Option[String] = None,
      registerForCleanup: Boolean = true
  ): Try[Path] = {
    val uniquePrefix = s"$prefix-${testCounter.incrementAndGet()}"

    WindowsFileCompatibility
      .createWindowsCompatibleTempFile(
        prefix = uniquePrefix,
        suffix = suffix,
        directory = Some(getTestTempDirectory())
      )
      .map { path =>
        // Write content if provided
        content.foreach { content =>
          Files.writeString(path, content)
        }

        if (registerForCleanup) {
          registerTestResource(path, "file")
        }

        path
      }
  }

  /**
    * Creates a test-specific temporary directory
    */
  def createTestTempDirectory(
      prefix: String = "test",
      registerForCleanup: Boolean = true
  ): Try[Path] = {
    val uniquePrefix = s"$prefix-${testCounter.incrementAndGet()}"

    WindowsFileCompatibility
      .createWindowsCompatibleTempDirectory(
        prefix = uniquePrefix,
        directory = Some(getTestTempDirectory())
      )
      .map { path =>
        if (registerForCleanup) {
          registerTestResource(path, "directory")
        }

        path
      }
  }

  /**
    * Creates a test file with specific content
    */
  def createTestFile(
      fileName: String,
      content: String,
      directory: Option[Path] = None
  ): Try[Path] = {
    val targetDir = directory.getOrElse(getTestTempDirectory())
    val filePath = targetDir.resolve(WindowsFileCompatibility.sanitizeFileName(fileName))

    Try {
      Files.createDirectories(targetDir)
      Files.writeString(filePath, content)
      registerTestResource(filePath, "file")
      filePath
    }
  }

  /**
    * Creates a test directory structure
    */
  def createTestDirectoryStructure(
      basePath: Path,
      structure: Map[String, Either[String, Map[String, Any]]]
  ): Try[Path] = {
    Try {
      Files.createDirectories(basePath)
      registerTestResource(basePath, "directory")

      def createStructure(currentPath: Path, items: Map[String, Either[String, Map[String, Any]]]): Unit = {
        items.foreach {
          case (name, Left(content)) => // File with content
            val filePath = currentPath.resolve(name)
            Files.writeString(filePath, content)
            registerTestResource(filePath, "file")

          case (name, Right(subStructure)) => // Directory
            val dirPath = currentPath.resolve(name)
            Files.createDirectories(dirPath)
            registerTestResource(dirPath, "directory")

            // Convert sub-structure to proper format
            val subItems = subStructure.collect {
              case (k, v: String)    => k -> Left(v)
              case (k, v: Map[_, _]) =>
                k -> Right(v.map {
                  case (kk: String, vv: String)    => kk -> Left(vv)
                  case (kk: String, vv: Map[_, _]) => kk -> Right(vv.asInstanceOf[Map[String, Any]])
                  case (kk, vv)                    => kk.toString -> Left(vv.toString)
                })
            }
            createStructure(dirPath, subItems)
        }
      }

      createStructure(basePath, structure)
      basePath
    }
  }

  /**
    * Registers a test resource for cleanup
    */
  def registerTestResource(path: Path, resourceType: String): Unit = {
    testResourceRegistry += TestResource(path, resourceType)
  }

  /**
    * Unregisters a test resource from cleanup
    */
  def unregisterTestResource(path: Path): Unit = {
    testResourceRegistry --= testResourceRegistry.filter(_.path == path)
  }

  /**
    * Safely deletes a file with Windows-specific considerations
    */
  def safeDeleteFile(path: Path): Try[Boolean] = {
    if (!Files.exists(path)) {
      Success(true) // Already deleted
    } else if (Files.isDirectory(path)) {
      Success(false) // Not a file
    } else {
      WindowsFileCompatibility.safeFileDelete(path)
    }
  }

  /**
    * Safely deletes a directory with all its contents
    */
  def safeDeleteDirectory(path: Path): Try[Boolean] = {
    if (!Files.exists(path)) {
      Success(true) // Already deleted
    } else if (!Files.isDirectory(path)) {
      Success(false) // Not a directory
    } else {
      Try {
        Files.walkFileTree(
          path,
          new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
              safeDeleteFile(file).get
              FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
              if (exc != null) throw exc
              Files.deleteIfExists(dir)
              FileVisitResult.CONTINUE
            }
          }
        )
        true
      }
    }
  }

  /**
    * Cleans up all registered test resources
    */
  def cleanupTestResources(): Unit = {
    val resources = testResourceRegistry.toSet
    testResourceRegistry.clear()

    // Sort by creation time (oldest first) to avoid dependencies
    resources.toSeq.sortBy(_.createdAt).foreach { resource =>
      resource.cleanup().recover {
        case ex =>
          System.err.println(s"Failed to cleanup test resource ${resource.path}: ${ex.getMessage}")
      }
    }

    // Clean up the main test temp directory if it exists
    testTempDir.foreach { dir =>
      if (Files.exists(dir)) {
        safeDeleteDirectory(dir).recover {
          case ex =>
            System.err.println(s"Failed to cleanup test temp directory $dir: ${ex.getMessage}")
        }
      }
      testTempDir = None
    }
  }

  /**
    * Executes a test with automatic resource cleanup
    */
  def withTestResources[T](testBody: => T): Try[T] = {
    try {
      val result = testBody
      Success(result)
    } catch {
      case ex: Throwable => Failure(ex)
    } finally {
      cleanupTestResources()
    }
  }

  /**
    * Executes a test with automatic resource cleanup in a Try context
    */
  def withTestResourcesTry[T](testBody: => Try[T]): Try[T] = {
    try {
      val result = testBody
      result
    } finally {
      cleanupTestResources()
    }
  }

  /**
    * Asserts file exists with cross-platform considerations
    */
  def assertFileExists(path: Path): Unit = {
    if (!CrossPlatformFileUtils.pathExists(path)) {
      throw new AssertionError(s"File does not exist: $path")
    }
  }

  /**
    * Asserts file does not exist with cross-platform considerations
    */
  def assertFileNotExists(path: Path): Unit = {
    if (CrossPlatformFileUtils.pathExists(path)) {
      throw new AssertionError(s"File unexpectedly exists: $path")
    }
  }

  /**
    * Asserts file content matches expected string
    */
  def assertFileContent(path: Path, expectedContent: String): Unit = {
    assertFileExists(path)
    val actualContent = Files.readString(path)
    if (actualContent != expectedContent) {
      throw new AssertionError(s"File content mismatch. Expected: '$expectedContent', Actual: '$actualContent'")
    }
  }

  /**
    * Asserts file content contains expected substring
    */
  def assertFileContains(path: Path, expectedSubstring: String): Unit = {
    assertFileExists(path)
    val content = Files.readString(path)
    if (!content.contains(expectedSubstring)) {
      throw new AssertionError(s"File does not contain expected substring '$expectedSubstring'. Content: '$content'")
    }
  }

  /**
    * Creates a file with Windows-compatible path handling
    */
  def createFileWithWindowsCompatibility(
      path: Path,
      content: String = "",
      overwrite: Boolean = false
  ): Try[Path] = {
    if (CrossPlatformFileUtils.isWindows) {
      WindowsFileCompatibility.retryWithBackoff { () =>
        Try {
          Retry.ExponentialBackoff(
            n = 10,
            longestInterval = 2000L, // 2 seconds max
            expBase = 2.0,
            silent = false
          ) {
            if (Files.exists(path) && !overwrite) {
              throw new FileAlreadyExistsException(s"File already exists: $path")
            }

            Files.createDirectories(path.getParent)
            Files.writeString(path, content)
            path
          }
        }
      }
    } else {
      Try {
        Files.createDirectories(path.getParent)
        if (overwrite || !Files.exists(path)) {
          Files.writeString(path, content)
        }
        path
      }
    }
  }

  /**
    * Waits for file operations to complete on Windows
    */
  def waitForFileOperation(
      path: Path,
      timeout: FiniteDuration = 5.seconds,
      checkInterval: FiniteDuration = 100.milliseconds
  ): Try[Boolean] = {
    if (CrossPlatformFileUtils.isWindows) {
      WindowsFileCompatibility.waitForFileAvailability(path, timeout, checkInterval)
    } else {
      Success(Files.exists(path))
    }
  }

  /**
    * Gets detailed information about a test resource
    */
  def getResourceInfo(path: Path): Try[String] = {
    Try {
      if (!Files.exists(path)) {
        "File does not exist"
      } else {
        val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
        val size = if (attrs.isRegularFile) Files.size(path) else 0L
        val lastModified = Files.getLastModifiedTime(path)

        s"""Path: $path
           |Type: ${if (attrs.isDirectory) "Directory" else if (attrs.isRegularFile) "File" else "Other"}
           |Size: $size bytes
           |Last Modified: $lastModified
           |Created: ${attrs.creationTime()}
           |Readable: ${Files.isReadable(path)}
           |Writable: ${Files.isWritable(path)}
           |Windows Lock Info: ${WindowsFileCompatibility.getWindowsFileLockInfo(path).getOrElse("N/A")}
           |""".stripMargin
      }
    }
  }

  // Register cleanup hook for test resources
  sys.addShutdownHook {
    cleanupTestResources()
  }
}
