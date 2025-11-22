package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.util.PathMagnet.URIPath
import ai.acyclic.prover.commons.util.{PathMagnet, Retry}
import com.tribbloids.spookystuff.commons.data.ReflCanUnapply
import org.apache.commons.io.FileUtils

import java.io.{File, InputStream, OutputStream}
import java.nio.file.*
import java.nio.file.attribute.{DosFileAttributeView, PosixFilePermission}

case class LocalResolver(
    override val retry: Retry = URIResolver.default.retry,
    writePermission: Set[FilePermissionType] = Set()
) extends URIResolver {
  import LocalResolver.*

  implicit class _Execution(
      originalPath: PathMagnet.URIPath
  ) extends Execution {

    import Resource.*
    import scala.jdk.CollectionConverters.*

    val nioPath: Path = Paths.get(originalPath.normaliseToLocal)

    val absoluteNioPath: Path = nioPath.toAbsolutePath

    // CAUTION: resolving is different on each driver or executors
    override val absolutePath: URIPath = PathMagnet.URIPath(absoluteNioPath.toString)

    // this is an old IO object, usage should be minimised
    // TODO: should embrace NIO 100%?
    // https://java7fs.fandom.com/wiki/Why_File_sucks
    @transient lazy val file: File = nioPath.toFile

    object _Resource extends (WriteMode => _Resource)
    case class _Resource(mode: WriteMode) extends Resource {

      override protected def _outer: URIExecution = _Execution.this

      override lazy val getName: String = nioPath.getFileName.toString

      override lazy val getType: String = {
        if (Files.isDirectory(nioPath)) DIR
        else if (Files.isSymbolicLink(nioPath)) SYMLINK
        else if (Files.isRegularFile(nioPath)) FILE
        else if (isExisting) UNKNOWN
        else throw new NoSuchFileException(s"File $nioPath doesn't exist")
      }

      override def _requireExisting(): Unit = require(file.exists())

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME_OUT
        else Files.probeContentType(nioPath)
      }

      override lazy val getLength: Long = Files.size(nioPath)

      override lazy val getLastModified: Long = Files.getLastModifiedTime(nioPath).toMillis

      override lazy val extraMetadata: ResourceMetadata = {
        // TODO: use Files.getFileAttributeView
        val unapplied = unapplyFile.unapply(file)
        ResourceMetadata.BuildFrom.unappliedForm(unapplied)
      }

      override lazy val children: Seq[_Execution] = {
        if (isDirectory) {

          Files
            .newDirectoryStream(nioPath)
            .iterator()
            .asScala
            .toSeq
            .map { subPath =>
              on(subPath.toString)
            }
        } else Nil
      }

      override protected def _newIStream: InputStream = {
        Files.newInputStream(nioPath)
      }

      override protected def _newOStream: OutputStream = {

        val fos = (isExisting, mode) match {

          case (true, WriteMode.ErrorIfExists) =>
            throw new FileAlreadyExistsException(s"$absolutePath already exists")

          case (true, WriteMode.Overwrite) =>
            delete(false)
            //              Files.createFile(path)
            Files.newOutputStream(nioPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC)

          case (true, WriteMode.Append) =>
            Files.newOutputStream(nioPath, StandardOpenOption.APPEND, StandardOpenOption.SYNC)

          case (false, _) =>
            if (!isExisting) {

              Files.createDirectories(absoluteNioPath.getParent)
              //              Files.createFile(path)
            }
            Files.newOutputStream(nioPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC)
        }

        // Apply file permissions in a cross-platform way
        applyCrossPlatformPermissions(nioPath, writePermission)

        fos
      }
    }

    override def _delete(mustExist: Boolean): Unit = {

      (isExisting, mustExist) match {
        case (false, false) =>
        case _              => FileUtils.forceDelete(file)
      }
    }

    override def moveTo(target: String, force: Boolean = false): Unit = {

      val newPath = Paths.get(target)

      Files.createDirectories(newPath.getParent)

      if (force)
        Files.move(absoluteNioPath, newPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      else
        Files.move(absoluteNioPath, newPath, StandardCopyOption.ATOMIC_MOVE)
    }

    /**
      * Apply file permissions in a cross-platform way
      */
    private def applyCrossPlatformPermissions(path: Path, permissions: Set[FilePermissionType]): Unit = {
      if (permissions.isEmpty) return

      try {
        if (CrossPlatformFileUtils.isUnix) {
          // On Unix/Linux/macOS, use POSIX permissions
          applyPosixPermissions(path, permissions)
        } else if (CrossPlatformFileUtils.isWindows) {
          // On Windows, use DOS file attributes
          applyDosPermissions(path, permissions)
        }
      } catch {
        case _: UnsupportedOperationException =>
        // Permissions not supported on this platform, ignore
        case _: java.nio.file.FileSystemException =>
        // Filesystem doesn't support these permissions, ignore
        case _: Exception =>
        // Other errors (like insufficient privileges), ignore gracefully
      }
    }

    /**
      * Apply permissions on Unix-like systems using POSIX attributes
      */
    private def applyPosixPermissions(path: Path, permissions: Set[FilePermissionType]): Unit = {
      try {
        val currentPermissions = Files.getPosixFilePermissions(path)

        val posixPermissions = permissions.flatMap {
          case FilePermissionType.Executable =>
            Set(
              PosixFilePermission.OWNER_EXECUTE,
              PosixFilePermission.GROUP_EXECUTE,
              PosixFilePermission.OTHERS_EXECUTE
            )
          case FilePermissionType.Writable =>
            Set(
              PosixFilePermission.OWNER_WRITE,
              PosixFilePermission.GROUP_WRITE,
              PosixFilePermission.OTHERS_WRITE
            )
          case FilePermissionType.Readable =>
            Set(
              PosixFilePermission.OWNER_READ,
              PosixFilePermission.GROUP_READ,
              PosixFilePermission.OTHERS_READ
            )
        }

        currentPermissions.addAll(posixPermissions.asJava)
        Files.setPosixFilePermissions(path, currentPermissions)
      } catch {
        case _: UnsupportedOperationException =>
          // POSIX permissions not supported, fall back to basic file permissions
          applyBasicUnixPermissions(path, permissions)
      }
    }

    /**
      * Apply basic Unix permissions when POSIX is not available
      */
    private def applyBasicUnixPermissions(path: Path, permissions: Set[FilePermissionType]): Unit = {
      val file = path.toFile

      permissions.foreach {
        case FilePermissionType.Executable => file.setExecutable(true)
        case FilePermissionType.Writable   => file.setWritable(true)
        case FilePermissionType.Readable   => file.setReadable(true)
      }
    }

    /**
      * Apply permissions on Windows using DOS file attributes
      */
    private def applyDosPermissions(path: Path, permissions: Set[FilePermissionType]): Unit = {
      val currentAttrs = Files.getFileAttributeView(path, classOf[DosFileAttributeView])

      if (currentAttrs != null) {
        val currentDosAttrs = currentAttrs.readAttributes()

        var isReadOnly = currentDosAttrs.isReadOnly
        var isHidden = currentDosAttrs.isHidden
        var isSystem = currentDosAttrs.isSystem
        var isArchive = currentDosAttrs.isArchive

        permissions.foreach {
          case FilePermissionType.Writable =>
            isReadOnly = false // Ensure file is writable
          case FilePermissionType.Readable =>
            isReadOnly = false // Ensure file is readable (not read-only)
          case FilePermissionType.Executable =>
            // On Windows, executability is typically determined by file extension
            // We can't easily set executable bit, but we ensure it's not read-only
            isReadOnly = false
        }

        // Update the attributes
        currentAttrs.setReadOnly(isReadOnly)
        currentAttrs.setHidden(isHidden)
        currentAttrs.setSystem(isSystem)
        currentAttrs.setArchive(isArchive)
      }

      // Fallback to basic File methods if DOS attributes don't work
      val file = path.toFile
      permissions.foreach {
        case FilePermissionType.Writable   => file.setWritable(true)
        case FilePermissionType.Readable   => file.setReadable(true)
        case FilePermissionType.Executable => file.setExecutable(true) // May not work on Windows but won't hurt
      }
    }
  }
}

object LocalResolver {

  object default
      extends LocalResolver(
        URIResolver.default.retry,
        Set()
      )

  lazy val unapplyFile: ReflCanUnapply[File] = ReflCanUnapply[File]()
}
