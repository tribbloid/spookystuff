package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.util.{PathMagnet, Retry}
import com.tribbloids.spookystuff.commons.data.ReflCanUnapply
import org.apache.commons.io.FileUtils

import java.io.{File, InputStream, OutputStream}
import java.nio.file.*
import java.nio.file.attribute.PosixFilePermission

case class LocalResolver(
    override val retry: Retry = URIResolver.default.retry,
    extraPermissions: Set[PosixFilePermission] = Set()
) extends URIResolver {
  import LocalResolver.*

  implicit class _Execution(
      originalPath: PathMagnet.URIPath
  ) extends Execution {

    import Resource.*
    import scala.jdk.CollectionConverters.*

    val nioPath: Path = Paths.get(originalPath.normaliseToLocal)

    val absoluteNioPath = nioPath.toAbsolutePath

    // CAUTION: resolving is different on each driver or executors
    override val absolutePath = PathMagnet.URIPath(absoluteNioPath.toString)

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

        // Only set POSIX permissions on systems that support them (not Windows)
        if (CrossPlatformFileUtils.isUnix && extraPermissions.nonEmpty) {
          try {
            val permissions = Files.getPosixFilePermissions(nioPath)
            permissions.addAll(extraPermissions.asJava)
            Files.setPosixFilePermissions(nioPath, permissions)
          } catch {
            case _: UnsupportedOperationException =>
              // POSIX permissions not supported on this platform (e.g., Windows), ignore
            case _: java.nio.file.FileSystemException =>
              // Filesystem doesn't support POSIX permissions, ignore
          }
        }

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
