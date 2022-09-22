package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.Retry
import org.apache.commons.io.FileUtils

import java.io.{File, InputStream, OutputStream}
import java.nio.file._
import java.nio.file.attribute.PosixFilePermission

case class LocalResolver(
    override val retry: Retry = URIResolver.default.retry,
    extraPermissions: Set[PosixFilePermission] = Set()
) extends URIResolver {

  @transient lazy val metadataParser: ResourceMetadata.ReflectionParser[File] =
    ResourceMetadata.ReflectionParser[File]()

  case class _Execution(pathStr: String) extends Execution {

    import Resource._

    val path: Path = Paths.get(pathStr)

    import scala.collection.JavaConverters._

    // CAUTION: resolving is different on each driver or executors
    val absolutePath: Path = path.toAbsolutePath

    // this is an old IO object, usage should be minimised
    //TODO: should embrace NIO 100%?
    // https://java7fs.fandom.com/wiki/Why_File_sucks
    @transient lazy val file: File = path.toFile

    override lazy val absolutePathStr: String = absolutePath.toString

    case class _Resource(mode: WriteMode) extends Resource {

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = path.getFileName.toString

      override lazy val getType: String = {
        if (Files.isDirectory(path)) DIR
        else if (Files.isSymbolicLink(path)) SYMLINK
        else if (Files.isRegularFile(path)) FILE
        else if (isExisting) UNKNOWN
        else throw new NoSuchFileException(s"File $path doesn't exist")
      }

      override def isExisting: Boolean = {
        file.exists()
      }

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME_OUT
        else Files.probeContentType(path)
      }

      override lazy val getLength: Long = Files.size(path)

      override lazy val getLastModified: Long = Files.getLastModifiedTime(path).toMillis

      override lazy val _metadata: ResourceMetadata = {
        // TODO: use Files.getFileAttributeView
        metadataParser(file)
      }

      override lazy val children: Seq[_Execution] = {
        if (isDirectory) {

          Files
            .newDirectoryStream(path)
            .iterator()
            .asScala
            .toSeq
            .map { subPath =>
              execute(subPath.toString)
            }
        } else Nil
      }

      override protected def _newIStream: InputStream = {
        Files.newInputStream(path)
      }

      override protected def _newOStream: OutputStream = {

        val fos = (isExisting, mode) match {

          case (true, WriteMode.CreateOnly) =>
            throw new FileAlreadyExistsException(s"$absolutePathStr already exists")

          case (true, WriteMode.Overwrite) =>
            delete(false)
            //              Files.createFile(path)
            Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC)

          case (true, WriteMode.Append) =>
            Files.newOutputStream(path, StandardOpenOption.APPEND, StandardOpenOption.SYNC)

          case (false, _) =>
            if (!isExisting) {

              Files.createDirectories(absolutePath.getParent)
              //              Files.createFile(path)
            }
            Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC)
        }

        val permissions = Files.getPosixFilePermissions(path)
        permissions.addAll(extraPermissions.asJava)
        Files.setPosixFilePermissions(path, permissions)

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
        Files.move(absolutePath, newPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      else
        Files.move(absolutePath, newPath, StandardCopyOption.ATOMIC_MOVE)
    }
  }
}

object LocalResolver extends LocalResolver(URIResolver.default.retry, Set())
