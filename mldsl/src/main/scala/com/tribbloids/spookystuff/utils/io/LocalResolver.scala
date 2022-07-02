package com.tribbloids.spookystuff.utils.io

import java.io.{File, InputStream, OutputStream}
import java.nio.file._
import com.tribbloids.spookystuff.utils.Retry
import org.apache.commons.io.FileUtils

import java.nio.file.attribute.PosixFilePermission

case class LocalResolver(
    override val retry: Retry = URIResolver.default.retry,
    extraPermissions: Set[PosixFilePermission] = Set()
) extends URIResolver {

  @transient lazy val mdParser: ResourceMetadata.ReflectionParser[File] = ResourceMetadata.ReflectionParser[File]()

  override def newExecution(pathStr: String): Execution = {
    Execution(Paths.get(pathStr))
  }
  case class Execution(path: Path) extends super.AbstractExecution {

    import Resource._

    import scala.collection.JavaConverters._

    // CAUTION: resolving is different on each driver or executors
    val absolutePath: Path = path.toAbsolutePath

    // this is an old IO object, usage should be minimised
    //TODO: should embrace NIO 100%?
    // https://java7fs.fandom.com/wiki/Why_File_sucks
    @transient lazy val file: File = path.toFile

    override lazy val absolutePathStr: String = absolutePath.toString

    trait LocalResource[T] extends Resource[T] {

      lazy val existingPath: Path = {
        if (Files.exists(path)) path
        else throw new NoSuchFileException(s"File $path doesn't exist")
      }

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = file.getName

      override lazy val getType: String = {
        if (Files.isDirectory(existingPath)) DIR
        else if (Files.isSymbolicLink(existingPath)) SYMLINK
        else if (Files.isRegularFile(existingPath)) FILE
        else UNKNOWN
      }

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME_OUT
        else Files.probeContentType(existingPath)
      }

      override lazy val getLength: Long = Files.size(existingPath)

      override lazy val getLastModified: Long = Files.getLastModifiedTime(existingPath).toMillis

      override lazy val _metadata: ResourceMetadata = {
        // TODO: use Files.getFileAttributeView
        mdParser(file)
      }

      override lazy val children: Seq[Execution] = {
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
    }

    override def input[T](fn: InputResource => T): T = {

      val ir = new InputResource with LocalResource[InputStream] {

        override def createStream: InputStream = {

          Files.newInputStream(path)
        }
      }

      try {
        fn(ir)
      } finally {
        ir.clean()
      }
    }

    override def output[T](mode: WriteMode)(fn: OutputResource => T): T = {

      val or = new OutputResource with LocalResource[OutputStream] {

        override def createStream: OutputStream = {
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
              if (!file.exists()) {

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

      try {
        val result = fn(or)
        result
      } finally {
        or.clean()
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

      Files.createDirectories(absolutePath.getParent)

      if (force)
        Files.move(path, newPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      else
        Files.move(path, newPath, StandardCopyOption.ATOMIC_MOVE)
    }
  }
}

object LocalResolver extends LocalResolver(URIResolver.default.retry, Set())
