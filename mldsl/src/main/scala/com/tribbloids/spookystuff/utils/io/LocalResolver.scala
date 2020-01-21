package com.tribbloids.spookystuff.utils.io

import java.io.{File, InputStream, OutputStream}
import java.nio.file._

import com.tribbloids.spookystuff.utils.Retry
import org.apache.commons.io.FileUtils

case class LocalResolver(
    override val retry: Retry = URIResolver.default.retry
) extends URIResolver {

  @transient lazy val mdParser: ResourceMetadata.ReflectionParser[File] = ResourceMetadata.ReflectionParser[File]()

  override def newSession(pathStr: String): Session = {
    Session(Paths.get(pathStr))
  }
  case class Session(path: Path) extends super.URISession {

    import Resource._

    import scala.collection.JavaConverters._

    // this is an old IO object, usage should be minimised
    //TODO: should embrace NIO 100%?
    // https://java7fs.fandom.com/wiki/Why_File_sucks
    lazy val file: File = path.toFile

    lazy val absolutePath: Path = path.toAbsolutePath

    override lazy val absolutePathStr: String = absolutePath.toString

    trait LocalResource[T] extends Resource[T] {

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = file.getName

      override lazy val getType: String = {
        if (Files.isDirectory(path)) DIR
        else if (Files.isRegularFile(path)) FILE
        else UNKNOWN
      }

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME
        else Files.probeContentType(path)
      }

      override lazy val getLength: Long = Files.size(path)

      override lazy val getLastModified: Long = Files.getLastModifiedTime(path).toMillis

      override lazy val _metadata: ResourceMetadata = {
        // TODO: use Files.getFileAttributeView
        mdParser(file)
      }

      override lazy val isExisting: Boolean = Files.exists(path)

      override lazy val children: Seq[URISession] = {
        if (isDirectory) {

          Files
            .newDirectoryStream(path)
            .iterator()
            .asScala
            .toSeq
            .map { subPath =>
              Session(subPath)
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
              Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)

            case (true, WriteMode.Append) =>
              Files.newOutputStream(path, StandardOpenOption.APPEND)

            case (false, _) =>
              Files.createDirectories(absolutePath.getParent)
//              Files.createFile(path)
              Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)
          }

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

    override def moveTo(target: String): Unit = {

      val newFile = new File(target).getAbsoluteFile
      newFile.getParentFile.mkdirs()

      Files.move(file.toPath, newFile.toPath, StandardCopyOption.ATOMIC_MOVE)
    }

//    override def mkDirs(): Unit = {
//
//      file.mkdirs()
//    }
  }
}

object LocalResolver extends LocalResolver(URIResolver.default.retry)
