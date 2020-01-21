package com.tribbloids.spookystuff.utils.io

//TODO: should embrace NIO 100%?
// https://java7fs.fandom.com/wiki/Why_File_sucks
import java.io._
import java.nio.file.{FileAlreadyExistsException, Files, Paths, StandardCopyOption}

import com.tribbloids.spookystuff.utils.Retry
import org.apache.hadoop.fs.FileUtil

case class LocalResolver(
    override val retry: Retry = URIResolver.default.retry
) extends URIResolver {

  @transient lazy val mdParser: ResourceMetadata.ReflectionParser[File] = ResourceMetadata.ReflectionParser[File]()

  override def newSession(pathStr: String): Session = Session(new File(pathStr))
  case class Session(file: File) extends super.URISession {
    //    ensureAbsolute(file)

    import Resource._

    lazy val absoluteFile: File = file.getAbsoluteFile

    override lazy val absolutePathStr: String = file.getAbsolutePath

    trait LocalResource[T] extends Resource[T] {

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = file.getName

      override lazy val getType: String = {
        if (file.isDirectory) DIR
        else if (file.isFile) "file"
        else UNKNOWN
      }

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME
        else Files.probeContentType(Paths.get(absolutePathStr))
      }

      override lazy val getLength: Long = file.length()

      override lazy val getLastModified: Long = file.lastModified()

      override lazy val _metadata: ResourceMetadata = {
        mdParser(file)
      }

      override lazy val isExisting: Boolean = file.exists()

      override lazy val children: Seq[URISession] = {
        if (isDirectory) {

          file
            .listFiles()
            .toSeq
            .map { file =>
              Session(file)
            }
        } else Nil
      }
    }

    override def input[T](fn: InputResource => T): T = {

      val ir = new InputResource with LocalResource[InputStream] {

        override def createStream: InputStream = {

          new FileInputStream(file)
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
              file.createNewFile()
              new FileOutputStream(absolutePathStr, false)

            case (true, WriteMode.Append) =>
              new FileOutputStream(absolutePathStr, true)

            case (false, _) =>
              absoluteFile.getParentFile.mkdirs()
              file.createNewFile()
              new FileOutputStream(absolutePathStr, false)
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
      val isExisting = file.exists()

      (isExisting, mustExist) match {
        case (false, true) => throw new UnsupportedOperationException(s"file ${file.toURI} does not exist")
        case _             => FileUtil.fullyDelete(absoluteFile, true)
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
