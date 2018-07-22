package com.tribbloids.spookystuff.utils.io

import java.io._
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

object LocalResolver extends URIResolver {

  val file2MD = ResourceMD.ReflectionParser[File]()

  override lazy val unlockForInput: Boolean = true

  override def Execution(pathStr: String) = Execution(new File(pathStr))
  case class Execution(file: File) extends super.Execution {
    //    ensureAbsolute(file)

    import Resource._

    override lazy val absolutePathStr: String = {
      file.getAbsolutePath
    }

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

      override lazy val getLenth: Long = file.length()

      override lazy val getLastModified: Long = file.lastModified()

      override lazy val _metadata: ResourceMD = {
        file2MD(file)
      }

      override lazy val isAlreadyExisting: Boolean = file.exists()

      override lazy val children: Seq[ResourceMD] = {
        if (isDirectory) {

          file.listFiles().toSeq
            .map {
              file =>
                val childExecution = Execution(file)
                val md = childExecution.input {
                  _.rootMetadata
                }
                md
            }
        }
        else Nil
      }
    }

    override def input[T](f: InputResource => T): T = {

      val ir = new InputResource with LocalResource[InputStream] {

        override def _stream: InputStream = {
          //          if (!absolutePathStr.endsWith(lockedSuffix)) {
          //            //wait for its locked file to finish its locked session
          //
          //            val lockedPath = absolutePathStr + lockedSuffix
          //            val lockedFile = new File(lockedPath)
          //
          //            //wait for 15 seconds in total
          //            retry {
          //              assert(!lockedFile.exists(),
          //                s"File $absolutePathStr is locked by another executor or thread")
          //            }
          //          }

          new FileInputStream(file)
        }
      }

      try {
        f(ir)
      }
      finally {
        ir.clean()
      }
    }

    override def _remove(mustExist: Boolean): Unit = { //TODO: validate mustExist
      file.delete()
    }

    override def output[T](overwrite: Boolean)(f: OutputResource => T): T = {

      val or = new OutputResource with LocalResource[OutputStream] {

        override def _stream: OutputStream = {
          (isAlreadyExisting, overwrite) match {
            case (true, false) => throw new FileAlreadyExistsException(s"$absolutePathStr already exists")
            case (true, true) =>
              remove(false)
              file.createNewFile()
            case (false, _) =>
              file.getParentFile.mkdirs()
              file.createNewFile()
          }

          val fos = new FileOutputStream(absolutePathStr, false)
          fos
        }
      }

      try {
        val result = f(or)
        result
      }
      finally {
        or.clean()
      }
    }
  }
  //
  //  override def lockAccessDuring[T](pathStr: String)(f: String => T): T = {
  //
  //    val file = new File(pathStr)
  //    //    ensureAbsolute(file)
  //
  //    val lockedPath = pathStr + lockedSuffix
  //    val lockedFile = new File(lockedPath)
  //
  //    retry {
  //      assert(!lockedFile.exists(),
  //        s"File $pathStr is locked by another executor or thread")
  //      //        Thread.sleep(3*1000)
  //    }
  //
  //    if (file.exists()) file.renameTo(lockedFile)
  //
  //    try {
  //      val result = f(lockedPath)
  //      result
  //    }
  //    finally {
  //      lockedFile.renameTo(file)
  //    }
  //  }
}
