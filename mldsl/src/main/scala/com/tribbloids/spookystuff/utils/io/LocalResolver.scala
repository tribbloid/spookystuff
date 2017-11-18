package com.tribbloids.spookystuff.utils.io

import java.io._
import java.nio.file.FileAlreadyExistsException

object LocalResolver extends URIResolver {

  val lockedSuffix: String = ".locked"

  def ensureAbsolute(file: File) = {
    assert(file.isAbsolute, s"BAD DESIGN: ${file.getPath} is not an absolute path")
  }

  override def input[T](pathStr: String)(f: (InputStream) => T) = {

    //    val file = new File(pathStr)
    //    ensureAbsolute(file)

    if (!pathStr.endsWith(lockedSuffix)) {
      //wait for its locked file to finish its locked session

      val lockedPath = pathStr + lockedSuffix
      val lockedFile = new File(lockedPath)

      //wait for 15 seconds in total
      retry {
        assert(!lockedFile.exists(), s"File $pathStr is locked by another executor or thread")
      }
    }

    val file = new File(pathStr)
    val fis = new FileInputStream(file)


    try {
      val result = f(fis)
      new Resource(result) {
        override lazy val metadata = describe(file)
      }
    }
    finally {
      fis.close()
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T) = {

    val file = new File(pathStr)
    //    ensureAbsolute(file)

    if (file.exists() && !overwrite) throw new FileAlreadyExistsException(s"$pathStr already exists")
    else if (!file.exists()) {
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    else if (file.isDirectory) {
      file.delete()

      file.getParentFile.mkdirs()
      file.createNewFile()
    }

    val fos = new FileOutputStream(pathStr, false)

    try {
      val result = f(fos)
      fos.flush()
      new Resource(result) {
        override lazy val metadata = describe(file)
      }
    }
    finally {
      fos.close()
    }
  }

  override def lockAccessDuring[T](pathStr: String)(f: (String) => T): T = {

    val file = new File(pathStr)
    //    ensureAbsolute(file)

    val lockedPath = pathStr + lockedSuffix
    val lockedFile = new File(lockedPath)

    retry {
      assert(!lockedFile.exists(), s"File $pathStr is locked by another executor or thread")
      //        Thread.sleep(3*1000)
    }

    if (file.exists()) file.renameTo(lockedFile)

    try {
      val result = f(lockedPath)
      result
    }
    finally {
      lockedFile.renameTo(file)
    }
  }

  override def toAbsolute(pathStr: String): String = {
    val file = new File(pathStr)
    file.getAbsolutePath
  }

  def describe(file: File): ResourceMetadata = {

    def getMetadata(file: File) = {
      val name = file.getName
      val map = reflectiveMetadata(file)
      val tpe = if (file.isDirectory)
        Some(URIResolver.DIR_TYPE)
      else
        None
      ResourceMetadata(
        file.getAbsolutePath,
        Option(name),
        tpe,
        map
      )
    }

    val root = {
      getMetadata(file)
    }

    val children = file.listFiles().map {
      file =>
        getMetadata(file)
    }
    root.copy(children = children)
  }
}
