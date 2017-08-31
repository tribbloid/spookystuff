package com.tribbloids.spookystuff.utils

import java.io._
import java.nio.file.FileAlreadyExistsException

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class PathResolver extends Serializable {

  def input[T](pathStr: String)(f: InputStream => T): T

  def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T): T

  def lockAccessDuring[T](pathStr: String)(f: String => T): T

  def toAbsolute(pathStr: String): String

  final def isAbsolute(pathStr: String) = {
    toAbsolute(pathStr) == pathStr
  }

  def resourceOrAbsolute(pathStr: String): String = {
    val resourcePath = CommonUtils.getCPResource(pathStr.stripPrefix("/")).map(_.getPath).getOrElse(pathStr)

    val result = this.toAbsolute(resourcePath)
    result
  }
}

object LocalResolver extends PathResolver {

  val lockedSuffix: String = ".locked"

  def ensureAbsolute(file: File) = {
    assert(file.isAbsolute, s"BAD DESIGN: ${file.getPath} is not an absolute path")
  }

  override def input[T](pathStr: String)(f: (InputStream) => T): T = {

    //    val file = new File(pathStr)
    //    ensureAbsolute(file)

    if (!pathStr.endsWith(lockedSuffix)) {
      //wait for its locked file to finish its locked session

      val lockedPath = pathStr + lockedSuffix
      val lockedFile = new File(lockedPath)

      //wait for 15 seconds in total
      CommonUtils.retry(CommonConst.DFSBlockedAccessRetries) {
        assert(!lockedFile.exists(), s"File $pathStr is locked by another executor or thread")
        //        Thread.sleep(3*1000)
      }
    }

    val fis = new FileInputStream(pathStr)

    try {
      f(fis)
    }
    finally {
      fis.close()
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T): T = {

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
      result
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

    CommonUtils.retry(CommonConst.DFSBlockedAccessRetries) {
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
}



