package com.tribbloids.spookystuff.utils

import java.io._
import java.nio.file.FileAlreadyExistsException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
;
abstract class PathResolver {
  def input[T](pathStr: String)(f: InputStream => T): T

  def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T): T
}

case object LocalResolver extends PathResolver {

  override def input[T](pathStr: String)(f: (InputStream) => T): T = {
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
    if (file.exists() && !overwrite) throw new FileAlreadyExistsException(s"$pathStr exists")
    else if (!file.exists()) {
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
}

case class HDFSResolver(conf: Configuration) extends PathResolver {

  def input[T](pathStr: String)(f: InputStream => T): T = Utils.retry(3){
    val path = new Path(pathStr)
    val fs = path.getFileSystem(conf)

    val fis = fs.open(path)

    try {
      f(fis)
    }
    finally {
      fis.close()
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T): T = Utils.retry(3){
    val path = new Path(pathStr)
    val fs = path.getFileSystem(conf)

    val fos = fs.create(path, overwrite)

    try {
      val result = f(fos)
      fos.flush()
      result
    }
    finally {
      fos.close()
    }
  }
}