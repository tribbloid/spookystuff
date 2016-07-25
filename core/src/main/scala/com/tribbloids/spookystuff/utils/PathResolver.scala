package com.tribbloids.spookystuff.utils

import java.io._
import java.nio.ByteBuffer
import java.nio.file.FileAlreadyExistsException

import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{SerializableWritable, SparkConf}

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node
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
    val resourcePath = SpookyUtils.getCPResource(pathStr.stripPrefix("/")).map(_.getPath).getOrElse(pathStr)

    val result = this.toAbsolute(resourcePath)
    result
  }
}

object LocalResolver extends PathResolver {

  val lockedSuffix: String = ".locked"

  def ensureAbsolute(file: File) = assert(file.isAbsolute, s"BAD DESIGN: ${file.getPath} is not an absolute path")

  override def input[T](pathStr: String)(f: (InputStream) => T): T = {

    val file = new File(pathStr)
    ensureAbsolute(file)

    if (!pathStr.endsWith(lockedSuffix)) {
      //wait for its locked file to finish its locked session

      val lockedPath = pathStr + lockedSuffix
      val lockedFile = new File(lockedPath)

      //wait for 15 seconds in total
      SpookyUtils.retry(5) {
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
    ensureAbsolute(file)

    if (file.exists() && !overwrite) throw new FileAlreadyExistsException(s"$pathStr already exists")
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

  override def lockAccessDuring[T](pathStr: String)(f: (String) => T): T = {

    val file = new File(pathStr)
    ensureAbsolute(file)

    val lockedPath = pathStr + lockedSuffix
    val lockedFile = new File(lockedPath)

    SpookyUtils.retry(5) {
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

case class HDFSResolver(
                         @transient hadoopConf: Configuration
                       ) extends PathResolver {

  def lockedSuffix: String = ".locked"

  @transient lazy val ser = new JavaSerializer(new SparkConf()).newInstance()

  //TODO: the following are necessary to bypass SPARK-7708, try to remove in the future
  @transient lazy val configWrapper: SerializableWritable[Configuration] = if (hadoopConf != null)
    new SerializableWritable[Configuration](hadoopConf)
  else
    ser.deserialize[SerializableWritable[Configuration]](ByteBuffer.wrap(configWrapperSerialized))

  val configWrapperSerialized: Array[Byte] = ser.serialize(configWrapper).array()

  override def toString = s"${this.getClass.getSimpleName}($configWrapper)"

  def ensureAbsolute(path: Path) = {
    assert(path.isAbsolute, s"BAD DESIGN: ${path.toString} is not an absolute path")
  }

  def input[T](pathStr: String)(f: InputStream => T): T = SpookyUtils.retry(3){
    val path: Path = new Path(pathStr)
//    ensureAbsolute(path)

    val fs = path.getFileSystem(configWrapper.value)

    if (!pathStr.endsWith(lockedSuffix)) {
      //wait for its locked file to finish its locked session

      val lockedPath = new Path(pathStr + lockedSuffix)

      //wait for 15 seconds in total
      SpookyUtils.retry(10) {
        assert(!fs.exists(lockedPath), s"File $pathStr is locked by another executor or thread")
        //        Thread.sleep(3*1000)
      }
    }

    val fis: FSDataInputStream = fs.open(path)

    try {
      f(fis)
    }
    finally {
      fis.close()
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T): T = SpookyUtils.retry(3){
    val path = new Path(pathStr)
//    ensureAbsolute(path)

    val fs = path.getFileSystem(configWrapper.value)

    val fos: FSDataOutputStream = fs.create(path, overwrite)

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

    val path = new Path(pathStr)
//    ensureAbsolute(path)
    val fs: hadoop.fs.FileSystem = path.getFileSystem(configWrapper.value)

    val lockedPath = new Path(pathStr + lockedSuffix)

    SpookyUtils.retry(5) {
      assert(!fs.exists(lockedPath), s"File $pathStr is locked by another executor or thread")
    }

    if (fs.exists(path)) fs.rename(path, lockedPath)

    try {
      val result = f(lockedPath.toString)
      result
    }
    finally {
      fs.rename(lockedPath, path)
      fs.delete(lockedPath, true)
    }
  }

  override def toAbsolute(pathStr: String): String = {
    val path = new Path(pathStr)

    if (path.isAbsolute) {
      if (path.isAbsoluteAndSchemeAuthorityNull)
        "file:" + path.toString
      else
        path.toString
    }
    else {
      val fs = path.getFileSystem(configWrapper.value)
      try {
        val root = fs.getWorkingDirectory.toString.stripSuffix("/")
        root +"/" +pathStr
      }
      finally {
        fs.close()
      }
    }
  }
}