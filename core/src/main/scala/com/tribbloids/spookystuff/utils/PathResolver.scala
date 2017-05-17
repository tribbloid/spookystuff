package com.tribbloids.spookystuff.utils

import java.io._
import java.nio.file.FileAlreadyExistsException
import java.security.PrivilegedActionException

import com.tribbloids.spookystuff.Const
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.spark.deploy.SparkHadoopUtil

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
    val resourcePath = SpookyUtils.getCPResource(pathStr.stripPrefix("/")).map(_.getPath).getOrElse(pathStr)

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
      SpookyUtils.retry(Const.DFSBlockedAccessRetries) {
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

    SpookyUtils.retry(Const.DFSBlockedAccessRetries) {
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

object HDFSResolver {

  def sparkDoAsOpt: Option[(() => Unit) => Unit] = Some {
    fn =>
      SparkHadoopUtil.get.runAsSparkUser(fn)
  }

  def defaultDoAsOpt: Option[(() => Unit) => Unit] = None
}

case class HDFSResolver(
                         @transient hadoopConf: Configuration,
                         doAsOpt: Option[(() => Unit) => Unit] = HDFSResolver.defaultDoAsOpt
                       ) extends PathResolver {

  def lockedSuffix: String = ".locked"

  val confBinary = new BinaryWritable(hadoopConf)

  def getHadoopConf: Configuration = {
    confBinary.value
  }

  override def toString = s"${this.getClass.getSimpleName}($getHadoopConf)"

  def ensureAbsolute(path: Path): Unit = {
    assert(path.isAbsolute, s"BAD DESIGN: ${path.toString} is not an absolute path")
  }

  protected def doAsUGI[T](f: =>T): T = {
    doAsOpt match {
      case None =>
        f
      case Some(doAs) =>
        try {
          @transient var result: AnyRef = null
          doAs {
            () =>
              result = f.asInstanceOf[AnyRef]
          }
          result.asInstanceOf[T]
        }
        catch {
          case e: Throwable =>
            // UGI.doAs wraps any exception in PrivilegedActionException, should be unwrapped and thrown
            throw SpookyUtils.unboxException[PrivilegedActionException](e)
        }
    }
  }

  def input[T](pathStr: String)(f: InputStream => T): T = doAsUGI{
    val path: Path = new Path(pathStr)
    //    ensureAbsolute(path)

    val fs = path.getFileSystem(getHadoopConf)

    if (!pathStr.endsWith(lockedSuffix)) {
      //wait for its locked file to finish its locked session

      val lockedPath = new Path(pathStr + lockedSuffix)

      //wait for 15 seconds in total
      SpookyUtils.retry(Const.DFSBlockedAccessRetries) {
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

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T): T = doAsUGI{
    val path = new Path(pathStr)

    val fs = path.getFileSystem(getHadoopConf)

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

  override def lockAccessDuring[T](pathStr: String)(f: (String) => T): T = doAsUGI{

    val path = new Path(pathStr)
    //    ensureAbsolute(path)
    val fs: hadoop.fs.FileSystem = path.getFileSystem(getHadoopConf)

    val lockedPath = new Path(pathStr + lockedSuffix)

    SpookyUtils.retry(Const.DFSBlockedAccessRetries, 1000) {
      assert(
        !fs.exists(lockedPath),
        {
          Thread.sleep(1000) //fs.exists is really fast, avoid flooding the fs
          s"File $pathStr is locked by another executor or thread"
        }
      )
    }

    val fileExists = fs.exists(path)
    if (fileExists) {
      fs.rename(path, lockedPath)
      SpookyUtils.retry(Const.DFSBlockedAccessRetries) {
        assert(fs.exists(lockedPath), s"Locking of $pathStr cannot be persisted")
      }
    }

    try {
      val result = f(lockedPath.toString)
      result
    }
    finally {
      if (fileExists) {
        fs.rename(lockedPath, path)
        fs.delete(lockedPath, true)
      }
    }
  }

  override def toAbsolute(pathStr: String): String = doAsUGI{
    val path = new Path(pathStr)

    if (path.isAbsolute) {
      if (path.isAbsoluteAndSchemeAuthorityNull)
        "file:" + path.toString
      else
        path.toString
    }
    else {
      val fs = path.getFileSystem(getHadoopConf)
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