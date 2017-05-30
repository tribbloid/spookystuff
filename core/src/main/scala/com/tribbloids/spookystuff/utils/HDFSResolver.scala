package com.tribbloids.spookystuff.utils

import java.io.{InputStream, OutputStream}
import java.security.PrivilegedActionException

import com.tribbloids.spookystuff.Const
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.util.SparkHelper

/**
  * Created by peng on 17/05/17.
  */
case class HDFSResolver(
                         @transient hadoopConf: Configuration,
                         ugiFactory: () => Option[UserGroupInformation] = HDFSResolver.noUGIFactory
                       ) extends PathResolver {

  import SpookyViews._

  def lockedSuffix: String = ".locked"

  val confBinary = new BinaryWritable(hadoopConf)

  def getHadoopConf: Configuration = {
    confBinary.value
  }

  override def toString = s"${this.getClass.getSimpleName}($getHadoopConf)"

  def ensureAbsolute(path: Path): Unit = {
    assert(path.isAbsolute, s"BAD DESIGN: ${path.toString} is not an absolute path")
  }

  @transient lazy val ugiOpt = ugiFactory()

  protected def doAsUGI[T](f: =>T): T = {
    ugiOpt match {
      case None =>
        f
      case Some(ugi) =>
        try {
          ugi.doAs {
            f
          }
        }
        catch {
          case e: Throwable =>
            // UGI.doAs wraps any exception in PrivilegedActionException, should be unwrapped and thrown
            throw SpookyUtils.unboxException[PrivilegedActionException](e)
        }
    }
  }

  def input[T](pathStr: String)(f: InputStream => T): T = {
    val path: Path = new Path(pathStr)
    val fs: FileSystem = path.getFileSystem(getHadoopConf)

    doAsUGI {
      if (!pathStr.endsWith(lockedSuffix)) {
        //wait for its locked file to finish its locked session

        val lockedPath = new Path(pathStr + lockedSuffix)

        fs.getStatus(path)

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
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: (OutputStream) => T): T = {
    val path = new Path(pathStr)
    val fs = path.getFileSystem(getHadoopConf)

    doAsUGI {
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

object HDFSResolver {

  def serviceUGIFactory = () => Some(SparkHelper.serviceUGI)

  def noUGIFactory = () => None
}