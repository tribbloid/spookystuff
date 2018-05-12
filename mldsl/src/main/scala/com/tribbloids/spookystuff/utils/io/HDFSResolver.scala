package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.security.{PrivilegedAction, PrivilegedActionException}

import com.tribbloids.spookystuff.utils.{CommonUtils, RetryExponentialBackoff, SerBox}
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.ml.dsl.utils.metadata.MetadataMap

import scala.collection.immutable.ListMap

/**
  * Created by peng on 17/05/17.
  */
case class HDFSResolver(
                         hadoopConf: SerBox[Configuration],
                         ugiFactory: () => Option[UserGroupInformation] = HDFSResolver.noUGIFactory
                       ) extends URIResolver {

  final def lockedSuffix: String = ".locked"

  def getHadoopConf: Configuration = {
    hadoopConf.value
  }

  override def toString = s"${this.getClass.getSimpleName}($getHadoopConf)"

  def ensureAbsolute(path: Path): Unit = {
    assert(path.isAbsolute, s"BAD DESIGN: ${path.toString} is not an absolute path")
  }

  def ugiOpt = ugiFactory()

  protected def doAsUGI[T](f: =>T): T = {
    ugiOpt match {
      case None =>
        f
      case Some(ugi) =>
        try {
          ugi.doAs {
            new PrivilegedAction[T] {
              override def run(): T = {
                f
              }
            }
          }
        }
        catch {
          case e: Throwable =>
            // UGI.doAs wraps any exception in PrivilegedActionException, should be unwrapped and thrown
            throw CommonUtils.unboxException[PrivilegedActionException](e)
        }
    }
  }

  def input[T](pathStr: String)(f: InputStream => T): Resource[T] = {
    val path: Path = new Path(pathStr)
    val fs: FileSystem = path.getFileSystem(getHadoopConf)

    new Resource[T] {

      override lazy val value: T = doAsUGI {

        if (!pathStr.endsWith(lockedSuffix)) {
          //wait for its locked file to finish its locked session

          val lockedPath = new Path(pathStr + lockedSuffix)

          //wait for 15 seconds in total
          RetryExponentialBackoff(4, 8000) {
            assert(!fs.exists(lockedPath),
              s"File $pathStr is locked by another executor or thread")
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

      override lazy val metadata = describe(path)
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean
                        )(f: OutputStream => T) = doAsUGI {
    val path = new Path(pathStr)
    val fs = path.getFileSystem(getHadoopConf)

    new Resource[T] {

      override val value: T = {

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

      override lazy val metadata = describe(path)
    }
  }

  override def lockAccessDuring[T](pathStr: String)(f: String => T): T = doAsUGI{

    val path = new Path(pathStr)
    //    ensureAbsolute(path)
    val fs: hadoop.fs.FileSystem = path.getFileSystem(getHadoopConf)

    val lockedPath = new Path(pathStr + lockedSuffix)

    RetryExponentialBackoff(4, 8000) {
      assert(
        //TODO: add expiration impl
        //TODO: retry CRC errors on read
        !fs.exists(lockedPath),
        s"File $pathStr is locked by another executor or thread")
    }

    if (fs.exists(path)) {
      fs.rename(path, lockedPath)

      RetryExponentialBackoff(4, 8000) {
        assert(
          fs.exists(lockedPath),
          s"Locking of $pathStr cannot be persisted")
      }
    }

    try {
      val result = f(lockedPath.toString)
      result
    }
    finally {
      if (fs.exists(lockedPath)) {
        fs.rename(lockedPath, path)
        fs.delete(lockedPath, true) //TODO: this line is useless?
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

  def describe(path: Path): ResourceMD = doAsUGI {
    import Resource._

    val fs: FileSystem = path.getFileSystem(hadoopConf.value)

    def getMap(status: FileStatus): ListMap[String, Any] = {
      var map = reflectiveMetadata(status)
      map ++= MetadataMap(
        URI_ -> toAbsolute(status.getPath.toUri.toString),
        NAME -> status.getPath.getName
      )

      if (status.isDirectory)
        map ++= MetadataMap(CONTENT_TYPE -> URIResolver.DIR_TYPE)

      map
    }

    val status = fs.getFileStatus(path)
    val root = getMap(status)

    if (status.isDirectory) {
      val children = fs.listStatus(path)

      val groupedChildren: Map[String, Seq[Map[String, Any]]] = {

        val withType = children.map {
          child =>
            val tpe = if (child.isDirectory) "directory"
            else if (child.isSymlink) "symlink"
            else if (child.isFile) "file"
            else "others"

            tpe -> child
        }

        withType.groupBy(_._1).mapValues {
          array =>
            array.toSeq.map(kv => getMap(kv._2))
        }
      }

      val map = root ++ groupedChildren
      val result = ResourceMD(map)
      val x = result.message
      result
    }
    else{
      root
    }
  }
}

object HDFSResolver {

  def noUGIFactory = () => None

  //  def serviceUGIFactory = () => Some(SparkHelper.serviceUGI)
  //
  //  val lazyService = SparkHelper.serviceUGI
  //
  //  def lazyServiceUGIFactory = () => Some(lazyService)
}