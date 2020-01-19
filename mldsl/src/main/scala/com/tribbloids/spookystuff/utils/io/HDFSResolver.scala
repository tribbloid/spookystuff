package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.security.{PrivilegedAction, PrivilegedActionException}

import com.tribbloids.spookystuff.utils.serialization.SerDeOverride
import com.tribbloids.spookystuff.utils.{CommonUtils, Retry}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.ml.dsl.utils.LazyVar

/**
  * Created by peng on 17/05/17.
  */
case class HDFSResolver(
    hadoopConf: SerDeOverride[Configuration],
    ugiFactory: () => Option[UserGroupInformation] = HDFSResolver.noUGIFactory,
    override val retry: Retry = URIResolver.defaultRetry
) extends URIResolver {

  import Resource._

  def _hadoopConf: Configuration = {
    hadoopConf.value
  }

  override def toString = s"${this.getClass.getSimpleName}(${_hadoopConf})"

  def ugiOpt: Option[UserGroupInformation] = ugiFactory()

  protected def doAsUGI[T](f: => T): T = {
    ugiOpt match {
      case None =>
        f
      case Some(ugi) if ugi == UserGroupInformation.getCurrentUser =>
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
        } catch {
          case e: Throwable =>
            // UGI.doAs wraps any exception in PrivilegedActionException, should be unwrapped and thrown
            throw CommonUtils.unboxException[PrivilegedActionException](e)
        }
    }
  }

  override def newSession(pathStr: String): Session = Session(new Path(pathStr))
  case class Session(path: Path) extends super.URISession {

    lazy val fs: LazyVar[FileSystem] = LazyVar {
      path.getFileSystem(_hadoopConf) //DON'T close! shared by all in the process
    }

    override lazy val absolutePathStr: String = doAsUGI {

      if (path.isAbsolute) {
        if (path.isAbsoluteAndSchemeAuthorityNull)
          "file:" + path.toString
        else
          path.toString
      } else {
        val root = fs.getWorkingDirectory.toString.stripSuffix("/")
        val combined = new Path(root + "/" + path.toString)

        combined.toString
      }
    }

    trait HDFSResource[T] extends Resource[T] {

      lazy val status: FileStatus = fs.getFileStatus(path)

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = status.getPath.getName

      override lazy val getType: String = {
        if (status.isDirectory) DIR
        else if (status.isFile) "file"
        else if (status.isSymlink) "symlink"
        else UNKNOWN
      }

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME
        else null
      }

      override lazy val getLength: Long = status.getLen

      override lazy val getLastModified: Long = status.getModificationTime

      override lazy val isExisting: Boolean = fs.exists(path)

      override lazy val _metadata: ResourceMetadata = HDFSResolver.mdParser.apply(status)

      override lazy val children: Seq[URISession] = {
        if (isDirectory) {

          val children = fs.listStatus(path).toSeq

          children.map { status =>
            Session(status.getPath)
          }
        } else Nil
      }
    }

    //TODO: retry CRC errors on read
    def input[T](fn: InputResource => T): T = doAsUGI {

      val ir = new InputResource with HDFSResource[InputStream] {

        override def createStream: InputStream = {

          fs.open(path)
        }
      }
      try {
        fn(ir)
      } finally {
        ir.clean()
      }
    }

    override def output[T](mode: WriteMode)(fn: OutputResource => T): T = doAsUGI {

      val or = new OutputResource with HDFSResource[OutputStream] {

        override def createStream: OutputStream = {
          val result = mode match {
            case WriteMode.CreateOnly => fs.create(path, false)
            case WriteMode.Append     => fs.append(path)
            case WriteMode.Overwrite  => fs.create(path, true)
          }

          result
        }
      }

      try {
        val result = fn(or)
        result
      } finally {
        or.clean()
      }
    }

    def _delete(mustExist: Boolean = true): Unit = doAsUGI {

      fs.delete(path, true)
    }

    override def moveTo(target: String): Unit = {

      val newPath = new Path(target)

      fs.rename(path, newPath)
    }

//    override def mkDirs(): Unit = {
//
//      fs.mkdirs(path)
//    }

    override def cleanImpl(): Unit = {
      fs.peek.foreach(_.close())
    }
  }
}

object HDFSResolver {

  def noUGIFactory: () => None.type = () => None

  val mdParser: ResourceMetadata.ReflectionParser[FileStatus] = ResourceMetadata.ReflectionParser[FileStatus]()

  //  def serviceUGIFactory = () => Some(SparkHelper.serviceUGI)
  //
  //  val lazyService = SparkHelper.serviceUGI
  //
  //  def lazyServiceUGIFactory = () => Some(lazyService)
}
