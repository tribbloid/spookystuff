package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.{CommonUtils, Retry}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation

import java.io.{InputStream, OutputStream}
import java.security.{PrivilegedAction, PrivilegedActionException}
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

/**
  * Created by peng on 17/05/17.
  */
case class HDFSResolver(
    hadoopConfFactory: () => Configuration,
    ugiFactory: () => Option[UserGroupInformation] = HDFSResolver.noUGIFactory,
    override val retry: Retry = URIResolver.default.retry
) extends URIResolver {

  import Resource._

  def _hadoopConf: Configuration = {
    hadoopConfFactory()
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

  override def newExecution(pathStr: String): Execution = Execution(new Path(pathStr))
  case class Execution(path: Path) extends super.AbstractExecution {

    lazy val fc: FileContext = FileContext.getFileContext(path.toUri, _hadoopConf)

    override lazy val absolutePathStr: String = doAsUGI {

      if (path.isAbsolute) {
        if (path.isAbsoluteAndSchemeAuthorityNull)
          "file:" + path.toString
        else
          path.toString
      } else {
        val root = fc.getWorkingDirectory.toString.stripSuffix("/")
        val combined = new Path(root + "/" + path.toString)

        combined.toString
      }
    }

    trait HDFSResource[T] extends Resource[T] {

      def status: FileStatus = fc.getFileStatus(path)

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = status.getPath.getName

      override lazy val getType: String = {
        if (status.isDirectory) DIR
        else if (status.isFile) FILE
        else if (status.isSymlink) SYMLINK
        else UNKNOWN
      }

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME_OUT
        else null
      }

      override lazy val getLength: Long = status.getLen

      override lazy val getLastModified: Long = status.getModificationTime

      override lazy val _metadata: ResourceMetadata = {
        HDFSResolver.mdParser.apply(status)
      }

      override lazy val children: Seq[Execution] = {
        if (isDirectory) {

          val childrenItr = fc.listStatus(path)

          val children = {
            val v = ArrayBuffer.empty[FileStatus]

            while (childrenItr.hasNext) {
              v += childrenItr.next()
            }

            v
          }

          children.map { status =>
            execute(status.getPath.toString)
          }
        } else Nil
      }
    }

    //TODO: retry CRC errors on read
    def input[T](fn: InputResource => T): T = doAsUGI {

      val ir = new InputResource with HDFSResource[InputStream] {

        override def createStream: InputStream = {

          fc.open(path)
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

          import CreateFlag._

          mkParent(path)

          val result = mode match {
            case WriteMode.CreateOnly => fc.create(path, util.EnumSet.of(CREATE))
            case WriteMode.Append     => fc.create(path, util.EnumSet.of(CREATE, APPEND))
            case WriteMode.Overwrite  => fc.create(path, util.EnumSet.of(CREATE, OVERWRITE))
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

      fc.delete(path, true)
    }

    override def moveTo(target: String): Unit = {

      val newPath = new Path(target)

      mkParent(newPath)

      fc.rename(path, newPath, Options.Rename.NONE)
    }

    protected def mkParent(path: Path): Unit = {
      val newParent = path.getParent

      val tryStatus = Try {
        fc.getFileStatus(newParent)
      }

      tryStatus match {
        case Success(status) if status.isDirectory =>
        // Do nothing
        case _ =>
          fc.mkdir(newParent, FsPermission.getDirDefault, true)
      }
    }

//    override def mkDirs(): Unit = {
//
//      fs.mkdirs(path)
//    }

//    override def cleanImpl(): Unit = {
//      fs.peek.foreach(_.close())
//    }
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
