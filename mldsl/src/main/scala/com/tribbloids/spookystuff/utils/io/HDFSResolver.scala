package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.Retry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.security.PrivilegedAction
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
//        try {
        ugi.doAs {
          new PrivilegedAction[T] {
            override def run(): T = {
              f
            }
          }
        }
//        } catch {
//          case e: Throwable =>
//            // UGI.doAs wraps any exception in PrivilegedActionException, should be unwrapped and thrown
//            throw CommonUtils.unboxException[PrivilegedActionException](e)
//        }
    }
  }

  case class _Execution(pathStr: String) extends Execution {

    val path = new Path(pathStr)

    lazy val fc: FileContext = FileContext.getFileContext(path.toUri, _hadoopConf)

    override lazy val absolutePathStr: String = doAsUGI {

      val defaultURI = fc.getDefaultFileSystem.getUri
      val qualified = path.makeQualified(defaultURI, fc.getWorkingDirectory)

      val uri: URI = qualified.toUri
      val newAuthority = Option(uri.getAuthority).getOrElse("")
      val withNewAuthority = new URI(uri.getScheme, newAuthority, uri.getPath, uri.getFragment)

      withNewAuthority.toString
    }

    case class _Resource(mode: WriteMode) extends Resource {

      lazy val status: FileStatus = fc.getFileStatus(path)

      override lazy val getURI: String = absolutePathStr

      override lazy val getName: String = status.getPath.getName

      override lazy val getType: String = {
        if (status.isDirectory) DIR
        else if (status.isSymlink) SYMLINK
        else if (status.isFile) FILE
        else UNKNOWN
      }

      override def isExisting: Boolean = fc.util().exists(path)

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME_OUT
        else null
      }

      override lazy val getLength: Long = status.getLen

      override lazy val getLastModified: Long = status.getModificationTime

      override lazy val _metadata: ResourceMetadata = {
        HDFSResolver.metadataParser.apply(status)
      }

      override lazy val children: Seq[_Execution] = {
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

      override protected def _newIStream: InputStream = {
        fc.open(path)
      }

      override protected def _newOStream: OutputStream = {
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

    override def inputNoValidation[T](fn: _Resource#InputView => T): T = doAsUGI {
      super.inputNoValidation(fn)
    }

    override def output[T](mode: WriteMode)(fn: _Resource#OutputView => T): T = doAsUGI {
      super.output(mode)(fn)
    }

    //TODO: retry CRC errors on read
//    override def inputNoValidation[T](fn: IResource => T): T = doAsUGI {
//
//      val ir = new IResource {
//
//        override def _newIStream: InputStream = {
//
//          fc.open(path)
//        }
//      }
//      try {
//        fn(ir)
//      } finally {
//        ir.clean()
//      }
//    }
//
//    override def output[T](mode: WriteMode)(fn: OResource => T): T = doAsUGI {
//
//      val or = new OResource {
//
//        override def _newIStream: OutputStream = {
//
//          import CreateFlag._
//
//          mkParent(path)
//
//          val result = mode match {
//            case WriteMode.CreateOnly => fc.create(path, util.EnumSet.of(CREATE))
//            case WriteMode.Append     => fc.create(path, util.EnumSet.of(CREATE, APPEND))
//            case WriteMode.Overwrite  => fc.create(path, util.EnumSet.of(CREATE, OVERWRITE))
//          }
//
//          result
//        }
//      }
//
//      try {
//        val result = fn(or)
//        result
//      } finally {
//        or.clean()
//      }
//    }

    def _delete(mustExist: Boolean = true): Unit = doAsUGI {

      (isExisting, mustExist) match {
        case (false, false) =>
        case _              => fc.delete(path, true)
      }
    }

    override def moveTo(target: String, force: Boolean = false): Unit = {

      val newPath = new Path(target)

      mkParent(newPath)

      if (force)
        fc.rename(path, newPath, Options.Rename.OVERWRITE)
      else
        fc.rename(path, newPath)
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

  val metadataParser: ResourceMetadata.ReflectionParser[FileStatus] = ResourceMetadata.ReflectionParser[FileStatus]()

  //  def serviceUGIFactory = () => Some(SparkHelper.serviceUGI)
  //
  //  val lazyService = SparkHelper.serviceUGI
  //
  //  def lazyServiceUGIFactory = () => Some(lazyService)
}
