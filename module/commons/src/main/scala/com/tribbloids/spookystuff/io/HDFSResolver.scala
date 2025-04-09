package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.util.{PathMagnet, Retry}
import com.tribbloids.spookystuff.commons.data.ReflCanUnapply
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
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

  import Resource.*

  def _hadoopConf: Configuration = {
    hadoopConfFactory()
  }

  override def toString: String = s"${this.getClass.getSimpleName}(${_hadoopConf})"

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

  implicit class _Execution(path: PathMagnet.URIPath) extends Execution {

    val hdfsPath: Path = new Path(path)

    lazy val fc: FileContext = FileContext.getFileContext(hdfsPath.toUri, _hadoopConf)

    override lazy val absolutePath: PathMagnet.URIPath = doAsUGI {

      val defaultURI = fc.getDefaultFileSystem.getUri
      val qualified = hdfsPath.makeQualified(defaultURI, fc.getWorkingDirectory)

      val uri: URI = qualified.toUri
      val newAuthority = Option(uri.getAuthority).getOrElse("")
      val withNewAuthority = new URI(uri.getScheme, newAuthority, uri.getPath, uri.getFragment)

      withNewAuthority.toString
    }

    object _Resource extends (WriteMode => _Resource)
    case class _Resource(mode: WriteMode) extends Resource {

      override protected def _outer: URIExecution = _Execution.this

      lazy val status: FileStatus = fc.getFileStatus(hdfsPath)

      override lazy val getName: String = status.getPath.getName

      override lazy val getType: String = {
        if (status.isDirectory) DIR
        else if (status.isSymlink) SYMLINK
        else if (status.isFile) FILE
        else UNKNOWN
      }

      override def _requireExisting(): Unit = require(fc.util().exists(hdfsPath))

      override lazy val getContentType: String = {
        if (isDirectory) DIR_MIME_OUT
        else null
      }

      override lazy val getLength: Long = status.getLen

      override lazy val getLastModified: Long = status.getModificationTime

      override lazy val extraMetadata: ResourceMetadata = {
        val unapplied = HDFSResolver.unapplyFileStatus.unapply(status)
        ResourceMetadata.BuildFrom.unappliedForm(unapplied)
      }

      override lazy val children: Seq[_Execution] = {
        if (isDirectory) {

          val childrenItr = fc.listStatus(hdfsPath)

          val children = {
            val v = ArrayBuffer.empty[FileStatus]

            while (childrenItr.hasNext) {
              v += childrenItr.next()
            }

            v
          }

          children.map { status =>
            on(status.getPath.toString)
          }
        }.toSeq
        else Nil
      }

      override protected def _newIStream: InputStream = {
        fc.open(hdfsPath)
      }

      override protected def _newOStream: OutputStream = {
        import CreateFlag.*

        mkParent(hdfsPath)

        val result = mode match {
          case WriteMode.ErrorIfExists => fc.create(hdfsPath, util.EnumSet.of(CREATE))
          case WriteMode.Append     => fc.create(hdfsPath, util.EnumSet.of(CREATE, APPEND))
          case WriteMode.Overwrite  => fc.create(hdfsPath, util.EnumSet.of(CREATE, OVERWRITE))
        }

        result
      }
    }

    override def doIO[T](mode: WriteMode)(fn: _Resource => T): T = doAsUGI {
      super.doIO(mode)(fn)
    }

    def _delete(mustExist: Boolean = true): Unit = doAsUGI {

      (isExisting, mustExist) match {
        case (false, false) =>
        case _              => fc.delete(hdfsPath, true)
      }
    }

    override def moveTo(target: String, force: Boolean = false): Unit = {

      val newPath = new Path(target)

      mkParent(newPath)

      if (force)
        fc.rename(hdfsPath, newPath, Options.Rename.OVERWRITE)
      else
        fc.rename(hdfsPath, newPath)
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

  val unapplyFileStatus: ReflCanUnapply[FileStatus] = ReflCanUnapply[FileStatus]()

  //  def serviceUGIFactory = () => Some(SparkHelper.serviceUGI)
  //
  //  val lazyService = SparkHelper.serviceUGI
  //
  //  def lazyServiceUGIFactory = () => Some(lazyService)
}
