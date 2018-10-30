package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.security.{PrivilegedAction, PrivilegedActionException}

import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.serialization.SerBox
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation

/**
  * Created by peng on 17/05/17.
  */
case class HDFSResolver(
    hadoopConf: SerBox[Configuration],
    ugiFactory: () => Option[UserGroupInformation] = HDFSResolver.noUGIFactory
) extends URIResolver {

  import Resource._

  override lazy val unlockForInput: Boolean = true

  def _hadoopConf: Configuration = {
    hadoopConf.value
  }

  override def toString = s"${this.getClass.getSimpleName}(${_hadoopConf})"

  def ugiOpt = ugiFactory()

  protected def doAsUGI[T](f: => T): T = {
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
        } catch {
          case e: Throwable =>
            // UGI.doAs wraps any exception in PrivilegedActionException, should be unwrapped and thrown
            throw CommonUtils.unboxException[PrivilegedActionException](e)
        }
    }
  }

  override def Execution(pathStr: String) = Execution(new Path(pathStr))
  case class Execution(path: Path) extends super.Execution {

    lazy val fs: FileSystem = path.getFileSystem(_hadoopConf) //DON'T close! shared by all in the process

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

      lazy val status = fs.getFileStatus(path)

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

      override lazy val getLenth: Long = status.getLen

      override lazy val getLastModified: Long = status.getModificationTime

      override lazy val isAlreadyExisting: Boolean = fs.exists(path)

      override lazy val _metadata = HDFSResolver.mdParser.apply(status)

      override lazy val children: Seq[ResourceMD] = {
        if (isDirectory) {

          val children = fs.listStatus(path).toSeq

          children.map { status =>
            val childExecution = Execution(status.getPath)
            childExecution.input(_.rootMetadata)
          }
        } else Nil
      }
    }

    //TODO: retry CRC errors on read
    def input[T](f: InputResource => T): T = doAsUGI {

      val ir = new InputResource with HDFSResource[InputStream] {

        override def _stream: InputStream = {

          //          if (!absolutePathStr.endsWith(lockedSuffix)) {
          //            //wait for its locked file to finish its locked session
          //
          //            val lockedPath = new Path(pathStr + lockedSuffix)
          //
          //            assertUnlocked(fs, lockedPath)
          //          }

          fs.open(path)
        }
      }
      try {
        f(ir)
      } finally {
        ir.clean()
      }
    }

    def _remove(mustExist: Boolean = true): Unit = doAsUGI {
      fs.delete(path, true)
    }

    override def output[T](overwrite: Boolean)(f: OutputResource => T): T = doAsUGI {

      val or = new OutputResource with HDFSResource[OutputStream] {
        override def _stream: OutputStream = {
          fs.create(path, overwrite)
        }
      }

      try {
        val result = f(or)
        result
      } finally {
        or.clean()
      }
    }
  }

  //  override def lockAccessDuring[T](pathStr: String)(f: String => T): T = doAsUGI{
  //    ???
  //    val path = new Path(pathStr)
  //    val lockedPath = new Path(pathStr + lockedSuffix)
  //    //    ensureAbsolute(path)
  //    val fs: FileSystem = path.getFileSystem(getHadoopConf)
  //
  //    val isNewFile = if (fs.exists(path)) {
  //      fs.rename(path, lockedPath)
  //      false
  //    }
  //    else {
  //      fs.createNewFile(lockedPath)
  //      true
  //    }
  //
  //
  //    assertUnlocked(fs, lockedPath)
  //
  //
  //
  //    try {
  //      val result = f(lockedPath.toString)
  //      result
  //    }
  //    finally {
  //      if (fs.exists(lockedPath)) {
  //        fs.rename(lockedPath, path)
  //        fs.delete(lockedPath, true) //TODO: this line is useless?
  //      }
  //    }
  //  }
}

object HDFSResolver {

  def noUGIFactory: () => None.type = () => None

  val mdParser = ResourceMD.ReflectionParser[FileStatus]()

  //  def serviceUGIFactory = () => Some(SparkHelper.serviceUGI)
  //
  //  val lazyService = SparkHelper.serviceUGI
  //
  //  def lazyServiceUGIFactory = () => Some(lazyService)
}
