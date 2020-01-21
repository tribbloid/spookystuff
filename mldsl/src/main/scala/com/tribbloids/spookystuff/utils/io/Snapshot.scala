package com.tribbloids.spookystuff.utils.io

import java.util.UUID

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, CommonUtils, NoRetry}

// stage by moving to a secret location
case class Snapshot(
    original: URIResolver#URISession,
    expire: Obsolescence = URIResolver.default.expire,
    override val _lifespan: Lifespan = Lifespan.JVM()
) extends LocalCleanable {

  import Snapshot._

  val resolver: URIResolver = original.outer

  object TempDir {

    lazy val pathStr: String = original.absolutePathStr + SUFFIX

    lazy val session: resolver.URISession = resolver.newSession(pathStr)
  }

  lazy val oldFilePath: String = original.absolutePathStr + OLD_SUFFIX

  object TempFile {

    lazy val createTime: Long = System.currentTimeMillis()

    lazy val pathStr: String = {

      val name = toFileName(createTime)
      val path = CommonUtils./:/(TempDir.pathStr, name)
      path
    }

    lazy val session: resolver.URISession = resolver.newSession(pathStr)

    def toFileName(time: Long): String = {
      val uuid = UUID.randomUUID()
      s"$uuid.T$time"
    }
    def fromFileName(v: String): Option[Long] = {
      val parts = v.split('.')

      if (parts.length != 2) return None

      val ext = parts.last

      if (!ext.startsWith("T")) {
        None
      }

      Some(ext.stripPrefix("T").toLong)
    }
  }

  def during[T](fn: URISession => T): T = {
    resolver.retry {
      duringOnce(fn)
    }
  }

  def duringOnce[T](fn: URISession => T): T = {
    val snapshotSession: LazySnapshotSession = acquireSession()

    val result = try {
      fn(snapshotSession)
    } catch {
      case e: ResourceLockError => throw e
      case e @ _                => throw new NoRetry.BypassedException(e)
    }

    snapshotSession.commitBack() // point of no return

    succeed()

    result
  }

  case class LazySnapshotSession() extends resolver.URISession {

    val originalSignature: IntegritySignature = getIntegritySignature()

    def getIntegritySignature(session: URISession = original): IntegritySignature = {
      session.input { in =>
        if (in.isExisting) Some(IntegritySignature.V(in.getLastModified, in.getLength))
        else None
      }
    }

    lazy val tempSession: resolver.URISession = TempFile.session

    @transient var tempFileInitialized: Boolean = false
    @transient var tempFileModified: Boolean = false

    protected def needOverwrittenFile[T](overwriteFn: () => T): T = {

      if (!tempFileInitialized) {

        if (original.isExisting) {
          //        println(s"empty snapshot: ${delegate.absolutePathStr}")
          tempSession.output(WriteMode.CreateOnly)(_.stream)
        } else {
//          println(s"dir only: ${TempFile.session.absolutePathStr}")
          tempSession.output(WriteMode.CreateOnly)(_.stream)
          tempSession.delete() // just leaving the directory
        }

        val delegateSignature = getIntegritySignature(tempSession)

        try {
          val result = overwriteFn()

          tempFileInitialized = true
          result
        } catch {
          case e: Throwable =>
            if (delegateSignature != getIntegritySignature(tempSession)) {
              // overwrite is partially successful
              tempFileInitialized = true
            } else {
              // overwrite doesn't do a thing
              tempSession.delete(false)
            }
            throw e
        }

      } else {
        overwriteFn()
      }
    }

    // takes a copy
    protected def needOriginalFile(): Unit = {

      if (!tempFileInitialized) {

        if (original.isExisting) {
          //        println(s"real snapshot: ${delegate.absolutePathStr}")
          original.copyTo(tempSession.absolutePathStr, WriteMode.CreateOnly)
        } else {
//          println(s"dir only: ${TempFile.session.absolutePathStr}")
          tempSession.output(WriteMode.CreateOnly)(_.stream)
          tempSession.delete() // just leaving the directory
        }
      }

      checkIntegrity() // otherwise the copy may be obsolete

      tempFileInitialized = true
    }

    // just a file rotation, 2 moves only
    def commitBack(): Unit = {

      if (tempSession.isExisting) {

        if (tempFileModified) {

          checkIntegrity()

          if (original.isExisting)
            original.moveTo(oldFilePath)

          tempSession.moveTo(original.absolutePathStr)
        } else {
          tempSession.delete(false)
        }
      }

    }

    def checkIntegrity(): Unit = {

      val exists = TempDir.session.isExisting

      if (exists) {

        // ensure that all snapshots in it are either obsolete or have a later create time

        // ensure that all snapshots in it are obsolete
        val existingTempFiles = TempDir.session.input(in => in.children)
        val notExpired = expire.filter(existingTempFiles)
        val notExpiredOrSelf = notExpired.filter { session =>
          session.absolutePathStr != TempFile.session.absolutePathStr
        }

        notExpiredOrSelf.foreach { ff =>
          val fileName = ff.absolutePathStr.split('/').last
          val createTime = TempFile.fromFileName(fileName).getOrElse {
            abort(
              s"""
                 |snapshot directory has been corrupted and contains an illegal file:
                 |$fileName
                 |""".stripMargin
            )
          }

          if (createTime <= TempFile.createTime)
            abort(
              s"""
                 |another snapshot file '${ff.absolutePathStr}' has been created,
                 |likely before the current snapshot being created at T${TempFile.createTime}
                 |""".stripMargin
            )
//          else {
//            println("")
//          }
        }
      } else {

        abort(
          s"""
             |snapshot directory has been deleted
             |""".stripMargin
        )
      }

      Thread.sleep(10)
      //WARNING: DO NOT DELETE! this line eliminates race condition between file renaming & both checks

      val updatedSignature = getIntegritySignature()
      if (this.originalSignature != updatedSignature)
        abort(
          s"""
             |file integrity signature has been updated:
             |original : $originalSignature
             |updated  : $updatedSignature
             |""".stripMargin
        )
    }

    override def absolutePathStr: String = tempSession.absolutePathStr

    override protected[io] def _delete(mustExist: Boolean): Unit = {

      needOverwrittenFile { () =>
        tempSession.delete(mustExist)
      }
      tempFileModified = true
    }

    override def moveTo(target: String): Unit = {

      needOriginalFile()

      tempSession.moveTo(target)
      tempFileModified = true
    }

    override def input[T](fn: InputResource => T): T = {

      needOriginalFile()

      tempSession.input(fn)
    }

    override def output[T](mode: WriteMode)(fn: OutputResource => T): T = {

      val result = mode match {
        case WriteMode.Overwrite | WriteMode.CreateOnly =>
          var result: Option[T] = None
          needOverwrittenFile { () =>
            result = Some(tempSession.output(mode)(fn))
          }
          result.get
        case WriteMode.Append =>
          needOriginalFile()

          tempSession.output(mode)(fn)
      }

      tempFileModified = true

      result
    }
  }

  def acquireSession(): LazySnapshotSession = {

    requireNoExistingSnapshot()

    LazySnapshotSession()
  }

  def requireNoExistingSnapshot(): Unit = {

    val exists = TempDir.session.input { in =>
      val result = in.isExisting
      if (result && !in.isDirectory)
        abort(
          s"""
             |snapshot directory should not be a file:
             |${TempDir.session}
             |""".stripMargin
        )
      result
    }

    if (exists) {
      // ensure that all snapshots in it are obsolete
      val existingTempFiles = TempDir.session.input(in => in.children)
      val notExpired = expire.filter(existingTempFiles)

      notExpired.foreach { ff =>
        val elapsedOpt = expire.checkSession(ff)
        elapsedOpt.foreach { v =>
          abort(
            s"another snapshot file '${ff.absolutePathStr}' has existed for ${v.elapsedMillis} milliseconds"
          )
        }
      }
    }
  }

  def succeed(): Unit = {
    TempDir.session.delete()
  }

  def abort(info: String): Nothing = {
    abortSilently()
    throw new ResourceLockError("Abort: " + info.trim)
  }
  def abortSilently(): Unit = {
    TempFile.session.delete(false)
  }
  override protected def cleanImpl(): Unit = {
    abortSilently()
  }
}

object Snapshot {

  val acquired: CachingUtils.ConcurrentMap[URIResolver#URISession, Long] = CachingUtils.ConcurrentMap()

  final val SUFFIX: String = ".lock"

  final val OLD_SUFFIX: String = ".old"

  object IntegritySignature {
    case class V(
        lastModified: Long,
        length: Long
    )
  }

  type IntegritySignature = Option[IntegritySignature.V]

//  case class IntegritySignature(
//      lastModified: Long,
//      length: Long
//  )
}
