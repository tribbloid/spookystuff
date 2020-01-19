package com.tribbloids.spookystuff.utils.io

import java.util.UUID

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, CommonUtils, NoRetry}

import scala.concurrent.duration.Duration

// stage by moving to a secret location
case class Snapshot(
    original: URIResolver#URISession,
    lockExpireAfter: Duration = URIResolver.defaultLockExpireAfter,
    override val _lifespan: Lifespan = Lifespan.JVM()
) extends LocalCleanable {

  import Snapshot._

  val resolver: URIResolver = original.outer

  object TempDir {

    lazy val pathStr: String = original.absolutePathStr + SUFFIX

    lazy val session: resolver.URISession = resolver.newSession(pathStr)
  }

  lazy val oldFilePath = original.absolutePathStr + OLD_SUFFIX

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

    snapshotSession.commitBack()

    conclude()

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

    lazy val delegate: resolver.URISession = TempFile.session

    @transient var tempFileInitialized: Boolean = false
    @transient var tempFileModified: Boolean = false

    protected def needOverwrittenFile[T](overwriteFn: () => T): T = {

      if (!tempFileInitialized) {

        if (original.isExisting) {
          //        println(s"empty snapshot: ${delegate.absolutePathStr}")
          delegate.output(WriteMode.CreateOnly)(_.stream)
        } else {
//          println(s"dir only: ${TempFile.session.absolutePathStr}")
          delegate.output(WriteMode.CreateOnly)(_.stream)
          delegate.delete() // just leaving the directory
        }

        val delegateSignature = getIntegritySignature(delegate)

        try {
          val result = overwriteFn()

          tempFileInitialized = true
          result
        } catch {
          case e: Throwable =>
            if (delegateSignature != getIntegritySignature(delegate)) {
              // overwrite is partially successful
              tempFileInitialized = true
            } else {
              // overwrite doesn't do a thing
              delegate.delete(false)
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
          original.copyTo(delegate.absolutePathStr, WriteMode.CreateOnly)
        } else {
//          println(s"dir only: ${TempFile.session.absolutePathStr}")
          delegate.output(WriteMode.CreateOnly)(_.stream)
          delegate.delete() // just leaving the directory
        }
      }

      validateIntegrity() // otherwise the copy may be obsolete

      tempFileInitialized = true
    }

    // just a file rotation, 2 mv only
    def commitBack(): Unit = {

      if (delegate.isExisting) {

        if (tempFileModified) {

          validateIntegrity()

          if (original.isExisting)
            original.moveTo(oldFilePath)

          delegate.moveTo(original.absolutePathStr)
        } else {
          delegate.delete(false)
        }
      }

    }

    def validateIntegrity(): Unit = {

      val updatedSignature = getIntegritySignature()
      if (this.originalSignature != updatedSignature)
        abort(
          s"""
             |file integrity signature has been updated:
             |original : $originalSignature
             |updated  : $updatedSignature
             |""".stripMargin
        )

      val exists = TempDir.session.isExisting

      if (exists) {

        // ensure that all snapshots in it are either obsolete or have a later create time

        val existingTempFiles = TempDir.session.input(in => in.children)
        val existingNotSelf = existingTempFiles.filter { session =>
          session.absolutePathStr != TempFile.session.absolutePathStr
        }
        existingNotSelf.foreach { ff =>
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
        }
      } else {

        abort(
          s"""
             |snapshot directory has been deleted
             |""".stripMargin
        )
      }
    }

    override def absolutePathStr: String = delegate.absolutePathStr

    override protected[io] def _delete(mustExist: Boolean): Unit = {

      needOverwrittenFile { () =>
        delegate.delete(mustExist)
      }
      tempFileModified = true
    }

    override def moveTo(target: String): Unit = {

      needOriginalFile()

      delegate.moveTo(target)
      tempFileModified = true
    }

    override def input[T](fn: InputResource => T): T = {

      needOriginalFile()

      delegate.input(fn)
    }

    override def output[T](mode: WriteMode)(fn: OutputResource => T): T = {

      val result = mode match {
        case WriteMode.Overwrite | WriteMode.CreateOnly =>
          var result: Option[T] = None
          needOverwrittenFile { () =>
            result = Some(delegate.output(mode)(fn))
          }
          result.get
        case WriteMode.Append =>
          needOriginalFile()

          delegate.output(mode)(fn)
      }

      tempFileModified = true

      result
    }
  }

  def acquireSession(): LazySnapshotSession = {

    requireNoExistingSnapshot()

    // yield SnapshotSession
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
      existingTempFiles.foreach { ff =>
        val lastModified = ff.input(in => in.getLastModified)

        val elapsed = System.currentTimeMillis() - lastModified

        if (elapsed >= lockExpireAfter.toMillis) {
          // expired, safe to delete
          ff.delete(false)
        } else {
          abort(
            s"another snapshot file '${ff.absolutePathStr}' has existed for $elapsed milliseconds"
          )
        }
      }
    }
  }

  def conclude(): Unit = {
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
