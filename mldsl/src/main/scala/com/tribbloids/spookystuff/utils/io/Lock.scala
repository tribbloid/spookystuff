package com.tribbloids.spookystuff.utils.io

import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, CommonUtils}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

class Lock(
    execution: URIResolver#URISession,
    expire: Obsolescence = URIResolver.default.expire,
    override val _lifespan: Lifespan = Lifespan.JVM()
) extends LocalCleanable {

  import Lock._

  @volatile var acquiredTimestamp: Long = -1

  override def cleanableLogFunction(logger: Logger): String => Unit = logger.info

  val resolver: URIResolver = execution.outer

  case class ByLockFile(session: resolver.URISession) {

    //  @tailrec
    final def assertUnlockedOnce(): Unit = {

      def existingLockIsExpired(r: Resource[_], e: FileAlreadyExistsException): Boolean = {
        val lockedTime = r.getLastModified
        val lockedDuration = System.currentTimeMillis() - lockedTime

        def errorInfo =
          s"Lock '$lockPathStr' is acquired by another executor or thread for $lockedDuration milliseconds"

        if (lockedDuration >= expire.ignoreAfter.toMillis) {
          LoggerFactory.getLogger(this.getClass).error(errorInfo + " and has expired")
          true
        } else {
          throw new ResourceLockError(errorInfo, e)
        }
      }

      def handleExistingLock(r: Resource[_], e: FileAlreadyExistsException): Unit = {
        val lockExpired = existingLockIsExpired(r, e)

        if (lockExpired) {
          drop()
          assertUnlockedOnce()
        }
      }

      session.output(mode = WriteMode.CreateOnly) { out =>
        //        if (out.isAlreadyExisting) {
        //
        //          this.logPrefixed("=== CANNOT ACCESS")
        //          handleExistingLock(out, null)
        //        } else {

//        if (acquire) {
//          try {
//            out.stream
//            acquiredTimestamp = out.getLastModified
//
//          } catch {
//            case e: FileAlreadyExistsException =>
//              handleExistingLock(out, e)
//          }
//        } else {

        if (out.isExisting) {

          handleExistingLock(out, null)
        }
//        }
        //        }
      }

    }

    def drop(): Unit = {

      session.delete(false)
    }

    def release(): Unit = {

      try {
        if (acquiredTimestamp == -1) {
          //DO NOT CHECK, lock failed
        } else {
          session.input { in =>
            // lock acquired or deleted by others, release will interfere with others
            if (!in.isExisting)
              throw new ResourceLockError(
                s"$logPrefix: lock file ${session.absolutePathStr} has been deleted"
              )

            val lastModified = in.getLastModified
            if (lastModified != acquiredTimestamp)
              throw new ResourceLockError(
                s"$logPrefix: lock file ${session.absolutePathStr} has been tampered and has a different timestamp: " +
                  s"expected $acquiredTimestamp, actual $lastModified"
              )
          }
        }
      } finally {

        drop()

        logRelease(session)
      }
    }
  }

  lazy val lockPathStr: String = execution.absolutePathStr + SUFFIX
  def byLockFile: ByLockFile = ByLockFile(resolver.newSession(lockPathStr))

  case class ByMoving(session: resolver.URISession) {

    def acquireByMv(): String = {

      resolver.retry {
        acquireByMvOnce()
      }
    }

    def acquireByMvOnce(): String = {

      val oldSize = execution.input { in =>
        in.getLength
      }

      execution.moveTo(session.absolutePathStr)

      val newSize = session.input { in =>
        in.getLength
      }

      assert(newSize == oldSize, s"cannot acquire, copy failed: newSize: $newSize oldSize: $oldSize")

      logAcquire(session)

      session.absolutePathStr
    }

    def releaseByMvOnce(): Unit = {

      logRelease(session)

      session.moveTo(execution.absolutePathStr)
    }
  }

  lazy val movedPathStr: String = {

    val name = UUID.randomUUID().toString
    val path = CommonUtils./:/(lockPathStr, name)
    path
  }
  def byMoving: ByMoving = {

    ByMoving(resolver.newSession(movedPathStr))
  }

  def during[T](f: String => T): T = {
    assertUnlocked()

    val lockSession = byLockFile.session
    var needRelease = false

    try {
      lockSession.output(mode = WriteMode.CreateOnly) { out =>
        val sig = Random.nextInt().byteValue()

        resolver.retry {
          val stream = out.stream
          needRelease = true
          stream.write(sig)
          stream.flush()
          this.acquiredTimestamp = out.getLastModified
          val size = out.getLength
          assert(size == 1, s"size before == $size")
        }

        logAcquire(lockSession)

        val result = f(execution.absolutePathStr)

        val size2 = out.getLength
        assert(size2 == 1, s"size after == $size2")

        logPrefixed("prepare for release")
        result
      }
    } finally {
      if (needRelease)
        release()
    }

//    val path = acquireByMv()
//    try {
//      f(path)
////      movedExecution.output(false) { out =>
////        f(path)
////      }
//
//    } finally {
//      releaseByMvOnce()
//    }

//    lockExecution.output(overwrite = false) { out =>
//      try {
//        f(path)
//      } finally {
//        release()
//      }
//    }
  }

//  def acquire(): String = {
//    assertUnlocked(false)
//    execution.absolutePathStr
//  }

  def assertUnlocked(): Unit = {
    //TODO: fail early for non LockError
    resolver.retry {
      byLockFile.assertUnlockedOnce()
    }
  }

  def release(): Unit = {

    byLockFile.release()
  }

  /**
    * unlock on cleanup
    */
  override protected def cleanImpl(): Unit = {

    release()
  }

  def logAcquire(execution: URIResolver#URISession) = {

    Lock.acquired += execution -> System.currentTimeMillis()

    this.logPrefixed(s"=== ACQUIRED!: ${execution.absolutePathStr}")
  }

  def logRelease(execution: URIResolver#URISession) = {
    Lock.acquired -= execution

    this.logPrefixed(s"=== RELEASED! ${execution.absolutePathStr}")
  }
}

object Lock {

  val acquired: CachingUtils.ConcurrentMap[URIResolver#URISession, Long] = CachingUtils.ConcurrentMap()

  final val SUFFIX: String = ".lock"
}
