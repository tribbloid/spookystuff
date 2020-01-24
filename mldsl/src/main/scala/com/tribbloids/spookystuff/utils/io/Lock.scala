package com.tribbloids.spookystuff.utils.io

import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, NoRetry}
import org.slf4j.LoggerFactory

/**
  * Deprecated: Doesn't work for HDFSResolver (and future resolvers for other file systems)!
  * use [[Snapshot]] instead
  */
@Deprecated
case class Lock(
    resolver: URIResolver,
    pathStr: String,
    expire: Obsolescence = URIResolver.default.expire,
    override val _lifespan: Lifespan = Lifespan.JVM()
) extends LocalCleanable {

  import Lock._

  @volatile var acquiredTimestamp: Long = -1

  def getOriginal: resolver.URISession = resolver.newSession(pathStr)
  lazy val original: resolver.URISession = getOriginal

//  override def cleanableLogFunction(logger: Logger): String => Unit = logger.info

  private case object ByLockFile {

    val session: resolver.URISession = resolver.newSession(original.absolutePathStr + LOCK)

    def during[T](fn: URISession => T): T = {

      original.outer.retry {
        duringOnce(fn)
      }
    }

    protected def duringOnce[T](fn: URISession => T): T = {
      checkUnlocked()

      @volatile var needRelease = false

//      try {
//        session.touch()
//      } catch {
//        case e: Throwable =>
//          throw new ResourceLockError("cannot move to lock file path", e)
//      }
      // touch in most FS are unfortunately not atomic, has to create an OutputStream to hog the resource

      session.output(WriteMode.CreateOnly) { out =>
        out.stream
        needRelease = true

        logAcquire(session)

        try {
          fn(original)
        } catch {
          case e: ResourceLockError => throw e
          case e @ _                => throw NoRetry(e)
        } finally {

          if (needRelease)
            release()
        }
      }
    }

    //  @tailrec
    final def checkUnlocked(): Unit = {

      def existingLockIsExpired(r: Resource[_], e: FileAlreadyExistsException): Boolean = {
        val lockedTime = r.getLastModified
        val lockedDuration = System.currentTimeMillis() - lockedTime

        def errorInfo =
          s"Lock '${session.absolutePathStr}' is acquired by another executor or thread for $lockedDuration milliseconds"

        if (lockedDuration >= expire.deleteAfter.toMillis) {
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
          checkUnlocked()
        }
      }

      session.input { in =>
        if (in.isExisting) {

          handleExistingLock(in, null)
        }
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
              throw new RuntimeException(
                s"$logPrefix: lock file ${session.absolutePathStr} has been deleted"
              )

            val lastModified = in.getLastModified
            if (lastModified != acquiredTimestamp)
              throw new RuntimeException(
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

  /**
    * Don't use, experimental only
    */
  private case object ByMoving {

    lazy val movedPathStr: String = {

      val rnd = UUID.randomUUID().toString
//      val path = original.absolutePathStr + s".$rnd" + LOCK
      val path = original.absolutePathStr + LOCK
      path
    }

    def moved: resolver.URISession = resolver.newSession(movedPathStr)

    def acquire(): Unit = {

      val moved = this.moved
      getOriginal.moveTo(moved.absolutePathStr)
      logAcquire(moved)
      moved.clean()
    }

    def release(): Unit = {

      val moved = this.moved
      logRelease(moved)
      moved.moveTo(original.absolutePathStr)
      moved.clean()
    }

    def duringOnce[T](fn: URISession => T): T = {
      acquire()
      try {
        fn(getOriginal)
      } catch {
        case e: ResourceLockError => throw e
        case e @ _                => throw NoRetry(e)
      } finally {

        release()
      }
    }

    def during[T](fn: URISession => T): T = {
      resolver.retry {
        duringOnce(fn)
      }
    }
  }

  def during[T](fn: URISession => T): T = {
    ByMoving.during(fn)
  }

  /**e
    * unlock on cleanup
    */
  override protected def cleanImpl(): Unit = {

//    ByLockFile.release()
    ByMoving.release()
  }

  def logAcquire(execution: URIResolver#URISession): Unit = {

    Lock.acquiredRegistry += execution -> System.currentTimeMillis()

    this.logPrefixed(s"=== ACQUIRED!: ${execution.absolutePathStr}")
  }

  def logRelease(execution: URIResolver#URISession): Unit = {
    Lock.acquiredRegistry -= execution

    this.logPrefixed(s"=== RELEASED! ${execution.absolutePathStr}")
  }
}

object Lock {

  val acquiredRegistry: CachingUtils.ConcurrentMap[URIResolver#URISession, Long] = CachingUtils.ConcurrentMap()

  final val LOCK: String = ".lock"
}
